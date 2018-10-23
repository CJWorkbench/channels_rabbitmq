import asyncio
import logging
import time
from collections import defaultdict, deque

import aio_pika
import msgpack

from channels.exceptions import ChannelFull

log = logging.getLogger(__name__)


def serialize(message, **kwargs):
    """
    Serializes message to a byte string.
    """
    value = msgpack.packb(message, use_bin_type=True)
    value = aio_pika.Message(value, **kwargs)
    return value


def deserialize(message):
    """
    Deserializes from a byte string.
    """
    return msgpack.unpackb(message.body, raw=False)


def channel_to_queue_name(channel):
    return channel[: channel.index("!")]


def _wakeup_next(waiters):
    """Wake up the next waiter (if any) that isn't cancelled."""
    while waiters:
        waiter = waiters.popleft()
        if not waiter.done():
            waiter.set_result(None)
            break


class MultiQueue:
    """
    Asyncio-friendly one-to-many queue that blocks when capacity is reached.

    This is not thread-safe. Use it from a single event loop to avoid errors.

    Usage:

        await multi_queue.put("q1", "hey")
        await multi_queue.put("q2", "jude")
        await multi_queue.get("q2")  # => "jude"

    This provides back-pressure: the producer must wait until a consumer has
    fetched an item before it can feed another. When `put()` blocks, that
    prevents the producer from reading in more data meaning it won't consume
    more of the RabbitMQ queue, and so the RabbitMQ queue will fill up and
    _its_ senders will begin raising ChannelFull.

    This queue also manages local groups. "Groups" are a distributed concept:
    10 channels may all be subscribed to a group, across three servers. Here,
    we deal with the 3-4 channels on this server which are subscribed to the
    group.
    """

    class OutQueue:
        def __init__(self, parent):
            self.parent = parent
            self._getters = deque()
            self._queue = deque()

        def put(self, item):
            self._queue.append(item)

        async def get(self):
            while not self._queue:
                getter = self.parent.loop.create_future()
                self._getters.append(getter)

                try:
                    await getter
                except:
                    getter.cancel()  # Just in case getter is not done yet.
                    self._getters.remove(getter)

            item = self._queue.popleft()
            self.parent.n -= 1
            _wakeup_next(self.parent._putters)

            return item

    def __init__(self, loop, capacity):
        self.loop = loop
        self.capacity = capacity
        self.n = 0

        self.local_groups = defaultdict(dict)  # group => {channel => expiry}
        self._out = defaultdict(lambda: MultiQueue.OutQueue(self))
        self._putters = deque()

    def full(self):
        return self.n >= self.capacity

    async def put_channel(self, channel, message):
        while self.full():
            putter = self.loop.create_future()
            self._putters.append(putter)
            try:
                await putter
            except:
                putter.cancel()  # Just in case putter is not done yet

                # Clean self._putters from canceled putters
                self._putters.remove(putter)

                raise

        self.n += 1
        self._out[channel].put(message)  # may create self._out[channel]
        _wakeup_next(self._out[channel]._getters)

    async def put_group(self, group, message):
        if group not in self.local_groups:
            return  # don't create group

        # Copy channel names, so we can iterate asynchronously without racing
        channels = list(self.local_groups[group])
        for channel in channels:
            await self.put_channel(channel, message)

    async def get(self, channel):
        try:
            # may create self._out[channel]
            item = await self._out[channel].get()
        finally:  # Even if there's a CanceledError
            if not self._out[channel]._queue and not self._out[channel]._getters:
                del self._out[channel]
        return item

    def group_add(self, group, channel, group_expiry=86400):
        channels = self.local_groups[group]  # may create set
        channels[channel] = time.time() + group_expiry
        return len(channels)

    def group_discard(self, group, channel):
        """
        Remove `channel` from `group` and return the number of channels left.

        Return None if the channel is not in the group.
        """
        if group not in self.local_groups:
            return None  # don't create set

        channels = self.local_groups[group]
        try:
            del channels[channel]
        except KeyError:
            return None  # it was already removed

        # Discard stale group memberships. These will happen if a
        # group_add() has no matching group_discard(). We only discard
        # within this one group because after we've discarded memberships,
        # the caller needs to check whether it should unbind from RabbitMQ.
        other_keys = channels.keys()
        now = time.time()
        for other_key in other_keys:
            if channels[other_key] < now:
                del channels[other_key]

        ret = len(channels)

        if ret == 0:
            del self.local_groups[group]

        return ret


class Connection:
    """
    A single event loop's connection to RabbitMQ.

    Django Channels doesn't prevent multiple event loops from existing (e.g.
    in unit tests). So channels_rabbitmq must handle the case where multiple
    event loops are running at once. The pattern that solves this: each event
    loop gets its own connection.

    When created, Connection will schedule the creation of an AMQP Queue and
    the streaming of all its messages into memory. Callers will receive
    messages from memory.

    There are two "capacity" parameters: `remote_capacity` determines how many
    messages RabbitMQ will queue before a send raises `ChannelFull`.
    `local_capacity` determines the maximum number of messages to hold in
    memory in our ever-running pull-messages-from-RabbitMQ loop. If consumers
    are too slow then `local_capacity` messages will be buffered in memory; at
    that point Connection won't pull any more from RabbitMQ; so senders can
    send another `remote_capacity` messages to RabbitMQ and then they'll start
    raising `ChannelFull`.

    There is also an "expiry" parameter: this determines the minimum number of
    seconds a message must remain in RabbitMQ before being culled.

    Queue names look like "channels_asldkjfg"; Channel names look like
    "channels_asldkjfg!asdaSDGdlkgj". RabbitMQ only sees the queue name; the
    channel names are embedded in `message["__asgi_channel__"]`. Each
    Connection is responsible for a single queue and multiple channels on that
    queue. We call that queue a "channel_key".

    Connection will decorate its event loop's `close` method so that the
    connection dies with the loop. This decoration cannot be removed.

    Connection isn't thread-safe: every method should be run within the same
    thread. This is what you want: an event loop runs on one thread, and this
    class's methods should all be called on the same event loop.
    """

    def __init__(
        self,
        loop,
        host,
        queue_name,
        *,
        local_capacity=100,
        remote_capacity=100,
        prefetch_count=10,
        expiry=60,
        group_expiry=86400
    ):
        self.loop = loop
        self.host = host
        self.local_capacity = local_capacity
        self.remote_capacity = remote_capacity
        self.prefetch_count = prefetch_count
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.queue_name = queue_name

        # incoming_messages: await `get()` on any channel-name queue to receive
        # the next message. If the `get()` is canceled, that's probably because
        # the caller is going away: we'll delete the queue in that case.
        self._incoming_messages = MultiQueue(loop, local_capacity)

        self._connection = self.loop.create_future()

        # self._send_lock is released when we're allowed to send commands to
        # RabbitMQ with self._send_channel.
        #
        # TODO optimize by building a pool of send channels. Each send waits
        # for an ack, so we could benefit from parallel sends.
        self._send_lock = asyncio.Semaphore(value=0, loop=loop)
        self._send_lock.acquire()  # released when _send_channel is not None
        self._send_channel = None

        # _groups_lock is released when _queue is set: that's when we're ready
        # to subscribe the queue to groups. (On disconnect, RabbitMQ will
        # automatically remove the subscription as it destroys the queue. On
        # reconnect, aio_pika will automatically recrease the queue and
        # its subscriptions.)
        #
        # Deadlock prevention: If you need self._groups_lock, acquire it
        # _before_ self._send_lock.
        self._groups_lock = asyncio.Semaphore(value=0, loop=loop)
        self._groups_exchange = None
        self._queue = None

        loop.create_task(self._connect(loop))

    async def _connect(self, loop):
        """
        Connect to RabbitMQ and schedule disconnect on event-loop close.
        """
        connection = await aio_pika.connect_robust(url=self.host, loop=loop)

        self._connection.set_result(connection)

        # Create the queue before opening send channels, so our first send can
        # only occur once the queue exists.
        await self._begin_consuming(connection)

        send_channel = await connection.channel(publisher_confirms=True)
        self._send_channel = send_channel

        # Declare "groups" exchange once per time we connect. It will
        # persist forever; spurious declarations do no harm.
        #
        # We'll send to the group through self._send_channel.
        self._groups_exchange = await self._send_channel.declare_exchange("groups")

        self._send_lock.release()  # We can bind groups to the queue now
        self._groups_lock.release()  # We can bind groups to the queue now

        return connection

    async def _begin_consuming(self, connection):
        """
        Reading messages from RabbitMQ until connection close.
        """

        async def consume(message):
            try:
                with message.process():  # acks when finished
                    d = deserialize(message)
                    channel = d.get("__asgi_channel__")
                    group = d.get("__asgi_group__")

                    if channel and group:
                        raise RuntimeError("Message has both channel and group")
                    elif not channel and not group:
                        raise RuntimeError("Message has neither channel nor group")

                    if channel:
                        # Put the message. Back-pressure if
                        # self._incoming_messages is at capacity.
                        await self._incoming_messages.put_channel(channel, d)
                    else:
                        await self._incoming_messages.put_group(group, d)
            except aio_pika.exceptions.ChannelClosed:
                # we tried to ack/nack and failed because we're closed. Assume
                # that's what the user wanted.
                pass

        channel = await connection.channel()
        await channel.set_qos(prefetch_count=self.prefetch_count)
        arguments = {
            "x-max-length": self.remote_capacity,
            "x-overflow": "reject-publish",
        }
        self._queue = await channel.declare_queue(
            name=self.queue_name, durable=False, exclusive=True, arguments=arguments
        )

        # We'll set no_ack=False so we get back-pressure in consume().
        await self._queue.consume(consume, no_ack=False)

    async def send(self, channel, message):
        """
        Send a message onto a (generic or specific) channel.

        This publishes through RabbitMQ even when sending from localhost to
        localhost. This gives approximate global ordering.
        """
        message["__asgi_channel__"] = channel
        message = serialize(message, expiration=int(self.expiry * 1000))

        queue_name = channel_to_queue_name(channel)

        # Publish with publisher_confirms=True. Assume the server is configured
        # with `overflow: reject-publish`, so we get a basic.nack if the queue
        # length is exceeded.
        async with self._send_lock:
            try:
                queued = await self._send_channel.default_exchange.publish(
                    message, routing_key=queue_name
                )
            except TypeError:
                # https://github.com/mosquito/aio-pika/issues/155
                queued = False

            if not queued:
                raise ChannelFull()

    async def receive(self, channel):
        """
        Receive the first message that arrives on the channel.

        If more than one coroutine waits on the same channel, only one waiter
        will receive the message when it arrives.
        """
        assert channel_to_queue_name(channel) == self.queue_name
        return await self._incoming_messages.get(channel)

    async def group_add(self, group, channel):
        assert (
            channel_to_queue_name(channel) == self.queue_name
        ), "This layer won't let you add a channel from another connection"

        # Lock before doing comparisons, so we don't race. We don't want to
        # duplicate bindings on the server, regardless of the sequence of
        # add+discard.
        async with self._groups_lock:
            n_bindings = self._incoming_messages.group_add(
                group, channel, self.group_expiry
            )
            if n_bindings == 1:
                # This group is new to our connection-level queue. Make a
                # connection-level binding.
                async with self._send_lock:
                    await self._queue.bind("groups", routing_key=group)

    async def group_discard(self, group, channel):
        assert (
            channel_to_queue_name(channel) == self.queue_name
        ), "This layer won't let you remove a channel from another connection"

        # Lock before doing comparisons, so we don't race. We don't want to
        # duplicate bindings on the server, regardless of the sequence of
        # add+discard.
        async with self._groups_lock:
            n_bindings = self._incoming_messages.group_discard(group, channel)
            if n_bindings == 0:
                # On this connection-level queue, we no longer have any
                # channels subscribed to this group. Kill the connection-level
                # binding.
                async with self._send_lock:
                    await self._queue.unbind("groups", routing_key=group)

    async def group_send(self, group, message):
        message["__asgi_group__"] = group
        message = serialize(message, expiration=int(self.expiry * 1000))

        async with self._send_lock:
            try:
                await self._groups_exchange.publish(message, routing_key=group)
            except TypeError:
                # https://github.com/mosquito/aio-pika/issues/155
                #
                # The Channels protocol has no way of reporting this error.
                # Just silently delete the message.
                log.warn("Aborting send to group %s: a queue is at capacity", group)
                pass

    async def close(self):
        connection = await self._connection
        await connection.close()
