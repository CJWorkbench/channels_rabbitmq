import asyncio
import logging
import time
from collections import defaultdict, deque

import aio_pika
import msgpack
from aio_pika.exceptions import AMQPError, ChannelClosed, DeliveryError
from aio_pika.robust_connection import RobustConnection

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
                except Exception:
                    getter.cancel()  # Just in case getter is not done yet.

                    self._getters.remove(getter)

                    raise

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
        self._is_closed = False

    def full(self):
        return self.n >= self.capacity

    async def put_channel(self, channel, message):
        if self._is_closed:
            return

        while self.full():
            putter = self.loop.create_future()
            self._putters.append(putter)
            try:
                await putter
            except ChannelClosed:
                # While RabbitMQ was sending us a message, we closed the
                # connection. Since the getters would never finish, the putters
                # would deadlock unless we sent an exception to them, too. This
                # exception means we're closing down.
                #
                # Route `message` to nowhere. It _should_ be dropped.
                return
            except Exception:
                putter.cancel()  # Just in case putter is not done yet

                # Clean self._putters from canceled putters
                self._putters.remove(putter)

                raise

        self.n += 1
        self._out[channel].put(message)  # may create self._out[channel]
        _wakeup_next(self._out[channel]._getters)

    async def put_group(self, group, message):
        if self._is_closed:
            return

        if group not in self.local_groups:
            return  # don't create group

        # Copy channel names, so we can iterate asynchronously without racing
        channels = list(self.local_groups[group])
        for channel in channels:
            await self.put_channel(channel, message)

    async def get(self, channel):
        if self._is_closed:
            raise ChannelClosed

        try:
            # may create self._out[channel]
            item = await self._out[channel].get()
        finally:  # Even if there's a CanceledError
            if not self._out[channel]._queue and not self._out[channel]._getters:
                del self._out[channel]
        return item

    def group_add(self, group, channel, group_expiry=86400):
        if self._is_closed:
            return None

        channels = self.local_groups[group]  # may create set
        channels[channel] = time.time() + group_expiry
        return len(channels)

    def group_discard(self, group, channel):
        """
        Remove `channel` from `group` and return the number of channels left.

        Return None if the channel is not in the group.
        """
        if self._is_closed:
            return None

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

    def close(self):
        self._is_closed = True

        # Cancel all gets
        for out_queue in self._out.values():
            for waiter in out_queue._getters:
                if not waiter.done():
                    waiter.set_exception(ChannelClosed)
        self._out.clear()

        # Cancel all puts
        for waiter in self._putters:
            if not waiter.done():
                waiter.set_exception(ChannelClosed)
        self._putters.clear()


class SaneRobustConnection(RobustConnection):
    """
    A RobustConnection that doesn't do stupid things.

    Changes from RobustConnection:
        * if connection fails because of CancelledError, don't retry.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # override
    async def reconnect(self):
        # The parent uses `suppress(Exception)`, which is evil.
        try:
            # Calls `_on_connection_lost` in case of errors
            # super(RobustConnection, ...) gives grandparent connect()
            if await super(RobustConnection, self).connect():
                await self.on_reconnect()
        except asyncio.CancelledError:
            raise
        except Exception as err:
            log.info("Ignoring exception %r" % err)

    # override
    def _on_connection_lost(self, future: asyncio.Future, connection, code, reason):
        if isinstance(reason, asyncio.CancelledError):
            return  # we raised it elsewhere

        super()._on_connection_lost(future, connection, code, reason)


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
        group_expiry=86400,
        command_timeout=10,
    ):
        self.loop = loop
        self.host = host
        self.local_capacity = local_capacity
        self.remote_capacity = remote_capacity
        self.prefetch_count = prefetch_count
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.queue_name = queue_name
        self.command_timeout = command_timeout

        # incoming_messages: await `get()` on any channel-name queue to receive
        # the next message. If the `get()` is canceled, that's probably because
        # the caller is going away: we'll delete the queue in that case.
        self._incoming_messages = MultiQueue(loop, local_capacity)

        self._connection = None
        self._is_closed = False

        # self._send_lock is released when we're allowed to send commands to
        # RabbitMQ with self._send_channel.
        #
        # TODO optimize by building a pool of send channels. Each send waits
        # for an ack, so we could benefit from parallel sends.
        self._send_lock = asyncio.Semaphore(value=0, loop=loop)
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

        # Build connection. This will stall forever, until the connection
        # succeeds. It will log in the meantime.
        self._connect_task = loop.create_task(
            aio_pika.connect_robust(
                url=self.host, loop=loop, connection_class=SaneRobustConnection
            )
        )

        loop.create_task(self._connect(self._connect_task))

    async def _connect(self, connect_task):
        """
        Connect to RabbitMQ and schedule disconnect on event-loop close.

        (Passing `self._connect_task` as an argument is a bit of a hack. It
        ensures we call _connect() only _after_ we start self._connect_task.
        Otherwise, we may not be able to await it.)
        """
        try:
            self._connection = await connect_task
        except asyncio.CancelledError:
            # This means `self.close()` was called. Release our locks, so all
            # the user's next calls can execute.
            assert self._is_closed
            self._send_lock.release()
            self._groups_lock.release()
            self._incoming_messages.close()
            raise  # asyncio asks us to re-raise. We're done here anyway.
        finally:
            self._connect_task = None

        # Create the queue before opening send channels, so our first send
        # can only occur once the queue exists.
        #
        # Returns once we have _begun_ consuming.
        await self._begin_consuming(self._connection)

        send_channel = await self._connection.channel(publisher_confirms=True)
        self._send_channel = send_channel

        # Declare "groups" exchange once per time we connect. It will
        # persist forever; spurious declarations do no harm.
        #
        # We'll send to the group through self._send_channel.
        self._groups_exchange = await self._send_channel.declare_exchange(
            "groups", timeout=self.command_timeout
        )

        self._send_lock.release()  # We can bind groups to the queue now
        self._groups_lock.release()  # We can bind groups to the queue now

    async def _begin_consuming(self, connection):
        """
        Start reading messages from RabbitMQ until connection close.

        Returns once we have subscribed to the queue (and before processing any
        messages).
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
        await channel.set_qos(
            prefetch_count=self.prefetch_count, timeout=self.command_timeout
        )
        arguments = {
            "x-max-length": self.remote_capacity,
            "x-overflow": "reject-publish",
        }
        self._queue = await channel.declare_queue(
            name=self.queue_name,
            durable=False,
            exclusive=True,
            arguments=arguments,
            timeout=self.command_timeout,
        )

        # We'll set no_ack=False so we get back-pressure in consume().
        await self._queue.consume(consume, no_ack=False, timeout=self.command_timeout)

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
            if self._is_closed:
                return

            try:
                queued = await self._send_channel.default_exchange.publish(
                    message, routing_key=queue_name
                )
            except DeliveryError:
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
            if self._is_closed:
                return

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
            if self._is_closed:
                return

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
            if self._is_closed:
                return

            try:
                await self._groups_exchange.publish(message, routing_key=group)
            except DeliveryError:
                log.warning("Aborting send to group %s: a queue is at capacity", group)
                pass

    async def close(self):
        if self._is_closed:
            return

        self._is_closed = True

        if self._connect_task:
            # _connect()'s robust_connect() call has not returned yet. Cancel,
            # causing asyncio.CancelledError there.
            self._connect_task.cancel()
            self._connect_task = None

            # Wait for _connect() to complete
            async with self._send_lock:
                pass
        else:
            # If self._connect_task is None, that means we _have_ connected at
            # least once, so self._connection _has_ been set at least once, and
            # self._send_lock has been released by self._connect().
            async with self._send_lock:
                if self._connection:  # the connection was established
                    await self._do_close(self._connection)

    async def _do_close(self, connection):
        try:
            await connection.close()
        except AMQPError as err:
            # Don't raise. Assume failed disconnect leaves us in the state
            # we want.
            log.warning("AMQPError while closing connection: %s", str(err))
            raise
        finally:
            self._incoming_messages.close()
