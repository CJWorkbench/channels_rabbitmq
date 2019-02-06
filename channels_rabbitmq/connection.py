import asyncio
import functools
import logging
import time
from collections import defaultdict, deque

import aioamqp
import msgpack
from aioamqp.exceptions import AmqpClosedConnection, ChannelClosed, PublishFailed

from channels.exceptions import ChannelFull

logger = logging.getLogger(__name__)


GroupsExchange = "groups"
ReconnectDelay = 1.0  # seconds


def serialize(body):
    """
    Serializes message to a byte string.
    """
    return msgpack.packb(body, use_bin_type=True)


def channel_to_queue_name(channel):
    return channel[: channel.index("!")]


async def gather_without_leaking(tasks):
    """
    Run a bunch of tasks to completion, _then_ raise the first exception.

    This differs from regular `asyncio.gather()`, which leaves tasks running on
    the event loop without waiting for them to finish.

    It also differs because it accepts a list, not varargs.

    This looks like a hack: shouldn't it be part of asyncio instead? Why isn't
    this the _default_? ... dunno.
    """
    try:
        # Run, raising first exception
        await asyncio.gather(*tasks)
    except Exception:
        # Wait for all tasks to finish, exceptional or not
        await asyncio.gather(*tasks, return_exceptions=True)


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
                    await getter  # raises ChannelClosed
                    # trust _wakeup_next() and close() to clear self._getters
                except asyncio.CancelledError:
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

    async def put_channel(self, asgi_channel, message):
        if self._is_closed:
            return

        while self.full():
            putter = self.loop.create_future()
            self._putters.append(putter)

            try:
                await putter  # raises ChannelClosed
                # trust _wakeup_next and close() to clear self._putters
            except ChannelClosed:
                # Drop the message. We're closed.
                return
            except asyncio.CancelledError:
                self._putters.remove(putter)
                raise

        self.n += 1
        # may create self._out[asgi_channel]
        self._out[asgi_channel].put(message)
        _wakeup_next(self._out[asgi_channel]._getters)

    async def put_group(self, group, message):
        if self._is_closed:
            return

        if group not in self.local_groups:
            return  # don't create group

        await gather_without_leaking(
            [
                self.put_channel(asgi_channel, message)
                for asgi_channel in self.local_groups[group]
            ]
        )

    async def get(self, asgi_channel):
        if self._is_closed:
            raise ChannelClosed

        try:
            # may create self._out[asgi_channel]
            item = await self._out[asgi_channel].get()
        finally:  # Even if there's an asyncio.CancelledError
            if (
                not self._out[asgi_channel]._queue
                and not self._out[asgi_channel]._getters
            ):
                del self._out[asgi_channel]
        return item

    def group_add(self, group, asgi_channel, group_expiry=86400):
        if self._is_closed:
            return None

        channels = self.local_groups[group]  # may create set
        channels[asgi_channel] = time.time() + group_expiry
        return len(channels)

    def group_discard(self, group, asgi_channel):
        """
        Remove `asgi_channel` from `group` and return n_channels_remaining.

        Return None if the asgi_channel is not in the group.
        """
        if self._is_closed:
            return None

        if group not in self.local_groups:
            return None  # don't create set

        channels = self.local_groups[group]
        try:
            del channels[asgi_channel]
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
        """
        Nullify pending puts; raise ChannelClosed on pending gets.
        """
        if self._is_closed:
            return

        self._is_closed = True

        # Cancel all puts
        for waiter in self._putters:
            if not waiter.done():
                waiter.set_exception(ChannelClosed)
        self._putters.clear()

        # Cancel all gets
        for out_queue in self._out.values():
            for waiter in out_queue._getters:
                if not waiter.done():
                    waiter.set_exception(ChannelClosed)
        self._out.clear()


def stall_until_connected_or_closed(fn):
    """
    Suspend this awaitable until `self` is connected.

    Call `await fn(self, channel, ...)` -- the connection object is
    an `aioamqp` Connection which should be used until the end of the call. If
    the connection drops mid-call, an exception will be raised.

    Raise `ChannelClosed` if the connection is closed before `fn` can be
    called.

    This is vulnerable to races -- after all, there's no way to _guarantee_
    connection. But it handles the basic cases:

    * During startup, before connection, queue messages instead of sending.
    * Handle shutdown, even during startup. (We'll connect then disconnect.)
    """

    @functools.wraps(fn)
    async def inner(self, *args, **kwargs):
        while not self._is_connected and not self._is_closed:
            await self._connect_event.wait()

        if self._is_closed:
            raise ChannelClosed

        return await fn(self, self._channel, *args, **kwargs)

    return inner


async def ack_message_if_we_can(channel, delivery_tag):
    try:
        await channel.basic_client_ack(delivery_tag)
        logger.debug("Acked delivery %s", delivery_tag)
    except ChannelClosed:
        # we tried to ack/nack and failed because we're closed. Assume
        # that's what the user wanted. It's not like we can acknowledge
        # the message or raise an exception.
        #
        # Worst-case, we reconnect and receive the message again:
        # At-least-once delivery.
        logger.debug("ConnectionClosed acking delivery %s", delivery_tag)
        pass


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

        # pending_puts: a "purgatory" for messages as we put them into
        # incoming_messages.
        #
        # This is complex, so hold on.
        #
        # aioamqp will "await" our `_handle_message` callback, meaning it won't
        # call anything until that callback returns. But handle_message can't
        # ack a message until incoming_messages has <= self.local_capacity
        # messages. (That's the whole point of local_capacity: to block acks,
        # so the remote queue gets backlogged.)
        #
        # So handle_message needs to kick off "background" tasks -- using
        # event_loop.create_task(). We need to manage those background tasks,
        # so we can clean them up when we close.
        #
        # That's pending_puts: tasks running in the background. Each such task
        # finishes by acking its message and deleting itself from this list.
        self._pending_puts = set()

        # Lock used to add/remove from groups atomically
        self._groups_lock = asyncio.Lock()

        self._is_closed = False

        # self._is_connected: means self._protocol and self._channel are
        # initialized and ready to use.
        self._is_connected = False
        self._protocol = None
        self._transport = None
        # self._connect_event: a transient variable that signals, "Something
        # happened."
        #
        # When disconnected, lots of calls will wait on self._connect_event.
        # When it gets set, that doesn't mean "we've connected": it just means,
        # "check again whether self._is_connected".
        self._connect_event = asyncio.Event(loop=loop)

        # self.worker: Something to await, to know that _everything_ is finished
        # (useful in unit tests when we actually want to disconnect).
        self.worker = asyncio.ensure_future(self._connect_forever(), loop=loop)

        # self.close() uses self._protocol and self._transport. Don't worry
        # about them being stale: it's okay for self.close() to make spurious
        # calls.
        self._protocol = None
        self._transport = None

    async def _connect_forever(self):
        """
        Connect -- and reconnect -- to RabbitMQ, forevermore.
        """
        while not self._is_closed:
            try:
                await self._connect_and_run()
            except (AmqpClosedConnection, ConnectionError, OSError) as err:
                if self._is_closed:
                    logger.debug("Connect/run on RabbitMQ failed: %r", err)
                    # these aren't errors when the caller said close(). Not
                    # really. AmqpClosedConnection is _expected_ even.
                    return

                logger.warning(
                    "Connect/run on RabbitMQ failed: %r; will retry in %fs",
                    err,
                    ReconnectDelay,
                )
                await asyncio.sleep(ReconnectDelay)
            except Exception:
                logger.exception("Unhandled exception from aioamqp")
                raise  # and crash

    def _notify_connect_event(self):
        """
        Notify anybody waiting for `self._connect_event` and reset it.
        """
        old_event = self._connect_event
        if not old_event.is_set():
            old_event.set()
        self._connect_event = asyncio.Event()
        # After we return, everyone waiting for `_connect_event` will wake up.

    async def _connect_and_run(self):
        self._is_connected = False

        logger.info("Channels connecting to RabbitMQ at %s", self.host)
        transport, protocol = await aioamqp.from_url(self.host)

        logger.debug("Connected; setting up")
        channel = await protocol.channel()

        # Set publisher confirms -- so we can uphold the guarantees we promise
        # in the Channels API.
        await channel.confirm_select()

        # Declare "groups" exchange. It may persist; spurious declarations
        # (such as on reconnect) are harmless.
        await channel.exchange_declare(GroupsExchange, "direct")

        # Queue up the handling of messages.
        #
        # This is an exclusive queue, because we want it to disappear when we
        # shut down. (Otherwise we'd leak queues every deploy.) And since an
        # exclusive queue disappears when we aren't connected, there's no way
        # to preserve every message across connects. That's okay -- disconnect
        # means one or two lost messages and nothing more.
        await channel.queue_declare(
            self.queue_name,
            exclusive=True,
            arguments={
                "x-max-length": self.remote_capacity,
                "x-overflow": "reject-publish",
                "x-message-ttl": int(self.expiry * 1000),
            },
        )
        await channel.basic_qos(prefetch_count=self.prefetch_count)
        # It's tempting to set no_ack=True, since this is an exclusive queue.
        # But if we do that, how do we back-pressure? What happens when we
        # receive too many messages -- do TCP buffers fill up and prevent other
        # messages from moving through this channel? ... let's not investigate
        # until speed or network traffic becomes an issue.
        await channel.basic_consume(self._handle_message, self.queue_name)

        self._is_connected = True
        self._protocol = protocol
        self._transport = transport
        self._channel = channel

        # Re-bind groups (after reconnect)
        async with self._groups_lock:
            groups = list(self._incoming_messages.local_groups.keys())
            logger.debug("Rebinding groups to queue %s: %r", self.queue_name, groups)
            await gather_without_leaking(
                [
                    channel.queue_bind(
                        self.queue_name, GroupsExchange, routing_key=group
                    )
                    for group in groups
                ]
            )

        self._notify_connect_event()  # anyone waiting for us?

        # and now run until eternity...
        if not self._is_closed:
            # What happens on error? One of two things:
            #
            # 1. RabbitMQ closes self._channel. Why would it do this? Well,
            #    that's not for us to ask. The most common case is:
            # 2. RabbitMQ closes self._protocol. If it does, self._protocol
            #    will go and close self._channel.
            # 3. Network error. self._protocol.worker will return in that
            #    case.
            #
            # In cases 2 and 3, `self._protocol.run()` will raise
            # AmqpClosedConnection, close connections, and bail. In case 1, we
            # need to force the close ourselves.
            logger.info("Monitoring for network interruptions")
            await self._channel.close_event.wait()  # case 1, 2, 3

        logger.info("Disconnecting")

        # case 1 only: if the channel was closed and the connection wasn't,
        # wipe out the connection. (Otherwise, this is a no-op.)
        await protocol.close()
        transport.close()  # probably spurious

        logger.debug("Cleaning up after disconnect")

        # await protocol.worker so that every Future that's been
        # created gets awaited.
        await protocol.worker

    async def _handle_message(self, channel, body, envelope, properties):
        try:
            d = msgpack.unpackb(body, raw=False)
            asgi_channel = d.get("__asgi_channel__")
            group = d.get("__asgi_group__")

            logger.debug(
                "Received message %s on ASGI channel/group %s",
                envelope.delivery_tag,
                asgi_channel or group,
            )

            if asgi_channel and group:
                raise RuntimeError("Message has both channel and group")
            elif not asgi_channel and not group:
                raise RuntimeError("Message has neither channel nor group")
        except Exception:
            await ack_message_if_we_can(channel, envelope.delivery_tag)
            raise

        # Delay the ack until after the message is added to the queue. But
        # _return_ immediately.
        #
        # This works around https://github.com/Polyconseil/aioamqp/issues/149
        loop = asyncio.get_event_loop()
        task = loop.create_task(
            self._handle_message_background(
                channel, asgi_channel, group, d, envelope.delivery_tag
            )
        )
        self._pending_puts.add(task)
        # Cleanup: remove from _pending_puts when done.
        task.add_done_callback(self._pending_puts.remove)

    async def _handle_message_background(
        self, channel, asgi_channel, group, data, delivery_tag
    ):
        """
        Deliver `data` to `asgi_channel` or `group`, then ack.

        See comment on `self._pending_puts`. To sum up: we need to schedule
        this code to run later, so we don't block aioamqp from handling network
        traffic.
        """
        # Put into _incoming_messages. This shouldn't raise anything, though
        # CancelledError is possible in theory. (CancelledError should not be
        # caught.)
        if asgi_channel:
            # Put the message. Back-pressure if
            # self._incoming_messages is at capacity.
            await self._incoming_messages.put_channel(asgi_channel, data)
        else:
            await self._incoming_messages.put_group(group, data)

        await ack_message_if_we_can(channel, delivery_tag)

    @stall_until_connected_or_closed
    async def send(self, channel, asgi_channel, message):
        """
        Send a message onto a (generic or specific) channel.

        This publishes through RabbitMQ even when sending from localhost to
        localhost. This gives approximate global ordering.

        Usage:

            connection.send({'foo': 'bar'})
        """
        message["__asgi_channel__"] = asgi_channel
        message = msgpack.packb(message, use_bin_type=True)

        queue_name = channel_to_queue_name(asgi_channel)

        # Publish with publisher_confirms=True. Assume the server is configured
        # with `overflow: reject-publish`, so we get a basic.nack if the queue
        # length is exceeded.
        try:
            logger.debug("publish %r on %s", message, queue_name)
            await channel.publish(message, "", queue_name)
        except PublishFailed:
            raise ChannelFull()
        logger.debug("ok")

    async def receive(self, asgi_channel):
        """
        Receive the first message that arrives on the channel.

        If more than one coroutine waits on the same channel, only one waiter
        will receive the message when it arrives.
        """
        assert channel_to_queue_name(asgi_channel) == self.queue_name
        return await self._incoming_messages.get(asgi_channel)

    @stall_until_connected_or_closed
    async def group_add(self, channel, group, asgi_channel):
        """
        Register to receive messages for ``group`` on RabbitMQ.

        Upon reconnect, this Connection will re-register every group ... but
        any messages sent while disconnected won't reach it.
        """
        assert (
            channel_to_queue_name(asgi_channel) == self.queue_name
        ), "This layer won't let you add a channel from another connection"

        async with self._groups_lock:
            n_bindings = self._incoming_messages.group_add(
                group, asgi_channel, self.group_expiry
            )
            if n_bindings == 1:
                logger.debug("Binding queue %s to group %s", self.queue_name, group)
                # This group is new to our connection-level queue. Make a
                # connection-level binding.
                await channel.queue_bind(
                    self.queue_name, GroupsExchange, routing_key=group
                )

    async def group_discard(self, group, asgi_channel):
        """
        No longer receive messages for ``group`` on RabbitMQ.

        If we're connected to RabbitMQ when calling this, it will return once
        disconnected. If we happen to be between reconnects, it will return
        immediately -- and when we finally reconnect, ``group`` will be
        unregistered.
        """
        assert (
            channel_to_queue_name(asgi_channel) == self.queue_name
        ), "This layer won't let you remove a channel from another connection"

        async with self._groups_lock:
            n_bindings = self._incoming_messages.group_discard(group, asgi_channel)
            if n_bindings == 0 and self._is_connected and not self._is_closed:
                logger.debug("Unbinding queue %s from group %s", self.queue_name, group)
                # Disconnect, if we're connected.
                await self._channel.queue_unbind(
                    self.queue_name, GroupsExchange, routing_key=group
                )

    @stall_until_connected_or_closed
    async def group_send(self, channel, group, message):
        message["__asgi_group__"] = group
        message = msgpack.packb(message, use_bin_type=True)

        logger.debug("group_send %r to %s", message, group)

        try:
            await channel.publish(message, GroupsExchange, routing_key=group)
        except PublishFailed:
            # The Channels protocol has no way of reporting this error.
            # Just silently delete the message.
            logger.warning("Aborting send to group %s: a queue is at capacity", group)
            pass

    async def close(self):
        self._is_closed = True

        # we're connected ... so disconnect!
        if self._protocol:
            try:
                await self._protocol.close()  # may be spurious
            except AmqpClosedConnection:
                pass
        if self._transport:
            self._transport.close()  # may be spurious

        # Wait for self._connect_forever() to exit. That'll mean all transient
        # variables (including self._protocol.worker) are cleaned up.
        await self.worker

        # close our queues. pending_puts' messages will be ignored, and the
        # tasks will be completed.
        self._incoming_messages.close()
        await gather_without_leaking(list(self._pending_puts))
