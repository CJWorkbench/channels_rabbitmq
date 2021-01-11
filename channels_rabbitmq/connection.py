import asyncio
import functools
import logging
from typing import Any, Callable, List, NamedTuple

import aiormq
import msgpack
from aiormq.exceptions import ChannelClosed, ConnectionClosed, DeliveryError

from channels.exceptions import ChannelFull

logger = logging.getLogger(__name__)


ReconnectDelay = 1.0  # seconds
BackpressureWarningInterval = 5.0  # seconds
ExpiryWarningInterval = 5.0  # seconds


def serialize(body):
    """Serializes message to a byte string."""
    return msgpack.packb(body)


def channel_to_queue_name(channel):
    return channel[: channel.index("!")]


async def gather_without_leaking(tasks):
    """Run a bunch of tasks to completion, _then_ raise the first exception.

    This differs from regular `asyncio.gather()`, which leaves tasks running on
    the event loop without waiting for them to finish.

    It also differs because it accepts a list, not varargs.

    This looks like a hack: shouldn't it be part of asyncio instead? Why isn't
    this the _default_? ... dunno.
    """
    try:
        # Run, raising first exception
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        # async_to_sync() can cause these. asyncio.CancelledError should not be
        # an Exception, but is is in Python <=3.7
        raise
    except Exception:
        # Wait for all tasks to finish, exceptional or not
        await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        raise


class MessageHandle(NamedTuple):
    """A message for queueing locally.

    Create this in the RabbitMQ consumer. Use it and discard it in MultiQueue.
    """

    data: Any
    """Contents of the message."""

    expires: float
    """Absolute time after which we can discard the message.

    This is event-loop time, as used in `loop.call_at()`. See
    https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.call_at
    """

    mark_delivered: Callable[[], None]
    """Callback for when the message is expired or delivered.

    This will be called _eventually_, exactly once, in these cases:

    * In the happy case, it's called immediately before an async `get()`
      returns it.
    * When it's queued but there's no `get()`, it's called after `expires`.
    """


class MultiQueue:
    """Asyncio-friendly one-to-many queue that blocks when capacity is reached.

    Usage:

        await multi_queue.put("q1", "hey")
        await multi_queue.put("q2", "jude")
        await multi_queue.get("q2")  # => "jude"

    Ordering guarantee: the awaiting `put()` calls succeed in FIFO order.

    This provides back-pressure: the producer must wait until a consumer has
    fetched an item before it can feed another. When `put()` blocks, that
    prevents the producer from reading in more data meaning it won't consume
    more of the RabbitMQ queue, and so the RabbitMQ queue will fill up and
    _its_ senders will begin raising ChannelFull.

    This queue also manages local groups. "Groups" are a distributed concept:
    10 channels may all be subscribed to a group, across three servers. Here,
    we deal with the 3-4 channels on this server which are subscribed to the
    group.

    To prevent overflow, each message has an expiry time. When adding, if we're
    about to block, we expire old messages to make room.
    """

    class ChannelQueue:
        """An "inner" queue: a single Django-Channels channel's MessageHandles.

        This solves a problem in the Django Channels Specification: a channel
        can exist without the layer knowing about it.

        Steps to reproduce the problem:

            1. Sender: send a message
            2. Recipient: receive the message

        This is backwards: the sender sent a message before the recipient
        even existed! But yep, that's the spec. The recipient _polls_ for
        messages, so it doesn't exist when it's busy or during startup.

        Hence ChannelQueue: it's like asyncio.queue(), but with a notion of
        "unused". An unused queue is an empty queue with no getter.
        """

        def __init__(self):
            self._queue = asyncio.Queue()  # unbounded: no putter queue

        def unused(self) -> bool:
            """True if there are no queued messages and no pending getters."""
            return self._queue.empty() and not self._queue._getters

        def put_nowait(self, message_handle) -> None:
            self._queue.put_nowait(message_handle)

        async def get(self) -> Any:
            """Wait for a message to become available, then return it."""
            message_handle = await self._queue.get()
            message_handle.mark_delivered()
            return message_handle.data

        def remove_expired(self, now: float) -> bool:
            """Remove (and mark_delivered()) MessageHandles we won't use.

            Return True if anything was deleted.
            """
            retval = False
            # We need to peek: self._queue is an asyncio.Queue, and
            # self._queue._queue is the collections.deque() _inside_ it.
            while not self._queue.empty() and self._queue._queue[0].expires <= now:
                message_handle = self._queue._queue.popleft()
                message_handle.mark_delivered()
                retval = True

            return retval

        @property
        def soonest_expiry(self) -> float:
            """The earliest "expires" of all queued messages."""
            return self._queue._queue[0].expires

        def close(self) -> None:
            """Raise ChannelClosed on all readers."""
            for waiter in self._queue._getters:
                if not waiter.done():
                    waiter.set_exception(
                        ConnectionClosed("<channels_rabbitmq.Connection.close>", "")
                    )

    def __init__(self, loop, capacity, local_expiry):
        self.loop = loop
        self.capacity = capacity
        self.local_expiry = local_expiry
        self.n = 0

        self.local_groups = {}  # group => {channel, ...}
        self._queues = {}  # asgi_channel => ChannelQueue
        self._nonempty = asyncio.Event()
        self._closed = asyncio.Event()
        # We'll order puts using a lock. Waiters on an asyncio.Lock are
        # released in FIFO order.
        self._putter_lock = asyncio.Lock()
        self._last_logged_backpressure = 0  # loop.time() result
        self._last_logged_expiry = 0  # loop.time() result

    def _drop_all_expired(self, now: float) -> bool:
        """Drop expired messages; return `true` if something happened."""
        deleted = False
        channels_to_delete = []
        for asgi_channel, queue in self._queues.items():
            if queue.remove_expired(now):
                deleted = True
            if queue.unused():
                channels_to_delete.append(asgi_channel)
        for asgi_channel in channels_to_delete:
            del self._queues[asgi_channel]
        return deleted

    async def expire_locally_until_closed(self):
        while not self._closed.is_set():
            closed_task = asyncio.create_task(self._closed.wait())

            while self.n == 0:
                nonempty_task = asyncio.create_task(self._nonempty.wait())
                await asyncio.wait(
                    {nonempty_task, closed_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if self._closed.is_set():
                    return

            # Assume messages expire in FIFO order. Therefore, we know there's
            # no need to check for expiry until the first unhandled message
            # expires.
            soonest_expiry = min(
                [
                    queue.soonest_expiry
                    for queue in self._queues.values()
                    if not queue._queue.empty()
                ]
            )
            now = asyncio.get_event_loop().time()
            if now < soonest_expiry:
                try:
                    await asyncio.wait_for(
                        self._closed.wait(), timeout=soonest_expiry - now
                    )
                    return  # closed!
                except asyncio.TimeoutError:
                    now = asyncio.get_event_loop().time()  # good

            # Now, we know timed_out.is_set()
            dropped = self._drop_all_expired(now)
            if dropped:
                self._log_local_expiry_debounced(now)

    def _increase_n(self):
        assert self.n < self.capacity
        self.n += 1  # decreased in self._cleanup_message()
        if self.n == self.capacity:
            now = self.loop.time()
            self._log_backpressure_debounced(now)
        self._nonempty.set()

    def _decrease_n(self) -> None:
        self.n -= 1
        if self.n == 0:
            self._nonempty.clear()

    def _put_message(
        self, message_handle: MessageHandle, asgi_channels: List[str]
    ) -> None:
        if len(asgi_channels) == 0:
            message_handle.mark_delivered()
            return

        n_undelivered = len(asgi_channels)

        def mark_delivered() -> None:
            nonlocal n_undelivered
            assert n_undelivered >= 1
            n_undelivered -= 1
            if n_undelivered == 0:
                self._decrease_n()
                message_handle.mark_delivered()

        queued_message_handle = message_handle._replace(mark_delivered=mark_delivered)

        for asgi_channel in asgi_channels:
            if asgi_channel not in self._queues:
                self._queues[asgi_channel] = MultiQueue.ChannelQueue()
            self._queues[asgi_channel].put_nowait(queued_message_handle)

        self._increase_n()

    def put_channel_nowait(
        self, asgi_channel: str, message_handle: MessageHandle
    ) -> None:
        """Queue a message.

        This will call `message_handle.mark_delivered()` when the message is
        either delivered or expired.
        """
        self._put_message(message_handle, [asgi_channel])

    def put_group_nowait(self, group: str, message_handle: MessageHandle) -> None:
        """Queue a message into all channels in `group` (selected atomically).

        This will call `message_handle.mark_delivered()` once, after _all_ deliveries
        have succeeded or timed out.

        The recipient channels are chosen at call time, though messages aren't
        _delivered_ at call time. So a recipient calling `get()` may receive a
        message after it called `group_discard()` -- as long as the
        `put_group()` happened before the `group_discard()`.
        """
        asgi_channels = self.local_groups.get(group, [])
        self._put_message(message_handle, asgi_channels)

    def _build_big_queues_str(self) -> str:
        blockers = [(name, len(q._queue._queue)) for name, q in self._queues.items()]
        blockers.sort(key=lambda b: b[1], reverse=True)
        return ", ".join(f"{name} ({count})" for name, count in blockers[:3])

    def _log_backpressure_debounced(self, now: float) -> None:
        """Log back-pressure if we haven't logged it in a while.

        Back-pressure is by design: the user configured it.

        However, back-pressure on a well-oiled website often means something's
        wrong -- for example, a cancelled Websocket client erroneously still
        added to a group; or a user with a slow web browser only consuming one
        message every 5s. You can't rely on a ChannelFull exception appearing:
        back-pressure can convince your users the site is broken, so they'll go
        away.

        Should this be _info_ (because it's a normal event) or _warning_?
        Warning seems appropriate because back-pressure can mean disaster on a
        production website.
        """
        if now - self._last_logged_backpressure > BackpressureWarningInterval:
            logger.warning(
                "Back-pressuring. Biggest queues: %s", self._build_big_queues_str()
            )
            self._last_logged_backpressure = now

    def _log_local_expiry_debounced(self, now: float) -> None:
        """Log dropped messages.

        Expiry is by design: the user configured it. But it's usually not
        intended.

        Should this be _info_ (because it's a normal event) or _warning_?
        Warning seems appropriate because expiry usually means there's a
        problem the dev should fix.
        """
        if now - self._last_logged_expiry > ExpiryWarningInterval:
            logger.warning(
                "A message (or messages) expired locally. Biggest queues: %s",
                self._build_big_queues_str(),
            )
            self._last_logged_expiry = now

    async def get(self, asgi_channel: str) -> Any:
        """Maybe wait for a `put_channel()` on `asgi_channel`; return its data."""
        if self._closed.is_set():
            raise ConnectionClosed("channels_rabbitmq.Connection.close", "")

        if asgi_channel not in self._queues:
            self._queues[asgi_channel] = MultiQueue.ChannelQueue()

        try:
            return await self._queues[asgi_channel].get()
        finally:  # Even if there's an asyncio.CancelledError
            if (
                asgi_channel in self._queues  # it may have been deleted in await?
                and self._queues[asgi_channel].unused()
            ):
                del self._queues[asgi_channel]

    def group_add(self, group, asgi_channel):
        if self._closed.is_set():
            return None

        channels = self.local_groups.setdefault(group, set())
        channels.add(asgi_channel)
        return len(channels)

    def group_discard(self, group, asgi_channel):
        """Remove `asgi_channel` from `group` and return n_channels_remaining.

        Return None if the asgi_channel is not in the group.
        """
        if self._closed.is_set():
            return None

        if group not in self.local_groups:
            return None  # don't create set

        channels = self.local_groups[group]
        try:
            channels.remove(asgi_channel)
        except KeyError:
            return None  # it was already removed

        ret = len(channels)

        if ret == 0:
            del self.local_groups[group]

        return ret

    def close(self):
        """Nullify pending puts; raise ConnectionClosed on pending gets."""
        if self._closed.is_set():
            return
        self._closed.set()

        # Cancel all gets
        for out_queue in self._queues.values():
            out_queue.close()
        self._queues.clear()


def stall_until_connected_or_closed(fn):
    """Suspend this awaitable until `self` is connected.

    Call `await fn(self, channel, ...)` -- the connection object is
    an `aiormq.Connection` which should be used until the end of the call. If
    the connection drops mid-call, an exception will be raised.

    Raise `ConnectionClosed` if the connection is closed before `fn` can be
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
            raise ConnectionClosed("channels_rabbitmq.Connection.close", "")

        return await fn(self, self._channel, *args, **kwargs)

    return inner


async def ack_message_if_we_can(channel, delivery_tag):
    try:
        await channel.basic_ack(delivery_tag)
        logger.debug("Acked delivery %s", delivery_tag)
    except ChannelClosed:
        # we tried to ack/nack and failed because we're closed. Assume
        # that's what the user wanted. It's not like we can acknowledge
        # the message or raise an exception.
        #
        # Worst-case, we reconnect and receive the message again:
        # At-least-once delivery.
        logger.debug("ConnectionClosed acking delivery %s", delivery_tag)
    except Exception:
        logger.exception("Unexpected failure during ack")  # print stack trace


class Connection:
    """A single event loop's connection to RabbitMQ.

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
        expiry=60,
        local_expiry=None,
        ssl_context=None,
        groups_exchange="groups",
    ):
        if local_expiry is None:
            local_expiry = expiry
        self.loop = loop
        self.host = host
        self.local_capacity = local_capacity
        self.remote_capacity = remote_capacity
        self.expiry = expiry
        self.local_expiry = local_expiry
        self.queue_name = queue_name
        self.ssl_context = ssl_context
        self.groups_exchange = groups_exchange

        # incoming_messages: await `get()` on any channel-name queue to receive
        # the next message. If the `get()` is canceled, that's probably because
        # the caller is going away: we'll delete the queue in that case.
        self._incoming_messages = MultiQueue(loop, local_capacity, local_expiry)

        # Lock used to add/remove from groups atomically
        self._groups_lock = asyncio.Lock()

        self._is_closed = False

        # self._is_connected: means self._connection and self._channel are
        # initialized and ready to use.
        #
        # self.close() uses self._connection. Don't worry about it being stale:
        # it's okay for self.close() to make spurious calls.
        self._connection = None

        # self._connect_event: a transient variable that signals, "Something
        # happened."
        #
        # When disconnected, lots of calls will wait on self._connect_event.
        # When it gets set, that doesn't mean "we've connected": it just means,
        # "check again whether self._is_connected".
        self._connect_event = asyncio.Event()

        # self.worker: Something to await, to know that _everything_ is finished
        # (useful in unit tests when we actually want to disconnect).
        self.worker = asyncio.ensure_future(self._connect_forever())
        self.expiry_worker = asyncio.ensure_future(
            self._incoming_messages.expire_locally_until_closed()
        )

    @property
    def _is_connected(self):
        """True if self._connection is set.

        The connection might be invalid: it might be in the process of
        disconnecting. Races abound; but at least we know that we _think_ we're
        connected.
        """
        return self._connection is not None and self._channel is not None

    async def _connect_forever(self):
        """Connect -- and reconnect -- to RabbitMQ, forevermore."""
        while not self._is_closed:
            try:
                await self._connect_and_run()
            except (
                ConnectionClosed,
                ChannelClosed,  # setup error: e.g., queue_declare conflict
                ConnectionError,
                OSError,
            ) as err:
                if self._is_closed:
                    logger.debug("Connect/run on RabbitMQ failed: %r", err)
                    # these aren't errors when the caller said close(). Not
                    # really. ConnectionClosed is _expected_ even.
                    return

                logger.warning(
                    "Connect/run on RabbitMQ failed: %r; will retry in %fs",
                    err,
                    ReconnectDelay,
                )
                await asyncio.sleep(ReconnectDelay)
            except asyncio.CancelledError:
                # async_to_sync() can cause these. asyncio.CancelledError should not be
                # an Exception, but is is in Python <=3.7
                raise  # and crash

    async def _connect_and_run(self):
        logger.info("Channels connecting to RabbitMQ at %s", self.host)

        self._connect_event.clear()
        self._channel = None

        # Set self._connection immediately, so close() can call
        # `self._connection.close()` during connect.
        # self._connection = aiormq.Connection(self.host, context=self.ssl_context)
        self._connection = aiormq.Connection(self.host)
        try:
            await self._connection.connect()
            self._channel = await self._setup_channel_during_connect(self._connection)
            self._connect_event.set()

            # And now run until eternity. Or, realistically, until RabbitMQ
            # closes the connection. (It will close the connection when we call
            # `self.close()`.)
            logger.info("Monitoring for network interruptions")
            await gather_without_leaking(
                [self._connection.closing, self._channel.closing]
            )

        finally:
            self._connection = None
            self._channel = None
            self._connect_event.clear()

    async def _setup_channel_during_connect(self, connection):
        """Create a new `channel` and ensure structures on RabbitMQ.

        Upon return, we guarantee:

        * The channel is set to "publisher confirms"
        * `self.groups_exchange` is declared
        * A `self.group_name` exclusive queue is declared, with
          `self.remote_capacity` and `self.local_capacity` set.
        * (If we're reconnecting) groups are bound on `self.groups_exchange`.
        * The channel is consuming with `self._handle_message`.

        Can raise ChannelError, ConnectionClosed, and basically any other error.
        """
        logger.debug("Connected; setting up")

        # Set publisher confirms -- so we can uphold the guarantees we promise
        # in the Channels API.
        channel = await connection.channel(publisher_confirms=True)

        # Declare "groups" exchange. It may persist; spurious declarations
        # (such as on reconnect) are harmless.
        await channel.exchange_declare(self.groups_exchange, exchange_type="direct")

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
        await channel.basic_qos(prefetch_count=self.local_capacity)

        # Re-bind groups (after reconnect)
        async with self._groups_lock:
            groups = list(self._incoming_messages.local_groups.keys())
            logger.debug("Rebinding groups to queue %s: %r", self.queue_name, groups)
            await gather_without_leaking(
                [
                    channel.queue_bind(
                        self.queue_name, self.groups_exchange, routing_key=group
                    )
                    for group in groups
                ]
            )

        # It's tempting to set no_ack=True, since this is an exclusive queue.
        # But if we do that, how do we back-pressure? What happens when we
        # receive too many messages -- do TCP buffers fill up and prevent other
        # messages from moving through this channel? ... let's not investigate
        # until speed or network traffic becomes an issue.
        await channel.basic_consume(self.queue_name, self._handle_message)

        return channel

    async def _handle_message(self, message: aiormq.types.DeliveredMessage) -> None:
        """Act upon a message from aiormq, then ack.

        Log errors, and never raise them.
        """
        try:
            data = msgpack.unpackb(message.body)
            asgi_channel = data.get("__asgi_channel__")
            group = data.get("__asgi_group__")

            if asgi_channel and group:
                raise RuntimeError("Message has both channel and group")
            elif not asgi_channel and not group:
                raise RuntimeError("Message has neither channel nor group")
        except asyncio.CancelledError:
            # async_to_sync() can cause these. asyncio.CancelledError should not be
            # an Exception, but is is in Python <=3.7
            raise
        except Exception:
            logger.exception("Ignoring message")  # will print a stack trace
            await ack_message_if_we_can(message.channel, message.delivery.delivery_tag)
            return None

        logger.debug(
            "Received message %s on ASGI channel/group %s",
            message.delivery.delivery_tag,
            asgi_channel or group,
        )

        def begin_ack():
            nonlocal message
            channel = message.channel
            delivery_tag = message.delivery.delivery_tag
            if not channel.is_closed:
                channel.create_task(ack_message_if_we_can(channel, delivery_tag))

        message_handle = MessageHandle(
            data, self.loop.time() + self.local_expiry, begin_ack
        )

        # Put the message. _incoming_messages will call begin_ack() when the
        # messages are received; until then, we'll back-pressure. (If we
        # delay all our acks, RabbitMQ will delay sending us more messages.)
        if asgi_channel:
            self._incoming_messages.put_channel_nowait(asgi_channel, message_handle)
        else:
            self._incoming_messages.put_group_nowait(group, message_handle)

    @stall_until_connected_or_closed
    async def send(self, channel, asgi_channel, message):
        """Send a message onto a (generic or specific) channel.

        This publishes through RabbitMQ even when sending from localhost to
        localhost. This gives approximate global ordering.

        Usage:

            connection.send({'foo': 'bar'})
        """
        message = {**message, "__asgi_channel__": asgi_channel}
        body = msgpack.packb(message, use_bin_type=True)

        queue_name = channel_to_queue_name(asgi_channel)
        logger.debug("publish %r on %s", body, queue_name)

        # Publish with publisher_confirms=True. Assume the server is configured
        # with `overflow: reject-publish`, so we get a basic.nack if the queue
        # length is exceeded.
        try:
            await channel.basic_publish(body, routing_key=queue_name)
        except DeliveryError:
            raise ChannelFull()

    async def receive(self, asgi_channel):
        """Receive the first message that arrives on the channel.

        If more than one coroutine waits on the same channel, only one waiter
        will receive the message when it arrives.
        """
        assert channel_to_queue_name(asgi_channel) == self.queue_name
        return await self._incoming_messages.get(asgi_channel)

    @stall_until_connected_or_closed
    async def group_add(self, channel, group, asgi_channel):
        """Register to receive messages for ``group`` on RabbitMQ.

        Upon reconnect, this Connection will re-register every group ... but
        any messages sent while disconnected won't reach it.
        """
        assert (
            channel_to_queue_name(asgi_channel) == self.queue_name
        ), "This layer won't let you add a channel from another connection"

        async with self._groups_lock:
            n_bindings = self._incoming_messages.group_add(group, asgi_channel)
            if n_bindings == 1:
                logger.debug("Binding queue %s to group %s", self.queue_name, group)
                # This group is new to our connection-level queue. Make a
                # connection-level binding.
                await channel.queue_bind(
                    self.queue_name, self.groups_exchange, routing_key=group
                )

    async def group_discard(self, group, asgi_channel):
        """No longer receive messages for ``group`` on RabbitMQ.

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
                    self.queue_name, self.groups_exchange, routing_key=group
                )

    @stall_until_connected_or_closed
    async def group_send(self, channel, group, message):
        message = {**message, "__asgi_group__": group}
        body = msgpack.packb(message, use_bin_type=True)

        logger.debug("group_send %r to %s", message, group)

        try:
            await channel.basic_publish(
                body, exchange=self.groups_exchange, routing_key=group
            )
        except aiormq.exceptions.DeliveryError:
            # "Sending to a group never raises ChannelFull; instead, it must
            # silently drop the message if it is over capacity, as per ASGI’s
            # at-most-once delivery policy."
            # https://channels.readthedocs.io/en/stable/channel_layer_spec.html#capacity
            #
            # ... let's at least _warn_....
            logger.warning("Aborting send to group %s: a queue is at capacity", group)

    async def close(self):
        self._is_closed = True

        if self._connection is not None:
            await self._connection.close()

        # Wait for self._connect_forever() to exit.
        try:
            await self.worker
        except asyncio.CancelledError:
            pass
        finally:
            # close our queues. pending_puts' messages will be ignored, and the
            # tasks will be completed.
            self._incoming_messages.close()

            try:
                await self.expiry_worker
            except asyncio.CancelledError:
                pass
