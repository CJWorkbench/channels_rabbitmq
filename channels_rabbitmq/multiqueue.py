from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict

from channels.exceptions import StopConsumer

from .util import ChannelRecipient, GroupRecipient, Recipient

BackpressureWarningInterval = 5.0  # seconds
ExpiryWarningInterval = 5.0  # seconds

logger = logging.getLogger(__name__)


class _Message:
    """Message passing through the MultiQueue."""

    data: Dict[str, Any]
    """The message to deliver."""

    n_queues: int
    """Number of queues where this message is waiting.

    If the message is waiting in 0 queues, it must be acked.
    """

    time: float
    """Absolute time when this message was queued.

    This is event-loop time, as used in `loop.call_at()`. See
    https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.call_at
    """

    ack: Callable[[], None]
    """Function to call exactly once, after the message has reached a consumer.

    The function must be a no-op after disconnect. (It mustn't send `ack` over
    another connection.) It may silently drop the ack -- but only when the
    connection is closing or closed. (Once the connection closes, the queue will
    disappear; that will implicitly ack this message.)
    """

    delivery_tag: int
    """Argument to ack()."""

    def __init__(
        self,
        data: Dict[str, Any],
        n_queues: int,
        time: float,
        ack: Callable[[], None],
    ):
        self.data = data
        self.n_queues = n_queues
        self.time = time
        self.ack = ack

    def mark_delivered(self):
        self.n_queues -= 1
        if self.n_queues == 0:
            self.ack()


class _ChannelQueue:
    """An "inner" queue: a single Django-Channels channel's _Messages.

    This solves a problem in the Django Channels Specification: a channel
    can exist without the layer knowing about it.

    Steps to reproduce the problem:

        1. Sender: send a message
        2. Recipient: receive the message

    This is backwards: the sender sent a message before the recipient
    even existed! But yep, that's the spec. The recipient _polls_ for
    messages, so it doesn't exist when it's busy or during startup.

    Hence _ChannelQueue: it's like asyncio.queue(), but with a notion of
    "unused". An unused queue is an empty queue with no getter.
    """

    def __init__(self):
        self._queue: asyncio.Queue[_Message] = asyncio.Queue()

    def unused(self) -> bool:
        """True if there are no queued messages and no pending getters."""
        return self._queue.empty() and not self._queue._getters

    def put_nowait(self, message: _Message) -> None:
        self._queue.put_nowait(message)

    async def get(self) -> Dict[str, Any]:
        """Wait for a message to become available, then return it."""
        message = await self._queue.get()
        message.mark_delivered()
        return message.data

    def remove_older_than(self, message_time: float) -> bool:
        """Remove (and mark_delivered()) _Messages we won't use.

        Return True if anything was deleted.
        """
        retval = False
        # We need to peek: self._queue is an asyncio.Queue, and
        # self._queue._queue is the collections.deque() _inside_ it.
        while not self._queue.empty() and self._queue._queue[0].time < message_time:
            message = self._queue._queue.popleft()
            message.mark_delivered()
            retval = True

        return retval

    @property
    def oldest_message_time(self) -> float:
        """The earliest "expires" of all queued messages."""
        return self._queue._queue[0].time

    def close(self) -> None:
        """Raise StopConsumer on all readers."""
        for waiter in self._queue._getters:
            if not waiter.done():
                waiter.set_exception(StopConsumer("Django Channel is shutting down"))


class MultiQueue:
    """Asyncio-friendly one-to-many queue that blocks when capacity is reached.

    Usage:

        await multi_queue.put_nowait("q1", Message({"foo": "hey"}))
        await multi_queue.put_nowait("q2", Message({"foo": "jude"}))
        await multi_queue.get("q2")  # => {"foo": "jude"}

    Ordering guarantee: on a given queue, messages are retrieved in FIFO order.

    MultiQueue gives back-pressure. Messages are only acked when delivered.
    RabbitMQ will never send us more than `prefetch_count` un-acked messages,
    so the MultiQueue will never hold more than `prefetch_count` messages.
    Excess messages will queue within RabbitMQ itself; depending on
    `remote_capacity`, this backlog may lead senders to raise `ChannelFull`.
    When MultiQueue holds `prefetch_count` messages, we log a warning.

    MultiQueue also manages local groups. "Groups" are a distributed concept:
    10 channels may all be subscribed to a group, across three servers. Here,
    we deal with the 3-4 channels on this server which are subscribed to the
    group.

    To prevent overflow, each message has an expiry time. We expire old messages
    to make room for new ones. In this event, we ack the old messages and log
    a warning.
    """

    def __init__(self, capacity):
        self.capacity = capacity
        self.n = 0

        self._local_groups = {}  # group => {channel, ...}
        self._channels = {}  # channel => _ChannelQueue
        self._nonempty = asyncio.Event()
        self._closed = asyncio.Event()
        # We'll order puts using a lock. Waiters on an asyncio.Lock are
        # released in FIFO order.
        self._putter_lock = asyncio.Lock()
        self._last_logged_backpressure = 0  # loop.time() result
        self._last_logged_expiry = 0  # loop.time() result

    def _drop_all_older_than(self, message_time: float) -> bool:
        """Drop expired messages; return `true` if something happened."""
        deleted = False
        channels_to_delete = []
        for channel, queue in self._channels.items():
            if queue.remove_older_than(message_time):
                deleted = True
            if queue.unused():
                channels_to_delete.append(channel)
        for channel in channels_to_delete:
            del self._channels[channel]
        return deleted

    async def expire_locally_until_closed(self, local_expiry: float):
        loop = asyncio.get_running_loop()
        closed_task = asyncio.create_task(self._closed.wait())

        while not closed_task.done():
            while not self._nonempty.is_set():
                nonempty_task = asyncio.create_task(self._nonempty.wait())
                await asyncio.wait(
                    {nonempty_task, closed_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if closed_task.done():
                    return

            # Assume messages expire in FIFO order. Therefore, we know there's
            # no need to check for expiry until the first unhandled message
            # expires.
            oldest_message_time = min(
                [
                    queue.oldest_message_time
                    for queue in self._channels.values()
                    if not queue._queue.empty()
                ]
            )
            now = loop.time()
            if oldest_message_time > now - local_expiry:
                await asyncio.wait(
                    {closed_task}, timeout=oldest_message_time + local_expiry - now
                )
                if closed_task.done():
                    return
                now = loop.time()

            dropped = self._drop_all_older_than(now - local_expiry)
            if dropped:
                self._log_local_expiry_debounced(now)

    def _increase_n(self):
        assert self.n < self.capacity
        self.n += 1  # decreased in self._cleanup_message()
        if self.n == self.capacity:
            now = asyncio.get_running_loop().time()
            self._log_backpressure_debounced(now)
        self._nonempty.set()

    def _decrease_n(self) -> None:
        self.n -= 1
        if self.n == 0:
            self._nonempty.clear()

    def put_nowait(
        self,
        recipient: Recipient,
        data: Dict[str, Any],
        time: float,
        ack: Callable[[], None],
    ) -> None:
        """Queue a message.

        This will call `ack` when the message is either delivered (to all
        recipients) or expired.

        For a GroupRecipient, recipient channels are chosen at call time. When
        a message is delivered, the group's members may have changed. A
        consumer calling `get()` may receive a message after it called
        `group_discard()` -- as long as the `put_nowait()` happened before
        `group_discard()`.
        """
        if isinstance(recipient, ChannelRecipient):
            channels = [recipient.channel]
        else:
            assert isinstance(recipient, GroupRecipient)
            try:
                channels = self._local_groups[recipient.group]
            except KeyError:
                ack()
                return

        def ack_and_decrease_n():
            ack()
            self._decrease_n()

        message = _Message(data, len(channels), time, ack=ack_and_decrease_n)

        for channel in channels:
            if channel not in self._channels:
                self._channels[channel] = _ChannelQueue()
            self._channels[channel].put_nowait(message)

        self._increase_n()

    def _build_big_queues_str(self) -> str:
        blockers = [(name, len(q._queue._queue)) for name, q in self._channels.items()]
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

    async def get(self, channel: str) -> Any:
        """Maybe wait for a `put_channel()` on `channel`; return its data."""
        if self._closed.is_set():
            raise StopConsumer("Django Channel was shut down")

        if channel not in self._channels:
            self._channels[channel] = _ChannelQueue()

        try:
            return await self._channels[channel].get()
        finally:  # Even if it raises StopConsumer
            if (
                channel in self._channels  # it may have been deleted in await?
                and self._channels[channel].unused()
            ):
                del self._channels[channel]

    def group_add(self, group, channel):
        channels = self._local_groups.setdefault(group, set())
        channels.add(channel)
        return len(channels)

    def group_discard(self, group, channel):
        """Remove `channel` from `group` and return n_channels_remaining.

        Return None if the channel is not in the group.
        """
        if group not in self._local_groups:
            return None  # don't create set

        channels = self._local_groups[group]
        try:
            channels.remove(channel)
        except KeyError:
            return None  # it was already removed

        ret = len(channels)

        if ret == 0:
            del self._local_groups[group]

        return ret

    def close(self):
        """Nullify pending puts; raise StopConsumer on pending gets."""
        if self._closed.is_set():
            return
        self._closed.set()

        # Cancel all gets
        for out_queue in self._channels.values():
            out_queue.close()
        self._channels.clear()
