from __future__ import annotations

import asyncio
import logging
import random
import string
import warnings
from typing import Iterable, Optional

import carehare
from channels.exceptions import ChannelFull, StopConsumer
from channels.layers import BaseChannelLayer

from .multiqueue import MultiQueue
from .reader import consume_into_multi_queue_until_connection_close
from .util import (
    ChannelRecipient,
    GroupRecipient,
    gather_without_leaking,
    serialize_message,
)

logger = logging.getLogger(__name__)

ReconnectDelay = 1.0  # seconds


async def _setup_connection(
    *,
    connection: carehare.Connection,
    queue_name: str,
    remote_capacity: Optional[int],
    expiry: Optional[int],
    groups_exchange: str
) -> None:
    # Declare "groups" exchange. It may persist; spurious declarations
    # (such as on reconnect) are harmless.
    await connection.exchange_declare(groups_exchange, exchange_type="direct")

    arguments = {}
    if remote_capacity is not None:
        arguments["x-max-length"] = remote_capacity
        arguments["x-overflow"] = "reject-publish"
    if expiry is not None:
        arguments["x-message-ttl"] = int(expiry * 1000)
    # This is an exclusive queue, because we want it to disappear when we
    # shut down. (Otherwise we'd leak queues every deploy.) And since an
    # exclusive queue disappears when we aren't connected, there's no way
    # to preserve every message across connects. That's okay -- disconnect
    # means one or two lost messages and nothing more.
    await connection.queue_declare(queue_name, exclusive=True, arguments=arguments)


async def _setup_subscriptions(
    *,
    connection: carehare.Connection,
    queue_name: str,
    groups_exchange: str,
    groups: Iterable[str]
) -> None:
    await gather_without_leaking(
        connection.queue_bind(
            queue_name=queue_name, exchange_name=groups_exchange, routing_key=group
        )
        for group in groups
    )


def _assert_channel_is_reply_channel(channel_name: str) -> None:
    if "!" not in channel_name:
        raise RuntimeError(
            """
***
You are trying to use "Normal Channels", as per
https://channels.readthedocs.io/en/1.x/concepts.html#channel-types.

channels_rabbitmq does not support Normal Channels. See
https://github.com/CJWorkbench/channels_rabbitmq/pull/11#issuecomment-499185070
for an explanation. You can build a pull request if you really want
this; but we heartily recommend you implement your work queue using
carehare directly. Your service will be simpler and it will scale better.
***"""
        )


def _random_letters(n: int) -> str:
    return "".join(random.choice(string.ascii_letters) for i in range(n))


class RabbitmqChannelLayer(BaseChannelLayer):
    """RabbitMQ channel layer.

    It routes all messages into a remote RabbitMQ server.

    Queue names look like "channels_asldkjfg"; Channel names look like
    "channels_asldkjfg!asdaSDGdlkgj". RabbitMQ only sees the queue name; the
    channel names are embedded in `message["__asgi_channel__"]`. Each
    Connection is responsible for a single queue and multiple channels on that
    queue. We call that queue a "channel_key".

    There are two "capacity" parameters: `remote_capacity` determines how many
    messages RabbitMQ will queue before a send raises `ChannelFull`.
    `local_capacity` determines the maximum number of messages to hold in
    memory in our ever-running pull-messages-from-RabbitMQ loop. If consumers
    are too slow then `local_capacity` messages will be buffered in memory; at
    that point Connection won't pull any more from RabbitMQ; so senders can
    send another `remote_capacity` messages to RabbitMQ and then they'll start
    raising `ChannelFull`.

    There is also an "expiry" parameter: this determines the minimum number of
    seconds a message must remain in RabbitMQ before being culled. A similar
    `local_expiry` determines the minimum amount of time a message must remain
    in the channels_rabbitmq buffers before it is acked (and potentially not
    delivered to all consumers).

    This layer does not implement "flush". To flush all state, simply
    disconnect all clients.
    """

    extensions = ["groups"]

    def __init__(
        self,
        host="amqp://guest:guest@127.0.0.1/asgi",
        local_capacity=100,
        remote_capacity=100,
        expiry=60,
        local_expiry: Optional[float] = None,
        group_expiry=None,
        ssl_context=None,
        groups_exchange="groups",
    ):
        self.host = host
        self.connect_timeout = 10.0  # seconds
        self.local_capacity = local_capacity
        self.remote_capacity = remote_capacity
        self.expiry = expiry
        self.local_expiry = local_expiry
        self.ssl_context = ssl_context
        self.groups_exchange = groups_exchange

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(
                """Refusing to initialize channel layer without a running event loop.

If you're writing a django-channels Worker, channels_rabbitmq does not support
this. Read the way to write workers in the carehare documentation.

If you're calling `async_to_sync()`, the call must be within code run by
`sync_to_async()`. Django Channels guarantees this for Django views. Elsewhere,
beware: `asgiref` *does* let you to call `async_to_sync()` without
`sync_to_async()`, but `channels_rabbitmq` *doesn't* -- hence this error."""
            )

        if group_expiry is not None:
            warnings.warn(
                (
                    "channels_rabbitmq does not support group_expiry. Please do not configure it. "
                    "For rationale, see "
                    "https://github.com/CJWorkbench/channels_rabbitmq/issues/18#issuecomment-547052373"
                ),
                category=DeprecationWarning,
            )

        # Choose queue name here: that way we can declare it on RabbitMQ with
        # exclusive=True and have it survive reconnections.
        self._queue_name = "channels_{rand}".format(rand=_random_letters(12))

        self._multi_queue = MultiQueue(capacity=local_capacity)
        self._carehare_connection: asyncio.Future[
            carehare.Connection
        ] = asyncio.Future()
        self._want_close: bool = False

    @property
    def carehare_connection(self) -> asyncio.Future[carehare.Connection]:
        if self._want_close:
            raise StopConsumer
        if self._carehare_connection.get_loop() != asyncio.get_running_loop():
            raise RuntimeError(
                "The caller tried using channels_rabbitmq on a different event loop than the one it was initialized with."
            )
        if not self._want_close and not hasattr(self, "_reconnect_forever_task"):
            self._reconnect_forever_task = asyncio.create_task(
                self._reconnect_forever()
            )
            if self.local_expiry is not None:
                self._expire_task = asyncio.create_task(
                    self._multi_queue.expire_locally_until_closed(
                        local_expiry=self.local_expiry
                    )
                )
        return self._carehare_connection

    async def _reconnect_forever(self):
        """Maintain self._carehare_connection.

        Cancel this task on event-loop shutdown (and at no other time). This
        will make `self.carehare_connection` raise `asyncio.CancelledError`.
        """
        EXPECTED_EXCEPTIONS = (
            carehare.ConnectionClosedByServer,
            carehare.ConnectionClosedByHeartbeatMonitor,
            OSError,
        )

        while not self._want_close:  # Reconnect (after disconnect) repeatedly
            while not self._want_close:  # Retry initial connect repeatedly
                try:
                    connection = carehare.Connection(
                        url=self.host,
                        connect_timeout=self.connect_timeout,
                        ssl=self.ssl_context,
                    )
                    await connection.connect()
                except (asyncio.TimeoutError, *EXPECTED_EXCEPTIONS) as err:
                    logger.exception(
                        "Failure connecting to RabbitMQ: %s. Retrying", str(err)
                    )
                    await asyncio.sleep(ReconnectDelay)
                    connection = None  # in case _want_close=True
                    continue  # retry

                try:
                    await _setup_connection(
                        connection=connection,
                        queue_name=self._queue_name,
                        remote_capacity=self.remote_capacity,
                        expiry=self.expiry,
                        groups_exchange=self.groups_exchange,
                    )
                    await _setup_subscriptions(
                        connection=connection,
                        queue_name=self._queue_name,
                        groups_exchange=self.groups_exchange,
                        groups=self._multi_queue._local_groups.keys(),
                    )
                except (carehare.ChannelClosedByServer, *EXPECTED_EXCEPTIONS) as err:
                    logger.exception(
                        "Failure declaring RabbitMQ objects: %s. Will retry.",
                        str(err),
                    )
                    await asyncio.sleep(ReconnectDelay)
                    connection = None  # in case _want_close=True
                    continue  # retry

                self._carehare_connection.set_result(connection)
                logger.info("Connected to RabbitMQ")
                break

            if self._want_close:
                break

            try:
                await consume_into_multi_queue_until_connection_close(
                    connection=connection,
                    channel=self._queue_name,
                    multi_queue=self._multi_queue,
                    prefetch_count=self.local_capacity,
                )
                await connection.closed
            except EXPECTED_EXCEPTIONS as err:
                logger.exception(
                    "Disconnected from RabbitMQ: %s. Will reconnect.", str(err)
                )

            self._carehare_connection = asyncio.Future()
            try:
                await connection.close()
            except EXPECTED_EXCEPTIONS:
                pass
            connection = None

        if connection is not None:
            try:
                await connection.close()
            except EXPECTED_EXCEPTIONS as err:
                logger.exception(
                    "Failure closing RabbitMQ connection: %s. Moving on....", str(err)
                )

    async def close(self):
        """Close our RabbitMQ connection and all tasks.

        This is not part of the Channel Layer Specification; but it should be a
        part of every server's lifecycle.

        After calling this, no more messages will be delivered. All pending and
        future calls to `receive()` will raise `StopConsumer` (causing all
        WebSockets clients to disconnect). Undelivered messages will be lost.

        After this method returns, the RabbitMQ connection will be closed and
        all internal tasks will be completed.
        """

        if not self._want_close:
            self._want_close = True
            self._multi_queue.close()

            if hasattr(self, "_expire_task"):
                self._expire_task.cancel()
            if (
                hasattr(self, "_reconnect_forever_task")
                and self._carehare_connection.done()
            ):
                await self._carehare_connection.result().close()

        if hasattr(self, "_expire_task"):
            try:
                await self._expire_task
            except asyncio.CancelledError:
                pass
            if hasattr(self, "_expire_task"):
                delattr(self, "_expire_task")

        if hasattr(self, "_reconnect_forever_task"):
            await self._reconnect_forever_task
            if hasattr(self, "_reconnect_forever_task"):
                delattr(self, "_reconnect_forever_task")

    async def send(self, channel, message):
        """Send a message onto a (general or specific) channel."""
        assert self.valid_channel_name(channel), "Channel name not valid"
        _assert_channel_is_reply_channel(channel)

        logger.debug("send %r to %s", message, channel)

        body = serialize_message(ChannelRecipient(channel), message)

        connection = await self.carehare_connection

        try:
            await connection.publish(
                body, exchange_name="", routing_key=channel.split("!")[0]
            )
        except carehare.ServerSentNack:
            raise ChannelFull from None
        except carehare.ConnectionClosed:
            logger.warning(
                "Failed to send message %r to %s because we disconnected",
                message,
                channel,
            )

    async def receive(self, channel):
        """Receive the first message that arrives on the channel.

        If more than one coroutine waits on the same channel, only one waiter
        will receive the message when it arrives.
        """
        # Make sure the channel name is valid then get the non-local part
        # and thus its index
        assert self.valid_channel_name(channel)
        _assert_channel_is_reply_channel(channel)
        assert channel.startswith(self._queue_name + "!")
        return await self._multi_queue.get(channel)

    async def new_channel(self, prefix=""):
        """Create a new channel name that can be used by something in our
        process as a specific channel.
        """
        return "%s!%s" % (self._queue_name, _random_letters(12))

    async def group_add(self, group, channel):
        """Add the channel name to a group.

        Spec deviation: `channel` must have been created on the same event loop
        as the RabbitMQ connection. In other words: you can't subscribe someone
        else's channel to a group.
        """
        assert self.valid_group_name(group), "Group name not valid"
        assert self.valid_channel_name(channel), "Channel name not valid"

        # Connect, to make sure the next line doesn't happen during _setup_subscriptions()
        connection = await self.carehare_connection

        # Now, assume _setup_subscriptions() isn't running, so there's no race
        # writing+reading self._multi_queue.
        n_bindings = self._multi_queue.group_add(group, channel)
        if n_bindings == 1:
            logger.debug("Binding queue %s to group %s", self._queue_name, group)
            # This group is new to our connection-level queue. Make a
            # connection-level binding.

            try:
                await connection.queue_bind(
                    queue_name=self._queue_name,
                    exchange_name=self.groups_exchange,
                    routing_key=group,
                )
            except carehare.ChannelClosedByServer as err:
                logger.exception(
                    "RabbitMQ refused to bind: %s. Messages to group %s will not reach clients on this server.",
                    str(err),
                    group,
                )
            except carehare.ConnectionClosed:
                pass  # we'll queue_bind() during reconnect

    async def group_discard(self, group, channel):
        """Remove the channel from the named group if it is in the group;
        does nothing otherwise (does not error)

        Spec deviation: `channel` must have been created on the same event loop
        as the RabbitMQ connection. In other words: you can't subscribe someone
        else's channel to a group.
        """
        assert self.valid_group_name(group), "Group name not valid"
        assert self.valid_channel_name(channel), "Channel name not valid"

        # Connect, to make sure the next line doesn't happen during _setup_subscriptions()
        connection = await self.carehare_connection

        # Now, assume _setup_subscriptions() isn't running, so there's no race
        # writing+reading self._multi_queue.
        n_bindings = self._multi_queue.group_discard(group, channel)
        if n_bindings == 0:
            logger.debug("Unbinding queue %s from group %s", self._queue_name, group)
            try:
                await connection.queue_unbind(
                    queue_name=self._queue_name,
                    exchange_name=self.groups_exchange,
                    routing_key=group,
                )
            except carehare.ChannelClosedByServer as err:
                logger.exception(
                    "RabbitMQ refused to unbind from group %s: %s. This suggests a bug somewhere.",
                    str(err),
                    group,
                )
            except carehare.ConnectionClosed:
                pass  # after reconnect, we're unbound anyway

    async def group_send(self, group, message):
        """Send a message to the entire group."""
        assert self.valid_group_name(group), "Group name not valid"

        logger.debug("group_send %r to %s", message, group)

        body = serialize_message(GroupRecipient(group), message)

        connection = await self.carehare_connection

        try:
            await connection.publish(
                body, exchange_name=self.groups_exchange, routing_key=group
            )
        except carehare.ServerSentNack:
            # "Sending to a group never raises ChannelFull; instead, it must
            # silently drop the message if it is over capacity, as per ASGI’s
            # at-most-once delivery policy."
            # https://channels.readthedocs.io/en/stable/channel_layer_spec.html#capacity
            #
            # ... let's at least _warn_....
            logger.warning("Aborting send to group %s: a queue is at capacity", group)
        except carehare.ConnectionClosed:
            logger.warning(
                "Failed to send message %r to group %s because we disconnected",
                message,
                group,
            )
