import asyncio
import random
import string
import threading
import types

from channels.layers import BaseChannelLayer

from .connection import Connection


class RabbitmqChannelLayer(BaseChannelLayer):
    """
    RabbitMQ channel layer.

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

    The `prefetch_count` determines the maximum number of messages we can
    receive from RabbitMQ at a time. This makes `local_capacity` a bit of a
    "loose" setting: if messages arrive quickly enough, we may end up with
    `prefetch_count + local_capacity - 1` messages in memory.

    There is also an "expiry" parameter: this determines the minimum number of
    seconds a message must remain in RabbitMQ before being culled.

    This layer does not implement "flush". To flush all state, simply
    disconnect all clients.
    """

    def __init__(
        self,
        host="amqp://guest:guest@127.0.0.1/asgi",
        local_capacity=100,
        remote_capacity=100,
        prefetch_count=10,
        expiry=60,
        group_expiry=86400,
    ):
        self.host = host
        self.local_capacity = local_capacity
        self.remote_capacity = remote_capacity
        self.prefetch_count = prefetch_count
        self.expiry = expiry
        self.group_expiry = 86400

        # In inefficient client code (e.g., async_to_sync()), there may be
        # several send() or receive() calls within different event loops --
        # meaning callers might be coming from different threads at the same
        # time. We'll use a threading.Lock() when absolutely necessary to
        # maintain this dict of loop+connection.
        self._connections_lock = threading.Lock()
        self._connections = {}  # loop => Connection

    extensions = ["groups"]

    def _create_connection(self, loop):
        """
        Start a new connection on the given event loop.

        Wrap the event loop's close() with a connection.close() call.
        """
        # Choose queue name here: that way we can declare it on RabbitMQ with
        # exclusive=True and have it survive reconnections.
        rand = "".join(random.choice(string.ascii_letters) for i in range(12))
        queue_name = f"channels_{rand}"

        connection = Connection(
            loop,
            self.host,
            queue_name,
            local_capacity=self.local_capacity,
            remote_capacity=self.remote_capacity,
            prefetch_count=self.prefetch_count,
            expiry=self.expiry,
            group_expiry=self.group_expiry,
        )
        self._connections[loop] = connection  # assume lock is held

        original_impl = loop.close
        manager = self

        def _wrapper(self, *args, **kwargs):  # self = loop
            # If the event loop was closed, there's nothing we can do anymore.
            if not self.is_closed():
                self.run_until_complete(connection.close())

            with manager._connections_lock:
                if self in manager._connections:
                    del manager._connections[self]

            # Restore the original close() implementation after we're done.
            self.close = original_impl
            return self.close(*args, **kwargs)

        loop.close = types.MethodType(_wrapper, loop)

        return connection

    def _get_connection_for_loop(self, loop=None):
        if loop is None:
            loop = asyncio.get_event_loop()

        try:
            return self._connections[loop]
        except KeyError:
            with self._connections_lock:
                if loop not in self._connections:  # Test again, with a lock.
                    self._connections[loop] = self._create_connection(loop)

                return self._connections[loop]

    async def send(self, channel, message):
        """
        Send a message onto a (general or specific) channel.
        """
        assert isinstance(message, dict), "message is not a dict"
        assert self.valid_channel_name(channel), "Channel name not valid"
        assert "__asgi_channel__" not in message
        assert "__asgi_group__" not in message
        assert "!" in channel

        connection = self._get_connection_for_loop()
        await connection.send(channel, message)

    async def receive(self, channel):
        """
        Receive the first message that arrives on the channel.

        If more than one coroutine waits on the same channel, only one waiter
        will receive the message when it arrives.
        """
        # Make sure the channel name is valid then get the non-local part
        # and thus its index
        assert self.valid_channel_name(channel)
        assert "!" in channel

        connection = self._get_connection_for_loop()
        return await connection.receive(channel)

    async def new_channel(self, prefix=""):
        """
        Create a new channel name that can be used by something in our
        process as a specific channel.
        """
        connection = self._get_connection_for_loop()
        return "!".join(
            [
                connection.queue_name,
                (
                    prefix
                    + "".join(random.choice(string.ascii_letters) for i in range(12))
                ),
            ]
        )

    async def group_add(self, group, channel):
        """
        Add the channel name to a group.

        Spec deviation: `channel` must have been created on the same event loop
        as the RabbitMQ connection. In other words: you can't subscribe someone
        else's channel to a group.
        """
        assert self.valid_group_name(group), "Group name not valid"
        assert self.valid_channel_name(channel), "Channel name not valid"

        connection = self._get_connection_for_loop()
        await connection.group_add(group, channel)

    async def group_discard(self, group, channel):
        """
        Remove the channel from the named group if it is in the group;
        does nothing otherwise (does not error)

        Spec deviation: `channel` must have been created on the same event loop
        as the RabbitMQ connection. In other words: you can't subscribe someone
        else's channel to a group.
        """
        assert self.valid_group_name(group), "Group name not valid"
        assert self.valid_channel_name(channel), "Channel name not valid"

        connection = self._get_connection_for_loop()
        await connection.group_discard(group, channel)

    async def group_send(self, group, message):
        """
        Send a message to the entire group.
        """
        assert isinstance(message, dict), "message is not a dict"
        assert self.valid_group_name(group), "Group name not valid"
        assert "__asgi_channel__" not in message
        assert "__asgi_group__" not in message

        connection = self._get_connection_for_loop()
        await connection.group_send(group, message)
