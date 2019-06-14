import asyncio
import ssl
import time
from pathlib import Path

import pytest
from aioamqp.exceptions import ChannelClosed

from channels.exceptions import ChannelFull
from channels_rabbitmq.connection import Connection, ReconnectDelay

HOST = "amqp://guest:guest@localhost/"
SSL_CONTEXT = ssl.create_default_context(
    cafile=str(Path(__file__).parent.parent / "ssl" / "server.cert")
)
SSL_CONTEXT.load_cert_chain(
    certfile=str(Path(__file__).parent.parent / "ssl" / "client.certchain"),
    keyfile=str(Path(__file__).parent.parent / "ssl" / "client.key"),
)


def ASYNC_TEST(fn):
    return pytest.mark.timeout(8)(pytest.mark.asyncio(fn))


@pytest.fixture
async def connect():
    connections = []

    def factory(queue_name, **kwargs):
        kwargs = {
            "host": HOST,
            "queue_name": queue_name,
            "ssl_context": SSL_CONTEXT,
            **kwargs,
        }

        connection = Connection(loop=asyncio.get_event_loop(), **kwargs)
        connections.append(connection)
        return connection

    yield factory

    for connection in connections:
        await connection.close()
    connections = []


@ASYNC_TEST
async def test_send_capacity(connect, caplog):
    """
    Makes sure we get ChannelFull when our in-memory structure runs out of
    memory.
    """
    connection = connect("x", remote_capacity=1, local_capacity=1, prefetch_count=1)
    await connection.send("x!y", {"type": "test.message1"})  # one queued+acked
    await connection.send("x!y", {"type": "test.message2"})  # one unacked
    await connection.send("x!y", {"type": "test.message3"})  # one ready
    with pytest.raises(ChannelFull):
        await connection.send("x!y", {"type": "test.message4"})
    assert "Back-pressuring. Biggest queues: x!y (1)" in caplog.text

    # Test that even after error, the queue works as expected.

    # Receive the acked message1. This will _eventually_ ack message2. RabbitMQ
    # will have unacked=0, ready=1. This will prompt it to send a new unacked
    # message.
    assert (await connection.receive("x!y"))["type"] == "test.message1"

    # Receive message2. This _guarantees_ message2 is acked.
    assert (await connection.receive("x!y"))["type"] == "test.message2"

    # Send message5. We're sending and receiving on the same TCP connection, so
    # RabbitMQ is aware that message2 was acked by the time we send message5.
    # That means its queue isn't maxed out any more.
    await connection.send("x!y", {"type": "test.message5"})  # one ready

    assert (await connection.receive("x!y"))["type"] == "test.message3"
    assert (await connection.receive("x!y"))["type"] == "test.message5"


@ASYNC_TEST
async def test_process_local_send_receive(connect):
    """
    Makes sure we can send a message to a process-local channel then receive it.
    """
    connection = connect("x")
    await connection.send("x!y", {"type": "test.message"})
    message = await connection.receive("x!y")
    assert message["type"] == "test.message"


@ASYNC_TEST
async def test_process_remote_send_receive(connect):
    """
    Makes sure we can send a message to a process-local channel then receive it.
    """
    connection1 = connect("x")
    connection2 = connect("y")

    # Make sure connection2's queue is created. A dummy `send()` will do it,
    # since it only completes after the queue is created.
    await connection2.send("nonexistent!channel", {"type": "no-op"})

    await connection1.send("y!y", {"type": "remote"})  # connection2 only
    await connection1.send("x!y", {"type": "local"})  # connection1 only

    assert (await connection2.receive("y!y"))["type"] == "remote"
    assert (await connection1.receive("x!y"))["type"] == "local"  # no remote


@ASYNC_TEST
async def test_multi_send_receive(connect):
    """
    Tests overlapping sends and receives, and ordering.
    """
    connection = connect("x")
    await connection.send("x!y", {"type": "message.1"})
    await connection.send("x!y", {"type": "message.2"})
    await connection.send("x!y", {"type": "message.3"})
    assert (await connection.receive("x!y"))["type"] == "message.1"
    assert (await connection.receive("x!y"))["type"] == "message.2"
    assert (await connection.receive("x!y"))["type"] == "message.3"


@ASYNC_TEST
async def test_reject_bad_channel(connect):
    """
    Makes sure sending/receiving on an invalid channel name fails.
    """
    connection = connect("x")
    with pytest.raises(AssertionError):
        await connection.receive("y!y")


@ASYNC_TEST
async def test_groups_local(connect):
    """
    Tests basic group operation.
    """
    connection = connect("x")
    await connection.group_add("test-group", "x!1")
    await connection.group_add("test-group", "x!2")
    await connection.group_add("test-group", "x!3")
    await connection.group_discard("test-group", "x!2")
    await connection.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message on the two channels that were in
    assert (await connection.receive("x!1"))["type"] == "message.1"
    assert (await connection.receive("x!3"))["type"] == "message.1"

    # "x!2" is unsubscribed. It should receive _other_ messages, though.
    await connection.send("x!2", {"type": "message.2"})
    assert (await connection.receive("x!2"))["type"] == "message.2"


@ASYNC_TEST
async def test_groups_discard(connect):
    """
    Tests basic group operation.
    """
    connection = connect("x")
    await connection.group_add("test-group", "x!1")
    await connection.group_discard("test-group", "x!1")
    await connection.group_add("test-group", "x!1")
    await connection.group_discard("test-group", "x!1")
    await connection.group_send("test-group", {"type": "ignored"})

    # message was ignored. We should receive _other_ messages, though.
    await connection.send("x!1", {"type": "normal"})
    assert (await connection.receive("x!1"))["type"] == "normal"


@ASYNC_TEST
async def test_group_discard_when_not_connected(connect):
    """
    Tests basic group operation.
    """
    connection = connect("x")

    await connection.group_discard("test-group", "x!1")
    await connection.group_send("test-group", {"type": "ignored"})
    await connection.send("x!1", {"type": "normal"})
    assert (await connection.receive("x!1"))["type"] == "normal"


@ASYNC_TEST
async def test_groups_remote(connect):
    """
    Tests basic group operation.
    """
    connection1 = connect("x")
    connection2 = connect("y")

    await connection1.group_add("test-group", "x!1")
    await connection1.group_add("test-group", "x!2")
    await connection2.group_add("test-group", "y!3")
    await connection1.group_discard("test-group", "x!2")
    await connection2.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message on the two channels that were in
    assert (await connection1.receive("x!1"))["type"] == "message.1"
    assert (await connection2.receive("y!3"))["type"] == "message.1"

    # "x!2" is unsubscribed. It should receive _other_ messages, though.
    await connection2.send("x!2", {"type": "message.2"})
    assert (await connection1.receive("x!2"))["type"] == "message.2"


@ASYNC_TEST
async def test_groups_exchange(connect):
    """
    Tests customizable groups exchange.
    """
    connection1 = connect("x", groups_exchange="test-groups-exchange")
    connection2 = connect("y", groups_exchange="test-groups-exchange")
    connection3 = connect("z")

    await connection1.group_add("test-group", "x!1")
    await connection1.group_add("test-group", "x!2")
    await connection2.group_add("test-group", "y!3")
    await connection3.group_add("test-group", "z!4")
    await connection1.group_discard("test-group", "x!2")
    await connection2.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message on the two channels that were in
    assert (await connection1.receive("x!1"))["type"] == "message.1"
    assert (await connection2.receive("y!3"))["type"] == "message.1"

    # "x!2" is unsubscribed. It should receive _other_ messages, though.
    await connection2.send("x!2", {"type": "message.2"})
    assert (await connection1.receive("x!2"))["type"] == "message.2"

    # "z!4" is in separate (default) exchange 'groups'.
    # It should receive _other_ messages, though.
    await connection3.group_send("test-group", {"type": "message.4"})
    assert (await connection3.receive("z!4"))["type"] == "message.4"


@ASYNC_TEST
async def test_groups_channel_full(connect):
    """
    Tests that group_send ignores ChannelFull
    """
    connection = connect("x", local_capacity=1, remote_capacity=1, prefetch_count=1)
    await connection.group_add("test-group", "x!1")

    # Message 1:
    # * server acks and control flow returns.
    # * consumer receives message in background, adds it to queue, acks it.
    # After this, local_capacity is full.
    await connection.group_send("test-group", {"type": "message.1"})
    # Wait a few ms to make sure the message reaches the consumer
    await asyncio.sleep(0.01)

    # Message 2:
    # * server acks and control flow returns.
    # * consumer receives message and stalls trying to add it to queue. No more
    #   consuming happens: prefetch_count is full.
    await connection.group_send("test-group", {"type": "message.2"})
    # Wait a few ms to make sure the message reaches the consumer
    await asyncio.sleep(0.01)

    # Message 3:
    # * server acks and control flow returns.
    # After this, remote_capacity is full.
    await connection.group_send("test-group", {"type": "message.3"})
    await connection.group_send("test-group", {"type": "message.4"})  # rejected
    await connection.group_send("test-group", {"type": "message.5"})  # rejected

    assert (await connection.receive("x!1"))["type"] == "message.1"
    assert (await connection.receive("x!1"))["type"] == "message.2"
    assert (await connection.receive("x!1"))["type"] == "message.3"

    # aaaand things are back to normal now that we're below capacity
    await connection.group_send("test-group", {"type": "message.6"})
    assert (await connection.receive("x!1"))["type"] == "message.6"


@ASYNC_TEST
async def test_groups_no_such_group(connect):
    """
    Tests that group_send does nothing if there is no such group
    """
    connection = connect("x")
    await connection.group_send("my-group", {"type": "message.1"})

    # Now create the group, and check that new messages to that group will work
    await connection.group_add("my-group", "x!1")
    await connection.group_send("my-group", {"type": "message.2"})
    assert (await connection.receive("x!1"))["type"] == "message.2"


@ASYNC_TEST
async def test_receive_after_disconnect(connect):
    connection = connect("x")
    await asyncio.sleep(0)  # start connecting (it happens in the background)
    await connection.close()
    with pytest.raises(ChannelClosed):
        await connection.receive("x!1")


@ASYNC_TEST
async def test_receive_after_disconnect_before_connect_begins(connect):
    connection = connect("x")
    await connection.close()
    with pytest.raises(ChannelClosed):
        await connection.receive("x!1")


@ASYNC_TEST
async def test_disconnect_at_same_time_as_everything(connect):
    """
    If we disconnect before the connection is established, don't deadlock.
    """
    connection = connect("x")
    await asyncio.sleep(0)  # start connecting (it happens in the background)

    # Schedule all these commands to run simultaneously. At this point, the
    # connection isn't established yet and no queue has been created.
    #
    # Begin the close() first -- that'll make it happen before any other
    # command acquires its lock.
    close = connection.close()
    send = connection.send("x!1", {"type": "hi"})
    group_add = connection.group_add("g", "x!1")
    group_send = connection.group_send("g", {"type": "ghi"})
    group_discard = connection.group_discard("g", "x!1")
    receive = connection.receive("x!1")

    (
        close_r,
        send_r,
        group_add_r,
        group_send_r,
        group_discard_r,
        receive_r,
    ) = await asyncio.gather(
        close,
        send,
        group_add,
        group_send,
        group_discard,
        receive,
        return_exceptions=True,
    )

    assert close_r is None
    assert isinstance(send_r, ChannelClosed)
    assert isinstance(group_add_r, ChannelClosed)
    assert isinstance(group_send_r, ChannelClosed)
    assert group_discard_r is None
    assert isinstance(receive_r, ChannelClosed)


@ASYNC_TEST
async def test_log_connection_refused(connect, caplog):
    """
    There's nowhere to report a connection error: log it.

    Without this, admins and developers would have a hard time learning why
    the channel layer isn't sending messages.
    """
    connection = connect("x", host="amqp://guest:guest@localhost:4561/")
    await asyncio.sleep(0.5)  # Enough time to try connecting once

    assert "Connect call failed" in caplog.text
    assert "will retry" in caplog.text

    await connection.close()


@ASYNC_TEST
async def test_no_ssl(connect):
    """
    Connect through TCP, without TLS.

    Assumes the server is listening over both a TLS port and a no-TLS port.
    """
    connect("x", ssl_context=None)


@ASYNC_TEST
async def test_reconnect_on_queue_name_conflict(connect):
    """
    When reconnecting to a cluster, a race may leave your queue name declared.

    `channels_rabbitmq` should try to reconnect when that happens. (We assume
    there aren't two clients with the same queue_name; if they are, hopefully
    one of them is reading the flurry of errors in the logs.)

    https://github.com/CJWorkbench/channels_rabbitmq/issues/9
    """
    connection1 = connect("x")
    # Ensure the connection is alive and kicking
    await connection1.send("x!y", {"type": "test.1"})
    assert (await connection1.receive("x!y"))["type"] == "test.1"

    # Now simulate a race: here comes the same client, but the queue is already
    # declared! Oh no!
    connection2 = connect("x")

    # (The old connection will die 0.2s after we try to declare the queue.)
    async def close_slowly():
        """
        Close connection1, "slowly".

        For 0.2s, connection1 will be open and conflicting with connection2,
        preventing connection2 from connecting. After return, connection1 will
        be closed. (If we're connected to a RabbitMQ cluster there may _still_
        be a conflict even after return; but that shouldn't be a problem
        because the reconnect will _eventually_ succeed.
        """
        await asyncio.sleep(0.2)
        await connection1.close()

    t1 = time.time()
    future_closed = asyncio.get_event_loop().create_task(close_slowly())
    await connection2.send("x!y", {"type": "test.2"})
    await future_closed  # clean up
    t2 = time.time()
    assert t2 - t1 >= ReconnectDelay, "send() should stall until reconnect"
    assert (await connection2.receive("x!y"))["type"] == "test.2"
