import asyncio

import pytest
from aio_pika.exceptions import ChannelClosed

from channels.exceptions import ChannelFull
from channels_rabbitmq.connection import Connection

HOST = "amqp://guest:guest@localhost/"


@pytest.fixture
async def connect():
    connections = []

    def factory(queue_name, **kwargs):
        kwargs = {"host": HOST, "command_timeout": 0.5, **kwargs}

        connection = Connection(
            loop=asyncio.get_event_loop(), queue_name=queue_name, **kwargs
        )
        connections.append(connection)
        return connection

    yield factory

    for connection in connections:
        await connection.close()
    connections = []


@pytest.mark.asyncio
async def test_send_capacity(connect):
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


@pytest.mark.asyncio
async def test_process_local_send_receive(connect):
    """
    Makes sure we can send a message to a process-local channel then receive it.
    """
    connection = connect("x")
    await connection.send("x!y", {"type": "test.message"})
    message = await connection.receive("x!y")
    assert message["type"] == "test.message"


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_reject_bad_channel(connect):
    """
    Makes sure sending/receiving on an invalid channel name fails.
    """
    connection = connect("x")
    with pytest.raises(AssertionError):
        await connection.receive("y!y")


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_group_discard_when_not_connected(connect):
    """
    Tests basic group operation.
    """
    connection = connect("x")

    await connection.group_discard("test-group", "x!1")
    await connection.group_send("test-group", {"type": "ignored"})
    await connection.send("x!1", {"type": "normal"})
    assert (await connection.receive("x!1"))["type"] == "normal"


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_groups_channel_full(connect):
    """
    Tests that group_send ignores ChannelFull
    """
    connection = connect("x", local_capacity=1, remote_capacity=1, prefetch_count=1)
    await connection.group_add("test-group", "x!1")
    await connection.group_send("test-group", {"type": "message.1"})  # acked
    await connection.group_send("test-group", {"type": "message.2"})  # unacked
    await connection.group_send("test-group", {"type": "message.3"})  # ready
    await connection.group_send("test-group", {"type": "message.4"})  # rejected
    await connection.group_send("test-group", {"type": "message.5"})  # rejected

    assert (await connection.receive("x!1"))["type"] == "message.1"
    assert (await connection.receive("x!1"))["type"] == "message.2"
    assert (await connection.receive("x!1"))["type"] == "message.3"

    # aaaand things are back to normal now that we're below capacity
    await connection.group_send("test-group", {"type": "message.6"})
    assert (await connection.receive("x!1"))["type"] == "message.6"


@pytest.mark.asyncio
async def test_receive_after_disconnect(connect):
    connection = connect("x")
    await connection.close()
    with pytest.raises(ChannelClosed):
        await connection.receive("x!1")


@pytest.mark.asyncio
async def test_disconnect_at_same_time_as_everything(connect):
    """
    If we disconnect before the connection is established, don't deadlock.
    """
    connection = connect("x")

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
    assert send_r is None
    assert group_add_r is None
    assert group_send_r is None
    assert group_discard_r is None
    assert isinstance(receive_r, ChannelClosed)


@pytest.mark.asyncio
async def test_log_connection_refused(connect, caplog):
    """
    There's nowhere to report a connection error: log it.

    Without this, admins and developers would have a hard time learning why
    the channel layer isn't sending messages.
    """
    connection = connect("x", host="amqp://guest:guest@localhost:4561/")
    await asyncio.sleep(0.5)  # Enough time to try connecting once
    refused_message = (
        "Connection refused: 500 - Connection to 127.0.0.1:4561 failed: "
        "[Errno 111] Connection refused"
    )
    retry_message = (
        "No connection to amqp://guest:guest@localhost:4561/; retrying in 1s"
    )

    assert any(r.getMessage() == refused_message for r in caplog.records)
    assert any(r.getMessage() == retry_message for r in caplog.records)
    await connection.close()
