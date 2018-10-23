import pytest

from asgiref.sync import async_to_sync
from channels.exceptions import ChannelFull
from channels_rabbitmq.core import RabbitmqChannelLayer

HOST = "amqp://guest:guest@localhost/"


@pytest.mark.asyncio
async def test_send_receive():
    """
    Makes sure we can send a message to a normal channel then receive it.
    """
    layer = RabbitmqChannelLayer(host=HOST)
    channel = await layer.new_channel()
    await layer.send(channel, {"type": "test.message", "text": "Ahoy-hoy!"})
    message = await layer.receive(channel)
    assert message["type"] == "test.message"
    assert message["text"] == "Ahoy-hoy!"


def test_multiple_event_loops():
    """
    Makes sure we can receive from two different event loops using
    process-local channel names.

    Real-world callers shouldn't be creating an excessive number of event
    loops. This test is mostly useful for unit-testers and people who use
    async_to_sync() to send messages.
    """
    layer = RabbitmqChannelLayer(host=HOST)

    async def send_and_close(message):
        channel = await layer.new_channel()
        await layer.send(channel, message)
        message = await layer.receive(channel)
        assert message["type"] == message["type"]

    async_to_sync(send_and_close)({"type": "test.message.1"})
    async_to_sync(send_and_close)({"type": "test.message.2"})


@pytest.mark.asyncio
async def test_send_capacity():
    """
    Makes sure we get ChannelFull when our in-memory structure runs out of
    memory.
    """
    layer = RabbitmqChannelLayer(
        host=HOST, remote_capacity=1, local_capacity=1, prefetch_count=1
    )
    channel = await layer.new_channel()
    await layer.send(channel, {"type": "test.message1"})  # one queued+acked
    await layer.send(channel, {"type": "test.message2"})  # one unacked
    await layer.send(channel, {"type": "test.message3"})  # one ready
    with pytest.raises(ChannelFull):
        await layer.send(channel, {"type": "test.message4"})

    # Test that even after error, the queue works as expected.

    # Receive the acked message1. This will _eventually_ ack message2. RabbitMQ
    # will have unacked=0, ready=1. This will prompt it to send a new unacked
    # message.
    assert (await layer.receive(channel))["type"] == "test.message1"

    # Receive message2. This _guarantees_ message2 is acked.
    assert (await layer.receive(channel))["type"] == "test.message2"

    # Send message5. We're sending and receiving on the same TCP connection, so
    # RabbitMQ is aware that message2 was acked by the time we send message5.
    # That means its queue isn't maxed out any more.
    await layer.send(channel, {"type": "test.message5"})  # one ready

    assert (await layer.receive(channel))["type"] == "test.message3"
    assert (await layer.receive(channel))["type"] == "test.message5"


@pytest.mark.asyncio
async def test_process_local_send_receive():
    """
    Makes sure we can send a message to a process-local channel then receive it.
    """
    layer = RabbitmqChannelLayer(host=HOST)
    channel = await layer.new_channel()
    await layer.send(channel, {"type": "test.message", "text": "Local only please"})
    message = await layer.receive(channel)
    assert message["type"] == "test.message"
    assert message["text"] == "Local only please"


@pytest.mark.asyncio
async def test_process_remote_send_receive():
    """
    Makes sure we can send a message to a process-local channel then receive it.
    """
    layer1 = RabbitmqChannelLayer(host=HOST)
    layer2 = RabbitmqChannelLayer(host=HOST)
    channel2 = await layer2.new_channel()

    # Make sure layer2's queue is created. A dummy `send()` will do it, since
    # it only completes after the queue is created.
    await layer2.send("nonexistent!channel", {"type": "no-op"})

    await layer1.send(channel2, {"type": "test.message", "text": "Remote only please"})
    message = await layer2.receive(channel2)
    assert message["type"] == "test.message"
    assert message["text"] == "Remote only please"


@pytest.mark.asyncio
async def test_multi_send_receive():
    """
    Tests overlapping sends and receives, and ordering.
    """
    layer = RabbitmqChannelLayer(host=HOST)
    channel = await layer.new_channel()
    await layer.send(channel, {"type": "message.1"})
    await layer.send(channel, {"type": "message.2"})
    await layer.send(channel, {"type": "message.3"})
    assert (await layer.receive(channel))["type"] == "message.1"
    assert (await layer.receive(channel))["type"] == "message.2"
    assert (await layer.receive(channel))["type"] == "message.3"


@pytest.mark.asyncio
async def test_reject_bad_channel():
    """
    Makes sure sending/receiving on an invalic channel name fails.
    """
    layer = RabbitmqChannelLayer(host=HOST)
    with pytest.raises(TypeError):
        await layer.send("=+135!", {"type": "foom"})
    with pytest.raises(TypeError):
        await layer.receive("=+135!")


@pytest.mark.asyncio
async def test_reject_bad_client_prefix():
    """
    Makes sure receiving on a non-prefixed local channel is not allowed.
    """
    layer = RabbitmqChannelLayer(host=HOST)
    with pytest.raises(AssertionError):
        await layer.receive("not-client-prefix!local_part")


@pytest.mark.asyncio
async def test_groups_within_layer():
    """
    Tests basic group operation.
    """
    layer = RabbitmqChannelLayer(host=HOST)
    channel1 = await layer.new_channel()
    channel2 = await layer.new_channel()
    channel3 = await layer.new_channel()
    await layer.group_add("test-group", channel1)
    await layer.group_add("test-group", channel2)
    await layer.group_add("test-group", channel3)
    await layer.group_discard("test-group", channel2)
    await layer.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message on the two channels that were in
    assert (await layer.receive(channel1))["type"] == "message.1"
    assert (await layer.receive(channel3))["type"] == "message.1"

    # channel2 is unsubscribed. It should receive _other_ messages, though.
    await layer.send(channel2, {"type": "message.2"})
    assert (await layer.receive(channel2))["type"] == "message.2"


@pytest.mark.asyncio
async def test_groups_discard():
    """
    Tests basic group operation.
    """
    layer = RabbitmqChannelLayer(host=HOST)
    channel1 = await layer.new_channel()
    await layer.group_add("test-group", channel1)
    await layer.group_discard("test-group", channel1)
    await layer.group_add("test-group", channel1)
    await layer.group_discard("test-group", channel1)
    await layer.group_send("test-group", {"type": "ignored"})
    await layer.send(channel1, {"type": "normal"})

    assert (await layer.receive(channel1))["type"] == "normal"


@pytest.mark.asyncio
async def test_groups_across_layers():
    """
    Tests basic group operation.
    """
    layer1 = RabbitmqChannelLayer(host=HOST)
    layer2 = RabbitmqChannelLayer(host=HOST)
    channel1 = await layer1.new_channel()
    channel2 = await layer1.new_channel()
    channel3 = await layer2.new_channel()
    await layer1.group_add("test-group", channel1)
    await layer1.group_add("test-group", channel2)
    await layer2.group_add("test-group", channel3)
    await layer1.group_discard("test-group", channel2)
    await layer2.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message on the two channels that were in
    assert (await layer1.receive(channel1))["type"] == "message.1"
    assert (await layer2.receive(channel3))["type"] == "message.1"

    # channel2 is unsubscribed. It should receive _other_ messages, though.
    await layer2.send(channel2, {"type": "message.2"})
    assert (await layer1.receive(channel2))["type"] == "message.2"


@pytest.mark.asyncio
async def test_groups_channel_full():
    """
    Tests that group_send ignores ChannelFull
    """
    layer = RabbitmqChannelLayer(
        host=HOST, local_capacity=1, remote_capacity=1, prefetch_count=1
    )
    channel = await layer.new_channel()
    await layer.group_add("test-group", channel)
    await layer.group_send("test-group", {"type": "message.1"})  # acked
    await layer.group_send("test-group", {"type": "message.2"})  # unacked
    await layer.group_send("test-group", {"type": "message.3"})  # ready
    await layer.group_send("test-group", {"type": "message.4"})  # rejected
    await layer.group_send("test-group", {"type": "message.5"})  # rejected

    assert (await layer.receive(channel))["type"] == "message.1"
    assert (await layer.receive(channel))["type"] == "message.2"
    assert (await layer.receive(channel))["type"] == "message.3"

    # aaaand things are back to normal now that we're below capacity
    await layer.group_send("test-group", {"type": "message.6"})
    assert (await layer.receive(channel))["type"] == "message.6"
