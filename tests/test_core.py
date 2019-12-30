import threading

import pytest

from asgiref.sync import async_to_sync
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
async def test_groups_exchange():
    """
    Tests custom group exchange.
    """
    layer = RabbitmqChannelLayer(host=HOST)
    layer2 = RabbitmqChannelLayer(host=HOST, groups_exchange="test-groups-exchange")
    channel1 = await layer.new_channel()
    channel2 = await layer2.new_channel()
    channel3 = await layer.new_channel()
    await layer.group_add("test-group", channel1)
    await layer2.group_add("test-group", channel2)
    await layer.group_add("test-group", channel3)
    await layer.group_send("test-group", {"type": "message.1"})

    # Make sure we get the message on the two channels that were in
    assert (await layer.receive(channel1))["type"] == "message.1"
    assert (await layer.receive(channel3))["type"] == "message.1"

    # channel2 is in separate exchange. It should receive _other_ messages, though.
    await layer.send(channel2, {"type": "message.2"})
    assert (await layer2.receive(channel2))["type"] == "message.2"


def test_async_to_sync_from_thread():
    def run():
        layer = RabbitmqChannelLayer(host=HOST)
        async_to_sync(layer.group_send)("x", {"type": "message.1"})
        assert True

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join()


def test_warn_when_group_expiry_set():
    with pytest.warns(DeprecationWarning) as record:
        RabbitmqChannelLayer(host=HOST, group_expiry=86400)
    assert str(record[0].message) == (
        "channels_rabbitmq does not support group_expiry. Please do not configure it. "
        "For rationale, see "
        "https://github.com/CJWorkbench/channels_rabbitmq/issues/18#issuecomment-547052373"
    )
    assert len(record) == 1
