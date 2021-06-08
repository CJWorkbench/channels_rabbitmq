import asyncio
import contextlib
import ssl
import threading
import time
from pathlib import Path

import pytest
from channels.exceptions import ChannelFull, StopConsumer

from channels_rabbitmq.core import RabbitmqChannelLayer, ReconnectDelay

HOST = "amqps://guest:guest@localhost"
SSL_CONTEXT = ssl.create_default_context(
    cafile=str(Path(__file__).parent.parent / "ssl" / "server.cert")
)
SSL_CONTEXT.load_cert_chain(
    certfile=str(Path(__file__).parent.parent / "ssl" / "client.certchain"),
    keyfile=str(Path(__file__).parent.parent / "ssl" / "client.key"),
)


def ASYNC_TEST(fn):
    return pytest.mark.timeout(8)(pytest.mark.asyncio(fn))


@contextlib.asynccontextmanager
async def open_layer(**kwargs):
    queue_name = kwargs.pop("queue_name", None)

    kwargs = {
        "host": HOST,
        "ssl_context": SSL_CONTEXT,
        **kwargs,
    }

    layer = RabbitmqChannelLayer(**kwargs)
    if queue_name is not None:
        layer._queue_name = queue_name
    try:
        yield layer
    finally:
        await layer.close()


@ASYNC_TEST
async def test_send_receive():
    """
    Makes sure we can send a message to a normal channel then receive it.
    """
    async with open_layer() as layer:
        channel = await layer.new_channel()
        await layer.send(channel, {"type": "test.message", "text": "Ahoy-hoy!"})
        message = await layer.receive(channel)
        assert message["type"] == "test.message"
        assert message["text"] == "Ahoy-hoy!"


@ASYNC_TEST
async def test_multiple_event_loops():
    """
    Makes sure we can receive from two different event loops using
    process-local channel names.

    Real-world callers shouldn't be creating an excessive number of event
    loops. This test is mostly useful for unit-testers and people who use
    async_to_sync() to send messages.
    """
    async with open_layer() as layer:
        channel = await layer.new_channel()

        def run():
            with pytest.raises(RuntimeError) as cm:
                asyncio.run(
                    layer.send(channel, {"type": "test.message", "text": "Ahoy-hoy!"})
                )
            assert (
                "The caller tried using channels_rabbitmq on a different event loop"
                in str(cm.value)
            )

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join()


@ASYNC_TEST
async def test_process_local_send_receive():
    """
    Makes sure we can send a message to a process-local channel then receive it.
    """
    async with open_layer() as layer:
        channel = await layer.new_channel()
        await layer.send(channel, {"type": "test.message", "text": "Local only please"})
        message = await layer.receive(channel)
        assert message["type"] == "test.message"
        assert message["text"] == "Local only please"


@ASYNC_TEST
async def test_reject_bad_channel():
    """
    Makes sure sending/receiving on an invalid channel name fails.
    """
    async with open_layer() as layer:
        with pytest.raises(TypeError):
            await layer.send("=+135!", {"type": "foom"})
        with pytest.raises(TypeError):
            await layer.receive("=+135!")
        with pytest.raises(AssertionError):
            await layer.receive("y!y")


@ASYNC_TEST
async def test_reject_send_normal_channel():
    async with open_layer() as layer:
        with pytest.raises(
            RuntimeError, match="channels_rabbitmq does not support Normal Channels"
        ):
            await layer.send("jobs", {"not": "supported"})


@ASYNC_TEST
async def test_reject_bad_client_prefix():
    """
    Makes sure receiving on a non-prefixed local channel is not allowed.
    """
    async with open_layer() as layer:
        with pytest.raises(AssertionError):
            await layer.receive("not-client-prefix!local_part")


@ASYNC_TEST
async def test_reject_receive_normal_channel():
    async with open_layer() as layer:
        with pytest.raises(
            RuntimeError, match="channels_rabbitmq does not support Normal Channels"
        ):
            await layer.receive("jobs")


@ASYNC_TEST
async def test_groups_within_layer():
    """
    Tests basic group operation.
    """
    async with open_layer() as layer:
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


def test_async_to_sync_without_event_loop():
    with pytest.raises(RuntimeError) as cm:
        RabbitmqChannelLayer()

    assert "Refusing to initialize channel layer without a running event loop" in str(
        cm.value
    )


@ASYNC_TEST
async def test_warn_when_group_expiry_set():
    with pytest.warns(DeprecationWarning) as record:
        async with open_layer(group_expiry=86400):
            pass
    assert str(record[0].message) == (
        "channels_rabbitmq does not support group_expiry. Please do not configure it. "
        "For rationale, see "
        "https://github.com/CJWorkbench/channels_rabbitmq/issues/18#issuecomment-547052373"
    )
    assert len(record) == 1


@ASYNC_TEST
async def test_send_capacity(caplog):
    """
    Makes sure we get ChannelFull when the queue exceeds remote_capacity
    """
    async with open_layer(queue_name="x", remote_capacity=1, local_capacity=1) as layer:
        await layer.send("x!y", {"type": "test.message1"})  # one local, unacked
        await layer.send("x!y", {"type": "test.message2"})  # one remote, queued
        with pytest.raises(ChannelFull):
            await layer.send("x!y", {"type": "test.message3"})
        assert "Back-pressuring. Biggest queues: x!y (1)" in caplog.text

        # Test that even after error, the queue works as expected.

        # Receive the acked message1. This will _eventually_ ack message2. RabbitMQ
        # will have unacked=0, ready=1. This will prompt it to send a new unacked
        # message.
        assert (await layer.receive("x!y"))["type"] == "test.message1"

        # Receive message2. This _guarantees_ message2 is acked.
        assert (await layer.receive("x!y"))["type"] == "test.message2"

        # Send message5. We're sending and receiving on the same TCP layer, so
        # RabbitMQ is aware that message2 was acked by the time we send message5.
        # That means its queue isn't maxed out any more.
        await layer.send("x!y", {"type": "test.message4"})  # one ready

        assert (await layer.receive("x!y"))["type"] == "test.message4"


@ASYNC_TEST
async def test_send_expire_remotely():
    # expiry 80ms: long enough for us to receive all messages; short enough to
    # keep the test fast.
    async with open_layer(
        queue_name="x", local_capacity=1, expiry=0.08, local_expiry=2
    ) as layer:
        await layer.send("x!y", {"type": "test.message1"})  # one local, unacked
        await layer.send("x!y", {"type": "test.message2"})  # remote, queued
        await asyncio.sleep(0.09)  # test.message2 should expire
        await layer.send("x!y", {"type": "test.message3"})  # remote
        assert (await layer.receive("x!y"))["type"] == "test.message1"
        # test.message2 should disappear entirely
        assert (await layer.receive("x!y"))["type"] == "test.message3"


@ASYNC_TEST
async def test_send_expire_locally(caplog):
    # expiry 20ms: long enough that we can deliver one message but expire
    # another.
    #
    # local_capacity=1: when we expire, we must ack so RabbitMQ can send
    # another message.
    async with open_layer(queue_name="x", local_expiry=0.02, local_capacity=1) as layer:
        await layer.send("x!y", {"type": "test.message1"})
        await asyncio.sleep(0.2)  # plenty of time; message.1 should expire
        await layer.send("x!y", {"type": "test.message2"})
        assert (await layer.receive("x!y"))["type"] == "test.message2"
        assert "expired locally" in caplog.text


@ASYNC_TEST
async def test_process_remote_send_receive():
    """
    Makes sure we can send a message to a process-local channel then receive it.
    """
    async with open_layer(queue_name="x") as x, open_layer(queue_name="y") as y:
        # Make sure y's queue is created. A dummy `send()` will do it,
        # since it only completes after the queue is created.
        await y.send("nonexistent!channel", {"type": "no-op"})

        await x.send("y!y", {"type": "remote"})
        await x.send("x!y", {"type": "local"})

        assert (await y.receive("y!y"))["type"] == "remote"
        assert (await x.receive("x!y"))["type"] == "local"  # no remote


@ASYNC_TEST
async def test_multi_send_receive():
    """
    Tests overlapping sends and receives, and ordering.
    """
    async with open_layer(queue_name="x") as layer:
        await layer.send("x!y", {"type": "message.1"})
        await layer.send("x!y", {"type": "message.2"})
        await layer.send("x!y", {"type": "message.3"})
        assert (await layer.receive("x!y"))["type"] == "message.1"
        assert (await layer.receive("x!y"))["type"] == "message.2"
        assert (await layer.receive("x!y"))["type"] == "message.3"


@ASYNC_TEST
async def test_groups_local():
    async with open_layer(queue_name="x") as layer:
        await layer.group_add("test-group", "x!1")
        await layer.group_add("test-group", "x!2")
        await layer.group_add("test-group", "x!3")
        await layer.group_discard("test-group", "x!2")
        await layer.group_send("test-group", {"type": "message.1"})

        # Make sure we get the message on the two channels that were in
        assert (await layer.receive("x!1"))["type"] == "message.1"
        assert (await layer.receive("x!3"))["type"] == "message.1"

        # "x!2" is unsubscribed. It should receive _other_ messages, though.
        await layer.send("x!2", {"type": "message.2"})
        assert (await layer.receive("x!2"))["type"] == "message.2"


@ASYNC_TEST
async def test_groups_discard():
    async with open_layer(queue_name="x") as layer:
        await layer.group_add("test-group", "x!1")
        await layer.group_discard("test-group", "x!1")
        await layer.group_add("test-group", "x!1")
        await layer.group_discard("test-group", "x!1")
        await layer.group_send("test-group", {"type": "ignored"})

        # message was ignored. We should receive _other_ messages, though.
        await layer.send("x!1", {"type": "normal"})
        assert (await layer.receive("x!1"))["type"] == "normal"


@ASYNC_TEST
async def test_group_discard_when_not_connected():
    async with open_layer(queue_name="x") as layer:
        await layer.group_discard("test-group", "x!1")
        await layer.group_send("test-group", {"type": "ignored"})
        await layer.send("x!1", {"type": "normal"})
        assert (await layer.receive("x!1"))["type"] == "normal"


@ASYNC_TEST
async def test_groups_remote():
    async with open_layer(queue_name="x") as x, open_layer(queue_name="y") as y:
        await x.group_add("test-group", "x!1")
        await x.group_add("test-group", "x!2")
        await y.group_add("test-group", "y!3")
        await x.group_discard("test-group", "x!2")
        await y.group_send("test-group", {"type": "message.1"})

        # Make sure we get the message on the two channels that were in
        assert (await x.receive("x!1"))["type"] == "message.1"
        assert (await y.receive("y!3"))["type"] == "message.1"

        # "x!2" is unsubscribed. It should receive _other_ messages, though.
        await y.send("x!2", {"type": "message.2"})
        assert (await x.receive("x!2"))["type"] == "message.2"


@ASYNC_TEST
async def test_groups_exchange():
    """
    Tests customizable groups exchange.
    """
    async with open_layer(
        queue_name="x", groups_exchange="test-groups-exchange"
    ) as x, open_layer(
        queue_name="y", groups_exchange="test-groups-exchange"
    ) as y, open_layer(
        queue_name="z"
    ) as z:
        await x.group_add("test-group", "x!1")
        await x.group_add("test-group", "x!2")
        await y.group_add("test-group", "y!3")
        await z.group_add("test-group", "z!4")
        await x.group_discard("test-group", "x!2")
        await y.group_send("test-group", {"type": "message.1"})

        # Make sure we get the message on the two channels that were in
        assert (await x.receive("x!1"))["type"] == "message.1"
        assert (await y.receive("y!3"))["type"] == "message.1"

        # "x!2" is unsubscribed. It should receive _other_ messages, though.
        await x.send("x!2", {"type": "message.2"})
        assert (await x.receive("x!2"))["type"] == "message.2"

        # "z!4" is in separate (default) exchange 'groups'.
        # It should receive _other_ messages, though.
        await z.group_send("test-group", {"type": "message.4"})
        assert (await z.receive("z!4"))["type"] == "message.4"


@ASYNC_TEST
async def test_groups_channel_full():
    """
    Tests that group_send ignores ChannelFull
    """
    async with open_layer(queue_name="x", local_capacity=1, remote_capacity=1) as layer:
        await layer.group_add("test-group", "x!1")

        # Message 1:
        # * server acks and control flow returns.
        # * consumer receives message in background, doesn't ack it.
        # After this, local_capacity is full.
        await layer.group_send("test-group", {"type": "message.1"})
        # Wait a few ms to make sure the message reaches the consumer
        await asyncio.sleep(0.01)

        # Message 2:
        # * server acks and control flow returns.
        # After this, remote_capacity is full.
        await layer.group_send("test-group", {"type": "message.2"})

        await layer.group_send("test-group", {"type": "message.3"})  # rejected

        assert (await layer.receive("x!1"))["type"] == "message.1"  # local
        assert (await layer.receive("x!1"))["type"] == "message.2"  # remote

        # aaaand things are back to normal now that we're below capacity
        await layer.group_send("test-group", {"type": "message.4"})
        assert (await layer.receive("x!1"))["type"] == "message.4"


@ASYNC_TEST
async def test_groups_no_such_group():
    """
    Tests that group_send does nothing if there is no such group.
    """
    async with open_layer(queue_name="x") as layer:
        await layer.group_send("my-group", {"type": "message.1"})

        # Now create the group, and check that new messages to that group will work
        await layer.group_add("my-group", "x!1")
        await layer.group_send("my-group", {"type": "message.2"})
        assert (await layer.receive("x!1"))["type"] == "message.2"


@ASYNC_TEST
async def test_groups_ack_group_that_only_exists_remotely():
    """
    Tests that we ack when receiving a message to no group.

    If Bob unsubscribes from a group but Alice has already sent a message to it,
    RabbitMQ will deliver the message to Bob's Connection even though Bob isn't
    subscribed. In that case, we need to ack the message -- otherwise, we leak
    a message.
    """
    # Set local_capacity=1 to test that the message will be acked. If it isn't
    # acked, message.2 will stall on the RabbitMQ side.
    async with open_layer(queue_name="bob", local_capacity=1) as bob, open_layer(
        queue_name="alice", local_capacity=1
    ) as alice:
        await bob.group_add("my-group", "bob!1")
        # white-box testing: simulate a half-completed group_discard()
        bob._multi_queue.group_discard("my-group", "bob!1")

        await alice.group_send("my-group", {"type": "message.1"})  # no recipient
        await alice.send("bob!1", {"type": "message.2"})
        assert (await bob.receive("bob!1"))["type"] == "message.2"


@ASYNC_TEST
async def test_receive_after_close_by_disconnect():
    async with open_layer(queue_name="x") as layer:
        await layer.carehare_connection

    with pytest.raises(StopConsumer):
        await layer.receive("x!1")


@ASYNC_TEST
async def test_receive_after_close_before_connect():
    async with open_layer(queue_name="x") as layer:
        pass

    with pytest.raises(StopConsumer):
        await layer.receive("x!1")


@ASYNC_TEST
async def test_disconnect_at_same_time_as_everything():
    """
    If we disconnect before the connection is established, don't deadlock.
    """
    async with open_layer(queue_name="x") as layer:
        layer.carehare_connection  # start connecting
        await asyncio.sleep(0)  # so connection_future task begins

        # Schedule all these commands to run simultaneously. At this point, the
        # connection isn't established yet and no queue has been created.
        #
        # Begin the close() first -- that'll make it happen before any other
        # command acquires its lock.
        close = layer.close()
        send = layer.send("x!1", {"type": "hi"})
        group_add = layer.group_add("g", "x!1")
        group_send = layer.group_send("g", {"type": "ghi"})
        group_discard = layer.group_discard("g", "x!1")
        receive = layer.receive("x!1")

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
        assert isinstance(send_r, StopConsumer)
        assert isinstance(group_add_r, StopConsumer)
        assert isinstance(group_send_r, StopConsumer)
        assert isinstance(group_discard_r, StopConsumer)
        assert isinstance(receive_r, StopConsumer)


@ASYNC_TEST
async def test_log_connection_refused(caplog):
    """
    There's nowhere to report a connection error: log it.

    Without this, admins and developers would have a hard time learning why
    the channel layer isn't sending messages.
    """
    async with open_layer(
        queue_name="x", host="amqp://guest:guest@localhost:4561", ssl_context=None
    ) as layer:
        # 0.5s: Enough time to try connecting once
        await asyncio.wait({layer.carehare_connection}, timeout=0.5)

    assert "Failure connecting" in caplog.text
    assert "Retrying" in caplog.text


@ASYNC_TEST
async def test_no_ssl():
    """
    Connect through TCP, without TLS.

    Assumes the server is listening over both a TLS port and a no-TLS port.
    """
    async with open_layer(
        queue_name="x", host=HOST.replace("amqps://", "amqp://"), ssl_context=None
    ) as layer:
        await layer.carehare_connection


@ASYNC_TEST
async def test_reconnect_on_queue_name_conflict():
    """
    When reconnecting to a cluster, a race may leave your queue name declared.

    `channels_rabbitmq` should try to reconnect when that happens. (We assume
    there aren't two clients with the same queue_name; if they are, hopefully
    one of them is reading the flurry of errors in the logs.)

    https://github.com/CJWorkbench/channels_rabbitmq/issues/9
    """
    async with open_layer(queue_name="x") as layer1:
        # Ensure the connection is alive and kicking
        await layer1.carehare_connection

        # Now simulate a race: here comes the same client, but the queue is already
        # declared! Oh no!
        async with open_layer(queue_name="x") as layer2:
            layer2.carehare_connection  # start connecting

            # (The old connection will die 0.2s after we try to declare the queue.)
            async def close_slowly():
                """
                Close layer1, "slowly".

                For 0.2s, layer1 will be open and conflicting with connection2,
                preventing connection2 from connecting. After return, layer1 will
                be closed. (If we're connected to a RabbitMQ cluster there may _still_
                be a conflict even after return; but that shouldn't be a problem
                because the reconnect will _eventually_ succeed.
                """
                await asyncio.sleep(0.2)
                await layer1.close()

            t1 = time.time()
            future_closed = asyncio.get_event_loop().create_task(close_slowly())
            await layer2.send("x!y", {"type": "test.2"})
            await future_closed  # clean up
            t2 = time.time()
            assert t2 - t1 >= ReconnectDelay, "send() should stall until reconnect"
            assert (await layer2.receive("x!y"))["type"] == "test.2"


@ASYNC_TEST
async def test_concurrent_send():
    """
    Ensure all frames for one AMQP message are sent before another is sent.

    https://github.com/CJWorkbench/channels_rabbitmq/issues/14
    """
    async with open_layer(queue_name="x") as layer:
        await layer.group_add("test-group", "x!1")

        # Send lots of concurrent messages: both with group_send() and send().
        # We're sending concurrently. Order doesn't matter.
        #
        # It can take _lots_ of messages to trigger bug #14 locally. Prior to the
        # bugfix (a mutex during publish), messages' frames could be interwoven;
        # but with Python 3.6 on Linux that happened rarely when sending fewer than
        # 100 concurrent messages locally.
        texts = set(f"x{i}" for i in range(100))
        messages = [{"type": text} for text in texts]
        group_sends = [layer.group_send("test-group", m) for m in messages]
        direct_sends = [layer.send("x!2", m) for m in messages]
        group_receives = [asyncio.ensure_future(layer.receive("x!1")) for _ in texts]
        direct_receives = [asyncio.ensure_future(layer.receive("x!2")) for _ in texts]
        await asyncio.gather(
            *(group_sends + direct_sends + group_receives + direct_receives)
        )

        assert set([m.result()["type"] for m in group_receives]) == texts
        assert set([m.result()["type"] for m in direct_receives]) == texts
