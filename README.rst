channels_rabbitmq
=================

A Django Channels channel layer that uses RabbitMQ as its backing store.

Does not support `Worker and Background Tasks
<https://channels.readthedocs.io/en/stable/topics/worker.html>`_.
(See `Rationale
<https://github.com/CJWorkbench/channels_rabbitmq/pull/11#issuecomment-499185070>`_
and use ``await get_channel_layer().current_connection`` to send to job queues.)

Works with Python 3.8 or 3.9.

Installation
------------

``pip install channels_rabbitmq``

Usage
-----

Then set up the channel layer in your Django settings file like so::

    CHANNEL_LAYERS = {
        "default": {
            "BACKEND": "channels_rabbitmq.core.RabbitmqChannelLayer",
            "CONFIG": {
                "host": "amqp://guest:guest@127.0.0.1/asgi",
                # "ssl_context": ... (optional)
            },
        },
    }

Possible options for ``CONFIG`` are listed below.

``host``
~~~~~~~~

URL of the server to connect to, adhering to `RabbitMQ spec
<https://www.rabbitmq.com/uri-spec.html>`_. To connect to a RabbitMQ cluster,
use a DNS server to resolve a hostname to multiple IP addresses.
channels_rabbitmq will automatically reconnect if at least one of them is
reachable in case of a disconnection.

``expiry``
~~~~~~~~~~

Minimum number of seconds a message should wait in a RabbitMQ queue, before it
may be silently dropped.

Defaults to ``60``. You generally shouldn't need to change this, but you may
want to turn it down if you have peaky traffic you wish to drop, or up if you
have peaky traffic you want to backlog until you get to it.

``local_capacity``
~~~~~~~~~~~~~~~~~~

Number of incoming messages queued in memory. Defaults to ``100``. (A message
sent to a group with two channels counts as one message.) When ``local_capacity``
messages are queued, the message backlog will grow on RabbitMQ.

(This controls the ``prefetch_count`` on the RabbitMQ queue.)

``local_expiry``
~~~~~~~~~~~~~~~~

Minimum number of seconds a message received from RabbitMQ must be held in
memory waiting for ``receive()``, before it may be dropped. Defaults to
``expiry``.

A warning will be logged when a message expires locally. The warning can
indicate that a channel has more messages than it can handle; or that
messages are being sent to a channel that does not exist. (Perhaps a missing
channel was implied by ``group_add()``, and a matching ``group_discard()``
was never called.)

If ``local_expiry < expiry``, then you can end up ignoring (and logging)
messages locally while they still exist in the RabbitMQ queue. These messages
will be acked, so RabbitMQ will behave as though they were delivered.

``remote_capacity``
~~~~~~~~~~~~~~~~~~~

Number of messages stored on RabbitMQ for each client. Defaults to ``100``.
(A message sent to a group with three channels on two distinct clients counts
as two messages.) When ``remote_capacity`` messages are queued in RabbitMQ,
the channel will refuse new messages. Calls from any client to ``send()`` or
``group_send()`` to the at-capacity client will raise ``ChannelFull``.

``ssl_context``
~~~~~~~~~~~~~~~

An `SSL context
<https://docs.python.org/3/library/ssl.html#ssl-contexts>`_. Changes the
default ``host`` port to 5671 (instead of 5672).

For instance, to connect to an TLS RabbitMQ service that will verify your
client::

    import ssl
    ssl_context = ssl.create_default_context(
        cafile=str(Path(__file__).parent.parent / 'ssl' / 'server.cert'),
    )
    ssl_context.load_cert_chain(
        certfile=str(Path(__file__).parent.parent / 'ssl' / 'client.certchain'),
        keyfile=str(Path(__file__).parent.parent / 'ssl' / 'client.key'),
    )
    CHANNEL_LAYERS['default']['CONFIG']['ssl_context'] = ssl_context

By default, there is no SSL context; all messages (and passwords) are
are transmitted in cleartext.

``groups_exchange``
~~~~~~~~~~~~~~~~~~~

Global direct exchange name used by channels to exchange group messages.
Defaults to ``"groups"``. See also `Design decisions`_.

Accessing Carehare
------------------

We use `carehare
<https://github.com/CJWorkbench/carehare>`_ for its thorough handling of errors.

Django Channels' specification does not account for "connecting" and
"disconnecting". This layer does its best by constantly reconnecting, forever.

Call ``await get_channel_layer().current_connection`` to access an open Carehare
connection. This lets you use job queues without "Worker and Background Tasks".
Like this::

    # raise asyncio.CancelledError on failure
    connection = await get_channel_layer().carehare_connection

    # raise carehare.ConnectionClosed or carehare.ChannelClosed on error
    await connection.publish(b"task", routing_key="job_queue")

(The Carehare documentation explains how to build workers.)

A note on errors: a "connected" connection isn't guaranteed to *stay* connected
throughout every publish. It was merely connected *at some point in the past*.
When a disconnect occurs, all pending operations on that connection will raise
``carehare.ConnectionClosed``. This channel layer will log the error, and
``get_channel_layer().carehare_connection`` will point to a new Future. (This
error+reconnect is *guaranteed to happen* in production.)

Design decisions
----------------

To scale enormously, this layer only creates one RabbitMQ queue per instance.
That means one web server gets one RabbitMQ queue, no matter how many
websocket connections are open. For each message being sent, the client-side
layer determines the RabbitMQ queue name and uses it as the routing key.

Groups are implemented using a single, global RabbitMQ direct exchange called
"groups" by default. To send a message to a group, the layer sends the message
to the "groups" exchange with the group name as the routing key. The client
binds and unbinds during ``group_add()`` and ``group_remove()`` to ensure
messages for any of its groups will reach it. See also the `groups_exchange`_
option.

RabbitMQ queues are ``exclusive``: when a client disconnects (through close or
crash), RabbitMQ will delete the queue and unbind the groups.

Once a connection has been created, it pollutes the event loop so that
``async_to_sync()`` will destroy the connection if it was created within
``async_to_sync()``. Each connection starts a background async loop that pulls
messages from RabbitMQ and routes them to receiver queues; each ``receive()``
queries receiver queues. Empty queues with no connections are deleted.

Deviations from the Channel Layer Specification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `Channel Layer Specification
<https://channels.readthedocs.io/en/latest/channel_layer_spec.html>`_ bends to
Redis-related restrictions. RabbitMQ cannot emulate Redis. Here are the
differences:

* **No ``flush`` extension**: To flush all state, simply disconnect all clients.
  (RabbitMQ won't allow one client to delete another client's data structures.)
* **No ``group_expiry`` option**: The `group_expiry option
  <https://channels.readthedocs.io/en/latest/channel_layer_spec.html#persistence>`_
  recovers when a ``group_add()`` has no matching ``group_discard()``. But the
  "group membership expiry" logic has a fatal flaw: it disconnects legitimate
  members. ``channels_rabbitmq`` addresses each root problem instead:

  * Web-server crash: RabbitMQ wipes all state related to a web server when
    the web server disconnects. There's no problem here for ``group_expiry``
    to solve.
  * Programming errors: You may err and call ``group_add()`` without
    eventually calling ``group_discard()``. Redis can't detect this
    programming error (because it can't detect web-server crashes). RabbitMQ
    can. The ``local_expiry`` option keeps your site running after you
    erroneously miss a ``group_discard()``. The channel layer warns when
    discarding expired messages. Monitor your server logs to detect your
    errors.
* **No "normal channels"**: `Normal channels
  <https://channels.readthedocs.io/en/latest/channel_layer_spec.html#channels>`_
  are job queues. In most projects, "normal channel" readers are worker
  processes, ideally divorced from Websockets and Django.

  If you want an async, RabbitMQ-based job queue, investigate `carehare
  <https://github.com/CJWorkbench/carehare>`_.

Dependencies
------------

You'll need Python 3.8+ and a RabbitMQ server.

If you have Docker, here's how to start a development server::

    ssl/prepare-certs.sh  # Create SSL certificates used in tests
    docker run --rm -it \
         -p 5671:5671 \
         -p 5672:5672 \
         -p 15672:15672 \
         -v "/$(pwd)"/ssl:/ssl \
         -e RABBITMQ_SSL_CACERTFILE=/ssl/ca.cert \
         -e RABBITMQ_SSL_CERTFILE=/ssl/server.cert \
         -e RABBITMQ_SSL_KEYFILE=/ssl/server.key \
         -e RABBITMQ_SSL_VERIFY=verify_peer \
         -e RABBITMQ_SSL_FAIL_IF_NO_PEER_CERT=true \
         rabbitmq:3.7.8-management-alpine

You can access the RabbitMQ management interface at http://localhost:15672.

Contributing
------------

To add features and fix bugs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, start a development RabbitMQ server::

    ssl/prepare-certs.sh  # Create SSL certificates used in tests
    docker run --rm -it \
         -p 5671:5671 \
         -p 5672:5672 \
         -p 15672:15672 \
         -v "/$(pwd)"/ssl:/ssl \
         -e RABBITMQ_SSL_CACERTFILE=/ssl/ca.cert \
         -e RABBITMQ_SSL_CERTFILE=/ssl/server.cert \
         -e RABBITMQ_SSL_KEYFILE=/ssl/server.key \
         -e RABBITMQ_SSL_VERIFY=verify_peer \
         -e RABBITMQ_SSL_FAIL_IF_NO_PEER_CERT=true \
         rabbitmq:3.8.11-management-alpine

Now take on the development cycle:

#. ``tox`` # to ensure tests pass.
#. Write new tests in ``tests/`` and make sure they fail.
#. Write new code in ``channels_rabbitmq/`` to make the tests pass.
#. Submit a pull request.

To deploy
~~~~~~~~~

Use `semver <https://semver.org/>`_.

#. ``git push`` and make sure Travis tests all pass.
#. ``git tag vX.X.X``
#. ``git push --tags``

TravisCI will push to PyPi.
