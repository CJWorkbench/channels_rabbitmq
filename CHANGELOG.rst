v4.0.1 - 2022-10-27
~~~~~~~~~~~~~~~~~~~

* Support Channels 4

v4.0.0 - 2021-06-08
~~~~~~~~~~~~~~~~~~~

* Try and forestall an error during reconnect. Connecting to RabbitMQ Node B
  after Node A went offline sometimes led to ``group_add()`` failures.
* Bump carehare to v1.0.0

**Upgrade instructions from v3.x**

* In accordance with RabbitMQ's URI specification, ``/`` is no longer allowed
  as the last character in an ``amqp`` URL. If your ``host`` ends with ``/``,
  delete the ``/``.

v3.0.0 - 2021-02-25
~~~~~~~~~~~~~~~~~~~

* Drop support for ``async_to_sync()`` without ``sync_to_async()``. If you have
  been using channels_rabbitmq to open a new RabbitMQ connection per
  message, you'll now get a RuntimeError.
  `Rationale <https://github.com/CJWorkbench/channels_rabbitmq/issues/28#issuecomment-734334065>`_
* Switched to `carehare <https://github.com/CJWorkbench/carehare>`_, so now we:
    * Expose errors. An error in the channel layers will be logged. (Previously,
      it could lead to a frozen layer.)
    * Release unawaited tasks and futures. Now, callers must await all futures.
      (Previously, there were missing warnings when callers didn't await.)
    * Expose ``get_channel_layer().carehare_connection``: a Future Connection.
      (It resets to a new Future upon disconnect.)
* Added ``await get_channel_layer().close()``, for clean shutdowns. On shutdown,
  all consumers will see ``StopConsuming``. (This is not in the Channel Layers
  Specification.)
* Stopped monkey-patching event-loop close. Now, ending the Python process
  without calling ``await get_channel_layer().close()`` may warn about unawaited
  coroutines. Use ASGI Lifespan to fix the warnings.

**Upgrade instructions from v2.x**

* Upgrade to **Python 3.8 or higher**.
* Ensure your ``host`` and ``ssl_context`` agree. If you use ``ssl_context``,
  your ``host`` must begin with ``amqps://``; otherwise, ``amqp://``.
* Ensure you do not call ``async_to_sync()`` outside of ``sync_to_async()``.

v2.0.0 - 2021-01-08
~~~~~~~~~~~~~~~~~~~

* ``local_capacity`` now counts messages. (Before, it was counting how many
  ``.get()`` calls were pending.)
* ``prefetch_count`` no longer exists. It was confusing when combined with
  ``local_capacity``.
* Switched to `aiormq <https://github.com/mosquito/aiormq>`_, which is
  better-maintained.
* Upgraded `msgpack <https://github.com/msgpack/msgpack-python>`_ to v1.

**Upgrade instructions from v1.x**

* If you have configured ``prefetch_count``, delete it.
* Upgrade to **Python 3.7 or higher**.
* Upgrade to **Django-Channels 3.0 or higher**.

v1.3.1 - 2020-12-29
~~~~~~~~~~~~~~~~~~~

* Fix package __version__ metadata, which prevented some users from
  installing v1.3.0. [issue #30] [PR #31]
* Fix async_to_sync() behavior on Python 3.6 and 3.7.

v1.3.0 - 2020-12-23
~~~~~~~~~~~~~~~~~~~

* Don't crash when called in async_to_sync() on asgiref >= 3.2.4
* Nix some deprecation warnings
* Allow installing alongside django-channels 3.0. [issue #29]

v1.2.1 - 2020-03-09
~~~~~~~~~~~~~~~~~~~

* Fix race when sending to group. [issue #23]

v1.2.0 - 2019-12-30
~~~~~~~~~~~~~~~~~~~

* Support Python 3.8 (upgrade to aioamqp=0.14.0) [issue #20] [PR #21]
* Deprecate ``group_expiry`` [issue #18] [PR #19]. Rationale:
  https://github.com/django/channels/issues/1371

Users who previously configured a ``group_expiry`` should remove it. It will
produce a deprecation warning, and it will be disallowed starting in v2.0.0.

v1.1.5 - 2019-08-22
~~~~~~~~~~~~~~~~~~~

* Allow asgiref=3.2.1. [issue #15]

v1.1.4 - 2019-07-09
~~~~~~~~~~~~~~~~~~~

* Avoid race which could drop messages and hang publish(). [issue #14]

v1.1.3 - 2019-06-16
~~~~~~~~~~~~~~~~~~~

* Warn when back-pressuring. Back-pressuring can make a web server
  unresponsive, even though it's behaving according to spec.
* Expire messages after receiving them from RabbitMQ but before
  ``receive()``. Warn when expiring. Allows recovery after messages
  are sent to nonexistent channels.

v1.1.2 - 2019-06-10
~~~~~~~~~~~~~~~~~~~

* Make ``groups_exchange`` configurable [issue #12]

v1.1.1 - 2019-05-23
~~~~~~~~~~~~~~~~~~~

* Retry after ``declare_queue`` errors during reconnect [issue #9]

v1.1.0 - 2019-05-09
~~~~~~~~~~~~~~~~~~~

* Add ``ssl_context`` configuration option [issue #2]
* Upgrade to aioamqp 0.13
* Avoid a 'Connection lost exc=None' warning when disconnecting from a thread
  with its own event loop [issue #4]

v1.0.1 - 2019-05-01
~~~~~~~~~~~~~~~~~~~

* Experimental support for Python 3.5. (There's no test coverage; use at your
  own risk!)

v1.0.0 - 2019-04-24
~~~~~~~~~~~~~~~~~~~

``channels_rabbitmq`` has been humming on production long enough that it
deserves the badge, "1.0".

``asgiref`` has seen a major version bump. ``channels_rabbitmq`` doesn't rely
on any of that changed code, but it does need newer dependencies so it can be
installed alongside the latest versions of ``channels`` and ``daphne``.

* Bump asgiref to 3.1 and channels to 2.2.

v0.0.11 - 2019-02-21
~~~~~~~~~~~~~~~~~~~~

* Bump msgpack from 0.5.2 to 0.6.1
