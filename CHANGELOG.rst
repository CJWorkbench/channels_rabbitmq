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
