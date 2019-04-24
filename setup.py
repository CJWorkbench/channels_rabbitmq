from os.path import dirname, join

from setuptools import find_packages, setup

from channels_rabbitmq import __version__

# We use the README as the long_description
readme = open(join(dirname(__file__), "README.rst")).read()

test_requires = ["pytest~=3.6.0", "pytest-asyncio~=0.8", "pytest-timeout~=1.3.3"]


setup(
    name="channels_rabbitmq",
    version=__version__,
    url="http://github.com/CJWorkbench/channels_rabbitmq/",
    author="Adam Hooper",
    author_email="adam@adamhooper.com",
    description="RabbitMQ-backed ASGI channel layer implementation",
    long_description=readme,
    license="BSD",
    zip_safe=False,
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    install_requires=[
        "aioamqp~=0.12.0",
        "asgiref~=3.1.2",
        "msgpack~=0.6.1",
        "channels~=2.2.0",
    ],
    extras_require={"tests": test_requires},
)
