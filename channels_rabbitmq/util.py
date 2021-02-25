import asyncio
from typing import Any, Dict, Iterable, NamedTuple, Union

import msgpack


class ChannelRecipient(NamedTuple):
    channel: str


class GroupRecipient(NamedTuple):
    group: str


Recipient = Union[ChannelRecipient, GroupRecipient]


def serialize_message(recipient: Recipient, data: Dict[str, Any]) -> bytes:
    assert isinstance(data, dict), "data is not a dict"
    assert "__asgi_channel__" not in data
    assert "__asgi_group__" not in data
    augmented_data = dict(data)
    if isinstance(recipient, ChannelRecipient):
        augmented_data["__asgi_channel__"] = recipient.channel
    elif isinstance(recipient, GroupRecipient):
        augmented_data["__asgi_group__"] = recipient.group
    return msgpack.packb(augmented_data)


class DeserializeResult(NamedTuple):
    recipient: Recipient
    data: Dict[str, Any]


def deserialize_message(message_bytes: bytes) -> DeserializeResult:
    data = msgpack.unpackb(message_bytes)
    channel = data.pop("__asgi_channel__", None)
    group = data.pop("__asgi_group__", None)
    assert (channel is None) != (group is None)
    if channel is not None:
        recipient = ChannelRecipient(channel)
    else:
        recipient = GroupRecipient(group)
    return DeserializeResult(recipient, data)


async def gather_without_leaking(tasks: Iterable[asyncio.Task]):
    """Run a bunch of tasks to completion, _then_ raise the first exception.

    This differs from regular `asyncio.gather()`, which leaves tasks running on
    the event loop without waiting for them to finish.

    It also differs because it accepts a list, not varargs.

    This looks like a hack: shouldn't it be part of asyncio instead? Why isn't
    this the _default_? ... dunno.
    """
    try:
        # Run, raising first exception
        await asyncio.gather(*tasks)
    except Exception:
        # Wait for all tasks to finish, exceptional or not
        await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        raise
