import asyncio
from functools import partial

import carehare

from .multiqueue import MultiQueue
from .util import deserialize_message


async def consume_into_multi_queue_until_connection_close(
    connection: carehare.Connection,
    channel: str,
    multi_queue: MultiQueue,
    prefetch_count: int,
) -> None:
    loop = asyncio.get_running_loop()

    try:
        async with connection.acking_consumer(
            channel, prefetch_count=prefetch_count
        ) as consumer:

            def _ack_or_no_op(delivery_tag: int):
                try:
                    consumer.ack(delivery_tag)
                except OSError:  # asyncio.Transport.write() failed
                    pass

            while True:
                # back-pressure until `consumer.ack()` was called enough times
                try:
                    body, delivery_tag = await consumer.next_delivery()
                except carehare.ConnectionClosed:
                    break
                recipient, data = deserialize_message(body)

                multi_queue.put_nowait(
                    recipient,
                    data,
                    time=loop.time(),
                    ack=partial(_ack_or_no_op, delivery_tag),
                )
    except carehare.ConnectionClosed:
        pass
