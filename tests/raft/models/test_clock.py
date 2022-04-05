import pytest
import trio

from raft.models import clock
from raft.models import EventType


GLOBAL_ITEMS = []


async def heartbeat_collector(receive_channel: trio.abc.ReceiveChannel):
    async with receive_channel:
        async for item in receive_channel:
            GLOBAL_ITEMS.append(item)


async def test_async_heartbeat():
    with trio.move_on_after(2):
        async with trio.open_nursery() as nursery:
            send_channel, receive_channel = trio.open_memory_channel(0)
            async with send_channel, receive_channel:
                heartbeat = clock.AsyncClock(
                    send_channel.clone(),
                    interval=0.10,
                    event_type=EventType.HeartbeatTime,
                )

                nursery.start_soon(heartbeat_collector, receive_channel.clone())
                nursery.start_soon(heartbeat.start)
                await trio.sleep(1.5)
                heartbeat.stop()
    assert len(GLOBAL_ITEMS) == 15
