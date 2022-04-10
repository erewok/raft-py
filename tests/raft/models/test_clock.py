import queue
import time

import trio

from raft.models import EventType, clock

GLOBAL_ITEMS = []


def test_heartbeat():
    events = queue.Queue(maxsize=20)
    heartbeat = clock.ThreadedClock(
        events,
        interval=0.10,
        event_type=EventType.HeartbeatTime,
    )
    heartbeat.start()
    time.sleep(0.6)
    heartbeat.stop()
    all_events = []
    while True:
        try:
            all_events.append(events.get_nowait())
        except queue.Empty:
            break
    assert len(all_events) == 5
    for item, next_item in zip(all_events, all_events[1:]):
        assert item == next_item
        assert item.type == EventType.HeartbeatTime


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
                await trio.sleep(0.5)
                heartbeat.stop()
    assert len(GLOBAL_ITEMS) == 5
    for item, next_item in zip(GLOBAL_ITEMS, GLOBAL_ITEMS[1:]):
        assert item == next_item
        assert item.type == EventType.HeartbeatTime
