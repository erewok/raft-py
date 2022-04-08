import pytest
import trio

from raft.io import storage
from raft.models import EventType
from raft.runtimes import aio


GLOBAL_ITEMS = []


@pytest.fixture()
def controller(config):
    return aio.AsyncEventController(
        1,
        config
    )


@pytest.fixture()
def runner(config):
    return aio.AsyncRuntime(
        1,
        config,
        storage.AsyncFileStorage
    )


async def channel_collector(receive_channel: trio.abc.ReceiveChannel):
    GLOBAL_ITEMS.clear()
    async with receive_channel:
        async for item in receive_channel:
            GLOBAL_ITEMS.append(item)


# # # # # # # # # # # # # # # # #
# AsyncEventController Tests
# # # # # # # # # # # # # # # # #
async def test_runstop_heartbeat(controller):
    async with trio.open_nursery() as nursery:
        send_channel, receive_channel = trio.open_memory_channel(40)
        async with send_channel, receive_channel:
            nursery.start_soon(channel_collector, receive_channel.clone())
            nursery.start_soon(controller.run_heartbeat, send_channel.clone())
            await trio.sleep(0.1)
            assert controller.cancel_scopes.get("heartbeat")
            controller.stop_heartbeat()
    # Imperfect
    assert len(GLOBAL_ITEMS) > 30
    for item, next_item in zip(GLOBAL_ITEMS, GLOBAL_ITEMS[1:]):
        assert item == next_item
        assert item.type == EventType.HeartbeatTime


async def test_runstop_election_timeout_timer(controller):
    async with trio.open_nursery() as nursery:
        send_channel, receive_channel = trio.open_memory_channel(40)
        async with send_channel, receive_channel:
            nursery.start_soon(channel_collector, receive_channel.clone())
            nursery.start_soon(controller.run_election_timeout_timer, send_channel.clone())
            await trio.sleep(0.1)
            assert controller.cancel_scopes.get("election_timer")
            controller.stop_election_timer()
    # Median should be half what heartbeat is
    assert len(GLOBAL_ITEMS) >= 15
    for item, next_item in zip(GLOBAL_ITEMS, GLOBAL_ITEMS[1:]):
        assert item == next_item
        assert item.type == EventType.ElectionTimeoutStartElection


# # # # # # # # # # # # # # # # #
# AsyncRuntime Tests
# # # # # # # # # # # # # # # # #
