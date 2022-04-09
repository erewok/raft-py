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


async def channel_collector(receive_channel: trio.abc.ReceiveChannel, event: trio.Event):
    GLOBAL_ITEMS.clear()
    async with receive_channel:
        async for item in receive_channel:
            GLOBAL_ITEMS.append(item)
            if event.is_set():
                return None


# # # # # # # # # # # # # # # # #
# AsyncEventController Tests
# # # # # # # # # # # # # # # # #
async def test_runstop_heartbeat(controller):
    async with trio.open_nursery() as nursery:
        event = trio.Event()
        send_channel, receive_channel = trio.open_memory_channel(40)
        nursery.start_soon(controller.run_heartbeat, send_channel)
        nursery.start_soon(channel_collector, receive_channel, event)
        await trio.sleep(0.1)
        assert controller.cancel_scopes.get("heartbeat")
        controller.stop_heartbeat()
    assert controller.heartbeat is None
    assert not ("heartbeat" in controller.cancel_scopes)
    # Imperfect
    assert len(GLOBAL_ITEMS) >= 4
    for item, next_item in zip(GLOBAL_ITEMS, GLOBAL_ITEMS[1:]):
        assert item == next_item
        assert item.type == EventType.HeartbeatTime


async def test_runstop_election_timeout_timer(controller):
    async with trio.open_nursery() as nursery:
        event = trio.Event()
        controller.set_nursery(nursery)
        send_channel, receive_channel = trio.open_memory_channel(40)
        nursery.start_soon(channel_collector, receive_channel, event)
        nursery.start_soon(controller.run_election_timeout_timer, send_channel)
        await trio.sleep(0.1)
        assert controller.cancel_scopes.get("election_timer")
        controller.stop_election_timer()
    assert controller.election_timer is None
    assert not ("election_timer" in controller.cancel_scopes)

    assert len(GLOBAL_ITEMS) >= 4
    for item, next_item in zip(GLOBAL_ITEMS, GLOBAL_ITEMS[1:]):
        assert item == next_item
        assert item.type == EventType.ElectionTimeoutStartElection


async def test_runstop_controller(controller, fig7_sample_message, request_vote_message):
    fig7_sample_message.dest = controller.address
    async with trio.open_nursery() as nursery:
        event = trio.Event()
        controller.set_nursery(nursery)
        send_channel, receive_channel = trio.open_memory_channel(40)
        async with send_channel, receive_channel:
            nursery.start_soon(channel_collector, receive_channel, event)
            nursery.start_soon(controller.run, send_channel)

            await controller.send_outbound_msg(fig7_sample_message)
            await controller.send_outbound_msg(request_vote_message)
            await trio.sleep(0.2)
            event.set()
        await trio.sleep(0.2)
        controller.stop()

    assert len(GLOBAL_ITEMS) == 4
    # Should be a mixture of heartbeats, and a logappend and a request vote
    for item, next_item in zip(GLOBAL_ITEMS, GLOBAL_ITEMS[1:]):
        assert item == next_item
        assert item.type == EventType.ElectionTimeoutStartElection


# # # # # # # # # # # # # # # # #
# AsyncRuntime Tests
# # # # # # # # # # # # # # # # #
