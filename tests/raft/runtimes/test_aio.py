import pytest
import trio

from raft.io import storage
from raft.models import EventType, MsgType
from raft.runtimes import aio

GLOBAL_ITEMS = []


@pytest.fixture()
def controller(config):
    return aio.AsyncEventController(1, config)


@pytest.fixture()
def runner(config):
    return aio.AsyncRuntime(1, config, storage.AsyncFileStorage)


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
        nursery.start_soon(controller.run_heartbeat, send_channel)
        nursery.start_soon(channel_collector, receive_channel)
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
        controller.set_nursery(nursery)
        send_channel, receive_channel = trio.open_memory_channel(40)
        nursery.start_soon(channel_collector, receive_channel)
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


async def test_runstop_controller_with_some_events(
    controller, fig7_sample_message, request_vote_message, debug_msg
):
    fig7_sample_message.dest = controller.address
    request_vote_message.dest = controller.address
    debug_msg.dest = controller.address
    async with trio.open_nursery() as nursery:
        controller.set_nursery(nursery)
        send_channel, receive_channel = trio.open_memory_channel(40)
        nursery.start_soon(channel_collector, receive_channel)
        nursery.start_soon(controller.run, send_channel)
        await trio.sleep(0.2)

        await controller.send_outbound_msg(fig7_sample_message)
        await controller.send_outbound_msg(request_vote_message)
        await controller.send_outbound_msg(debug_msg)
        await trio.sleep(0.2)
        controller.stop()
        nursery.cancel_scope.cancel()

    assert len(GLOBAL_ITEMS) == 3
    # Should be a mixture of heartbeats, and a logappend and a request vote
    first, second, third = GLOBAL_ITEMS
    assert first.type == EventType.LeaderAppendLogEntryRpc
    assert first.msg.type == MsgType.AppendEntriesRequest
    assert second.type == EventType.CandidateRequestVoteRpc
    assert second.msg.type == MsgType.RequestVoteRequest
    assert third.msg.type == MsgType.DEBUG_MESSAGE
    # IT's supposed to explicitly clobber only in this case
    assert third.msg.source == controller.address


# # # # # # # # # # # # # # # # #
# AsyncRuntime Tests
# # # # # # # # # # # # # # # # #


async def test_runstop_runtime_with_some_events(
    runner, fig7_sample_message, request_vote_message, debug_msg
):
    fig7_sample_message.dest = runner.event_controller.address
    request_vote_message.dest = runner.event_controller.address
    debug_msg.dest = runner.event_controller.address
    async with trio.open_nursery() as nursery:
        nursery.start_soon(runner.run_async)
        await trio.sleep(0.3)

        await runner.event_controller.send_outbound_msg(fig7_sample_message)
        await runner.event_controller.send_outbound_msg(request_vote_message)
        await runner.event_controller.send_outbound_msg(debug_msg)
        await trio.sleep(0.2)
        runner.stop()
        nursery.cancel_scope.cancel()
