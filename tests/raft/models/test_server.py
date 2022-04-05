from __future__ import nested_scopes
import pytest

from raft import models
from raft.models import Event, EventType
from raft.models import rpc
from raft.models import server


@pytest.fixture
def candidate_request_vote_event(config):
    def inner(node_id, term, dest):
        return Event(
            EventType.CandidateRequestVoteRpc,
            rpc.RequestVoteRpc(
                term=term,
                candidate_id=node_id,
                last_log_index=-1,
                last_log_term=-1,
                dest=dest,
                source=config.node_mapping[node_id]["addr"],
            ),
        )

    return inner


@pytest.fixture
def receive_server_candidate_vote_event(config):
    def inner(node_id, term, granted, dest):
        return Event(
            EventType.ReceiveServerCandidateVote,
            rpc.RequestVoteResponse(
                term=term,
                source_node_id=node_id,
                vote_granted=granted,
                dest=dest,
                source=config.node_mapping[node_id]["addr"],
            ),
        )

    return inner


def is_empty_response(resp: server.ResponsesEvents) -> bool:
    return not resp.responses and not resp.events


# Testing Conversions from one Server type to another
def test_candidate_validate_conversions(candidate):
    # any other server type is valid
    assert candidate.validate_conversion(server.Follower) is True
    assert candidate.validate_conversion(server.Candidate) is True
    assert candidate.validate_conversion(server.Leader) is True


def test_follower_validate_conversions(follower):
    with pytest.raises(ValueError):
        follower.validate_conversion(server.Follower)
    with pytest.raises(ValueError):
        follower.validate_conversion(server.Leader)
    with pytest.raises(ValueError):
        # Some random value...
        follower.validate_conversion(server.Leader.mro()[1])
    # only this is valid
    assert follower.validate_conversion(server.Candidate) is True


def test_leader_validate_conversions(leader):
    with pytest.raises(ValueError):
        leader.validate_conversion(server.Candidate)
    with pytest.raises(ValueError):
        leader.validate_conversion(server.Leader)
    with pytest.raises(ValueError):
        # Some random value...
        leader.validate_conversion(server.Leader.mro()[1])
    # only this is valid
    assert leader.validate_conversion(server.Follower) is True


# Testing Follower Log Append
@pytest.mark.parametrize(
    "log_to_pull,success_expected",
    (
        ("a_log", False),
        ("b_log", False),
        ("c_log", True),
        ("d_log", True),
        ("e_log", False),
        ("f_log", False),
    ),
)
def test_follower_msg_append(
    log_to_pull, success_expected, follower, figure7_logs, fig7_sample_message
):
    follower.log = figure7_logs[log_to_pull]
    follower.commit_index = 4
    follower.current_term = follower.log[-1].term
    event = Event(EventType.LeaderAppendLogEntryRpc, fig7_sample_message)
    result, more_events = follower.handle_append_entries_message(event)
    # We expect an event to trigger election timeout restart in runtime
    assert len(more_events) == 1
    assert more_events[0].type == EventType.ResetElectionTimeout
    # we also expect a response message
    assert len(result) == 1
    resp = result[0]
    assert resp.success is success_expected
    assert resp.source == fig7_sample_message.dest
    assert resp.dest == fig7_sample_message.source
    expected_match = 10 if success_expected else -1
    assert resp.match_index == expected_match

    # test the higher-level function
    inst, (resps, more_events) = follower.handle_event(event)
    assert inst is follower
    # We expect an event to trigger election timeout restart in runtime
    assert len(more_events) == 1
    assert more_events[0].type == EventType.ResetElectionTimeout
    resp = resps[0]
    assert resp.success is success_expected
    assert resp.source == fig7_sample_message.dest
    assert resp.dest == fig7_sample_message.source


# Testing Candidate conversions
@pytest.mark.parametrize(
    "etype,new_class,events",
    (
        (
            EventType.SelfWinElection,
            server.Leader,
            [models.EVENT_CONVERSION_TO_LEADER, models.EVENT_START_HEARTBEAT],
        ),
        (
            EventType.LeaderAppendLogEntryRpc,
            server.Follower,
            [models.EVENT_CONVERSION_TO_FOLLOWER],
        ),
        (EventType.Tick, None, []),
    ),
)
def test_follower_candidate_convert(etype, new_class, events, candidate):
    event = Event(etype, None)
    inst, resps = candidate.handle_event(event)
    if etype == EventType.Tick:
        assert is_empty_response(resps)
    else:
        assert not is_empty_response(resps)
        for left, right in zip(events, resps.events):
            assert left == right

    if new_class is None:
        assert inst == candidate
        return None  # Further tests below are about conversion

    assert isinstance(inst, new_class)
    assert id(inst) != id(candidate)
    if isinstance(inst, server.Candidate):
        assert inst.current_term == candidate.current_term + 1
    for key in filter(lambda el: el != "current_term", inst.transfer_attrs):
        assert getattr(inst, key) == getattr(candidate, key)


# Election starts (or restarts for Candidate) -> Returns new candidate and Sends Vote Requests
def test_candidate_construct_request_vote_rpcs(follower, candidate):
    """There's a lot going on in this test"""
    for raft_server in (follower, candidate):
        event = Event(EventType.ElectionTimeoutStartElection, None)
        new_candidate, resp_events = raft_server.handle_event(event)
        assert new_candidate is not raft_server
        resps, empty = new_candidate.handle_start_election(event)
        assert resp_events and resp_events == (resps, empty)
        assert resps
        assert not empty  # no successive events
        assert new_candidate.current_term == raft_server.current_term + 1
        for key in filter(lambda el: el != "current_term", raft_server.transfer_attrs):
            assert getattr(raft_server, key) == getattr(new_candidate, key)

        for node_id, msg in zip(new_candidate.all_node_ids, resps):
            # make sure leader is not including itself in recipients
            assert new_candidate.address != msg.dest
            # Check recipient is correct
            assert new_candidate.config.node_mapping[node_id]["addr"] == msg.dest
            # make sure leader is telling nodes where to send replies
            assert new_candidate.address == msg.source
            # Get the message and confirm its values make sense
            msg.last_log_index == len(new_candidate.log) - 1
            msg.last_log_term == new_candidate.log[-1].term if new_candidate.log else 0


def test_follower_handle_request_vote_rpc(
    follower, fig7_a_log, candidate_request_vote_event
):
    follower.log = fig7_a_log
    follower.current_term = follower.log[-1].term

    # Grant vote if log long enough and nobody voted for yet
    event = candidate_request_vote_event(1, 8, ("127.0.0.1", 3112))
    event.msg.last_log_index = len(follower.log) + 1
    event.msg.last_log_term = follower.log[-1].term
    follower.node_id = 2

    results = follower.handle_request_vote_rpc(event)
    assert not is_empty_response(results)
    resp = results.responses[0]
    assert resp.vote_granted
    assert follower.voted_for == 1
    assert resp.dest == event.msg.source
    assert resp.source == event.msg.dest
    assert len(results.events) == 1
    assert results.events[0].type == EventType.ResetElectionTimeout

    # mark it something else and see vote rejected
    follower.voted_for = 3
    results = follower.handle_request_vote_rpc(event)
    assert not is_empty_response(results)
    resp = results.responses[0]
    assert not resp.vote_granted
    assert follower.voted_for == 3
    assert resp.dest == event.msg.source
    assert resp.source == event.msg.dest
    assert not results.events

    # reset to None and check the log_index is long enough for vote
    follower.voted_for = None
    event.msg.last_log_index = 2
    results = follower.handle_request_vote_rpc(event)
    assert not is_empty_response(results)
    resp = results.responses[0]
    assert not resp.vote_granted
    assert follower.voted_for is None
    assert resp.dest == event.msg.source
    assert resp.source == event.msg.dest
    assert not results.events

    inst, resps_events = follower.handle_event(event)
    assert inst is follower
    assert inst.current_term == event.msg.term
    result, empty = resps_events
    assert not empty
    assert not result[0].vote_granted
    assert not results.events


def test_handle_vote_response(
    candidate, follower, fig7_a_log, candidate_request_vote_event
):
    follower.log = fig7_a_log
    follower.current_term = follower.log[-1].term
    # Reusing logic from follower-request vote tests
    event = candidate_request_vote_event(1, 8, ("127.0.0.1", 3112))
    event.msg.last_log_index = len(follower.log) + 1
    event.msg.last_log_term = follower.log[-1].term
    follower.node_id = 2
    responses, _ = follower.handle_request_vote_rpc(event)
    # sanity check
    assert responses
    resp_event = Event(EventType.ReceiveServerCandidateVote, responses[0])
    result = candidate.handle_vote_response(resp_event)
    assert is_empty_response(result)
    # Idempotent: same response doesn't magically make them win
    result = candidate.handle_vote_response(resp_event)
    assert is_empty_response(result)
    # After they have more votes, they can win
    candidate.votes_received = set((3, 4))
    result = candidate.handle_vote_response(resp_event)
    assert not is_empty_response(result)
    _, events = result
    assert events
    assert events[0].type == EventType.SelfWinElection


# Testing Leader events


def test_leader_handle_event(leader, sample_append_confirm_rpc):
    etype = EventType.AppendEntryConfirm
    event = Event(etype, sample_append_confirm_rpc)
    leader.current_term = 20
    # should not convert
    inst, maybe_resp_evs = leader.handle_event(event)
    assert is_empty_response(maybe_resp_evs)
    assert inst is leader

    # Try to coerce it to convert
    event.msg.term = 100
    inst2, maybe_resp_evs = leader.handle_event(event)
    assert inst2 is not leader
    assert not is_empty_response(maybe_resp_evs)
    assert len(maybe_resp_evs.events) == 1
    assert maybe_resp_evs.events[0] == models.EVENT_CONVERSION_TO_FOLLOWER
    assert isinstance(inst2, server.Follower)


# Testing Leader Steps down
def check_that_leader_steps_down_if_out_of_date(leader, sample_append_confirm_rpc):
    event = Event(EventType.AppendEntryConfirm, sample_append_confirm_rpc)
    event.msg.term = 20
    # In this case, the leader realizes it's out of date and immediately converts
    inst, maybe_resp_evs = leader.handle_event(event)
    assert maybe_resp_evs is not None
    assert inst is not leader
    assert isinstance(inst, server.Follower)


# Testing Send Append Entries RPC
def test_leader_handle_heartbeat(leader):
    """There's a lot going on in this test"""
    event = Event(EventType.HeartbeatTime, None)
    inst, (resps, empty) = leader.handle_event(event)
    # no conversion happened
    assert not empty
    assert inst is leader
    for node_id, msg in zip(leader.all_node_ids, resps):
        # make sure leader is not including itself in recipients
        assert leader.address != msg.dest
        # Check recipient is correct
        assert leader.config.node_mapping[node_id]["addr"] == msg.dest
        # make sure leader is telling nodes where to send replies
        assert leader.address == msg.source
        # Get the message and confirm its values make sense
        msg.prev_log_index == leader.next_index[node_id] - 1
        len(msg.entries) + leader.next_index[node_id] == len(leader.log)


# This should be a *known* client!
APPEND_SUCCESS = Event(
    EventType.AppendEntryConfirm,
    rpc.AppendEntriesResponse(
        term=6, match_index=10, source_node_id=2, success=True, dest=("127.0.0.1", 3112)
    ),
)

# This should be a *known* client!
APPEND_FAIL = Event(
    EventType.AppendEntryConfirm,
    rpc.AppendEntriesResponse(
        term=1, match_index=2, source_node_id=4, success=False, dest=("127.0.0.1", 3114)
    ),
)


# Testing Leader handling append
def test_leader_handle_append_response(leader):
    """There's a lot going on in this test"""
    inst, resps = leader.handle_event(APPEND_SUCCESS)
    assert is_empty_response(resps)
    assert inst is leader
    assert leader.commit_index == 8
    assert leader.match_index[APPEND_SUCCESS.msg.source_node_id] == len(leader.log)
    assert leader.next_index[APPEND_SUCCESS.msg.source_node_id] == len(leader.log) + 1

    # before failure: grab current expected next_index
    current_next_for_node = leader.next_index[APPEND_FAIL.msg.source_node_id]
    current_match_for_node = leader.next_index[APPEND_FAIL.msg.source_node_id]

    inst, resps = leader.handle_event(APPEND_FAIL)
    assert is_empty_response(resps)
    assert inst is leader
    assert leader.commit_index == 8
    assert leader.match_index[APPEND_FAIL.msg.source_node_id] == current_match_for_node
    assert (
        leader.next_index[APPEND_FAIL.msg.source_node_id] == current_next_for_node - 1
    )
