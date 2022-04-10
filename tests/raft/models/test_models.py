import json

import pytest

from raft import models

TYPICAL_SOURCE_DEST = {"dest": ("127.0.0.1", 3112), "source": ("127.0.0.1", 3111)}


@pytest.mark.parametrize(
    "msg_data,expected_event_type",
    (
        ({}, None),
        (
            {
                "term": 11,
                "leader_id": 1,
                "prev_log_index": 11,
                "prev_log_term": 11,
                "entries": [{"command": "a", "term": 11}],
                "leader_commit_index": 1,
                "type": models.rpc.MsgType.AppendEntriesRequest.value,
                **TYPICAL_SOURCE_DEST,
            },
            models.EventType.LeaderAppendLogEntryRpc,
        ),
        (
            {
                "term": 11,
                "success": True,
                "match_index": 10,
                "type": models.rpc.MsgType.AppendEntriesResponse.value,
                **TYPICAL_SOURCE_DEST,
            },
            models.EventType.AppendEntryConfirm,
        ),
        (
            {
                "term": 12,
                "candidate_id": 1,
                "last_log_index": 11,
                "last_log_term": 11,
                "type": models.rpc.MsgType.RequestVoteRequest.value,
                **TYPICAL_SOURCE_DEST,
            },
            models.EventType.CandidateRequestVoteRpc,
        ),
        (
            {
                "term": 12,
                "granted": True,
                "type": models.rpc.MsgType.RequestVoteResponse.value,
                **TYPICAL_SOURCE_DEST,
            },
            models.EventType.ReceiveServerCandidateVote,
        ),
        (
            {
                "type": models.rpc.MsgType.DEBUG_MESSAGE.value,
                **TYPICAL_SOURCE_DEST,
            },
            models.EventType.DEBUG_REQUEST,
        ),
        (
            {
                "type": models.rpc.MsgType.ClientRequest.value,
                "body": "GET a",
                "callback_addr": ["127.0.0.1", 9999],
            },
            models.EventType.ClientAppendRequest,
        ),
    ),
)
def test_parse_msg_to_event(msg_data, expected_event_type):
    result = models.parse_msg_to_event(json.dumps(msg_data))
    if expected_event_type is None:
        assert result is None
    else:
        assert result.type is expected_event_type
