import json

import pytest

from raft.models import rpc

TYPICAL_SOURCE_DEST = {"dest": ("127.0.0.1", 3112), "source": ("127.0.0.1", 3111)}


@pytest.mark.parametrize("val", (b"", b"{}"))
def test_parse_msg_failing(val):
    with pytest.raises(ValueError):
        rpc.parse_msg(val)


def test_parse_append_entries():
    data = json.dumps(
        {
            "term": 11,
            "leader_id": 1,
            "prev_log_index": 11,
            "prev_log_term": 11,
            "entries": [{"command": "a", "term": 11}],
            "leader_commit_index": 1,
            "type": int(rpc.MsgType.AppendEntriesRequest),
            **TYPICAL_SOURCE_DEST,
        }
    )
    result = rpc.parse_msg(data)
    assert result.type is rpc.MsgType.AppendEntriesRequest
    assert result.term == 11
    assert rpc.AppendEntriesRpc.from_dict(result.to_dict()) == result


def test_parse_append_entries_resp():
    data = json.dumps(
        {
            "term": 11,
            "success": True,
            "match_index": 10,
            "type": int(rpc.MsgType.AppendEntriesResponse),
            **TYPICAL_SOURCE_DEST,
        }
    )
    result = rpc.parse_msg(data)
    assert result.type is rpc.MsgType.AppendEntriesResponse
    assert result.term == 11
    assert rpc.AppendEntriesResponse.from_dict(result.to_dict()) == result


def test_parse_request_vote():
    data = json.dumps(
        {
            "term": 12,
            "candidate_id": 1,
            "last_log_index": 11,
            "last_log_term": 11,
            "type": int(rpc.MsgType.RequestVoteRequest),
            **TYPICAL_SOURCE_DEST,
        }
    )
    result = rpc.parse_msg(data)
    assert result.type is rpc.MsgType.RequestVoteRequest
    assert result.term == 12
    assert rpc.RequestVoteRpc.from_dict(result.to_dict()) == result


def test_parse_request_vote_resp():
    data = json.dumps(
        {
            "term": 12,
            "granted": True,
            "type": int(rpc.MsgType.RequestVoteResponse),
            **TYPICAL_SOURCE_DEST,
        }
    )
    result = rpc.parse_msg(data)
    assert result.type is rpc.MsgType.RequestVoteResponse
    assert result.term == 12
    # Test that to_dict and from_dict are isomorphic
    assert rpc.RequestVoteResponse.from_dict(result.to_dict()) == result


def test_parse_debug():
    msg = json.dumps({"type": int(rpc.MsgType.DEBUG_MESSAGE)})
    result = rpc.parse_msg(msg)
    assert result.type is rpc.MsgType.DEBUG_MESSAGE
    assert result.term == -99


def test_parse_client_request():
    msg = json.dumps(
        {
            "type": rpc.MsgType.ClientRequest.value,
            "body": "GET a",
            "callback_addr": ["127.0.0.1", 9999],
        }
    )
    result = rpc.parse_msg(msg)
    assert result.type is rpc.MsgType.ClientRequest
    assert result.command == b"GET a"
    assert result.source == ("127.0.0.1", 9999)
