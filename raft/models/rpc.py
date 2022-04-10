import enum
import json
from abc import ABC, abstractmethod
from operator import methodcaller
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union

from raft.io import transport
from raft.models.log import LogEntry

RPC = TypeVar("RPC", bound="RpcBase")


@enum.unique
class MsgType(enum.IntEnum):
    RequestVoteRequest = 1
    RequestVoteResponse = 2
    AppendEntriesRequest = 3
    AppendEntriesResponse = 4
    ClientRequest = 5
    DEBUG_MESSAGE = 99

    def __str__(self):
        return self.name


def parse_msg(msg_bytes: bytes):
    try:
        data = json.loads(msg_bytes)
    except (ValueError, TypeError):
        data = {}
    msg_type = data.get("type")
    if msg_type == MsgType.RequestVoteRequest:
        return RequestVoteRpc.from_dict(data)
    elif msg_type == MsgType.RequestVoteResponse:
        return RequestVoteResponse.from_dict(data)
    elif msg_type == MsgType.AppendEntriesRequest:
        return AppendEntriesRpc.from_dict(data)
    elif msg_type == MsgType.AppendEntriesResponse:
        return AppendEntriesResponse.from_dict(data)
    elif msg_type == MsgType.ClientRequest:
        return ClientRequest(
            cmd=data.get("body", "").encode("utf-8"),
            source=tuple(data.get("callback_addr", [])),
        )
    elif msg_type == MsgType.DEBUG_MESSAGE:
        return Debug()

    # if we got here, we couldn't successfully parse this thing
    raise ValueError("This message is unparseable as an RPC message")


class Debug(Generic[RPC]):
    __slots__ = ["type", "dest", "source", "term"]

    def __init__(
        self,
        dest: Optional[transport.Address] = None,
        source: Optional[transport.Address] = None,
    ):
        self.type = MsgType.DEBUG_MESSAGE
        self.dest = dest
        self.source = source
        self.term = -99

    def to_bytes(self) -> bytes:
        return b'{"type": 99}'


class ClientRequest(Generic[RPC]):
    __slots__ = ["type", "command", "source"]

    def __init__(self, cmd=b"", source=("", 1000)):
        self.type = MsgType.ClientRequest
        self.command = cmd
        self.source = source


class RpcBase(ABC, Generic[RPC]):  # noqa
    """Implementors should only need a `to_dict` if inheriting from this class"""

    def __eq__(self, other):
        return self.to_dict() == other.to_dict()

    def __repr__(self):
        return str(self.to_dict())

    @abstractmethod
    def to_dict(self):
        raise NotImplementedError("Implement `to_dict`")

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    def to_bytes(self) -> bytes:
        return self.to_json().encode("utf-8")


class AppendEntriesRpc(RpcBase, Generic[RPC]):
    __slots__ = [
        "term",
        "leader_id",
        "prev_log_index",
        "prev_log_term",
        "entries",
        "leader_commit_index",
        "type",
        "dest",
        "source",
    ]

    def __init__(
        self,
        term: int = -1,
        leader_id: int = -1,
        prev_log_index: int = -1,
        prev_log_term: int = -1,
        entries: List[LogEntry] = None,
        leader_commit_index: int = -1,
        dest: Optional[transport.Address] = None,
        source: Optional[transport.Address] = None,
    ):
        entries = entries or []
        self.term = term
        self.leader_id = leader_id
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = entries
        self.leader_commit_index = leader_commit_index
        self.dest = dest
        self.source = source
        self.type = MsgType.AppendEntriesRequest

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "leader_id": self.leader_id,
            "prev_log_index": self.prev_log_index,
            "prev_log_term": self.prev_log_term,
            "entries": list(map(methodcaller("to_dict"), self.entries)),
            "leader_commit_index": self.leader_commit_index,
            "type": int(MsgType.AppendEntriesRequest),
            "dest": self.dest,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, data: dict):
        if data.get("type") != MsgType.AppendEntriesRequest:
            raise ValueError("Msg is not an AppendEntriesRequest")
        return cls(
            **{
                "term": data.get("term", -1),
                "leader_id": data.get("leader_id", -1),
                "prev_log_index": data.get("prev_log_index", -1),
                "prev_log_term": data.get("prev_log_term", -1),
                "entries": [LogEntry.from_json(it) for it in data.get("entries", [])],
                "leader_commit_index": data.get("leader_commit_index", -1),
                "dest": tuple(data.get("dest")),
                "source": tuple(data.get("source")),
            }
        )


class AppendEntriesResponse(RpcBase, Generic[RPC]):
    __slots__ = [
        "term",
        "success",
        "match_index",
        "type",
        "source_node_id",
        "dest",
        "source",
    ]

    def __init__(
        self,
        term: int = -1,
        match_index: int = -1,
        source_node_id: int = -1,
        success: bool = False,
        dest: Optional[transport.Address] = None,
        source: Optional[transport.Address] = None,
    ):
        self.term = term
        self.success = success
        self.match_index = match_index
        self.source_node_id = source_node_id
        self.dest = dest
        self.source = source
        self.type = MsgType.AppendEntriesResponse

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "match_index": self.match_index,
            "success": self.success,
            "source_node_id": self.source_node_id,
            "type": int(self.type),
            "dest": self.dest,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, data: dict):
        if data.get("type") != MsgType.AppendEntriesResponse:
            raise ValueError("Msg is not an AppendEntriesResponse")
        return cls(
            **{
                "term": data.get("term", -1),
                "match_index": data.get("match_index", -1),
                "success": data.get("success", False),
                "source_node_id": data.get("source_node_id", -1),
                "dest": tuple(data.get("dest")),
                "source": tuple(data.get("source")),
            }
        )


class RequestVoteRpc(RpcBase, Generic[RPC]):
    __slots__ = [
        "term",
        "candidate_id",
        "last_log_index",
        "last_log_term",
        "type",
        "dest",
        "source",
    ]

    def __init__(
        self,
        term: int,
        candidate_id: int,
        last_log_index: int,
        last_log_term: int,
        dest: Optional[transport.Address] = None,
        source: Optional[transport.Address] = None,
    ):
        self.term = term
        self.candidate_id = candidate_id
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term
        self.dest = dest
        self.source = source
        self.type = MsgType.RequestVoteRequest

    def to_dict(self) -> Dict:
        return {
            "term": self.term,
            "candidate_id": self.candidate_id,
            "last_log_index": self.last_log_index,
            "last_log_term": self.last_log_term,
            "type": int(self.type),
            "dest": self.dest,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, data: dict):
        if data.get("type") != MsgType.RequestVoteRequest:
            raise ValueError("Msg is not an RequestVoteRequest")
        return cls(
            **{
                "term": data.get("term", -1),
                "candidate_id": data.get("candidate_id", False),
                "last_log_index": data.get("last_log_index", -1),
                "last_log_term": data.get("last_log_term", -1),
                "dest": tuple(data.get("dest")),
                "source": tuple(data.get("source")),
            }
        )


class RequestVoteResponse(RpcBase, Generic[RPC]):
    __slots__ = ["term", "vote_granted", "type", "source_node_id", "dest", "source"]

    def __init__(
        self,
        term: int,
        source_node_id: int,
        vote_granted: bool,
        dest: Optional[transport.Address] = None,
        source: Optional[transport.Address] = None,
    ):
        self.term = term
        self.vote_granted = vote_granted
        self.source_node_id = source_node_id
        self.dest = dest
        self.source = source
        self.type = MsgType.RequestVoteResponse

    def to_dict(self) -> Dict:
        return {
            "term": self.term,
            "vote_granted": self.vote_granted,
            "source_node_id": self.source_node_id,
            "type": int(self.type),
            "dest": self.dest,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, data: dict):
        if data.get("type") != MsgType.RequestVoteResponse:
            raise ValueError("Msg is not an RequestVoteResponse")
        return cls(
            **{
                "term": data.get("term", -1),
                "vote_granted": data.get("vote_granted", False),
                "source_node_id": data.get("source_node_id", -1),
                "dest": tuple(data.get("dest")),
                "source": tuple(data.get("source")),
            }
        )


RPCMessage = Union[
    RequestVoteResponse[RPC],
    RequestVoteRpc[RPC],
    AppendEntriesResponse[RPC],
    AppendEntriesRpc[RPC],
    Debug[RPC],
    ClientRequest[RPC],
]
