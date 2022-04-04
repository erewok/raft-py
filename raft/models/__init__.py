import enum
from typing import Any

# Next == /\ \/ \E i \in Server : Restart(i)
#            \/ \E i \in Server : Timeout(i)
#            \/ \E i,j \in Server : RequestVote(i, j)
#            \/ \E i \in Server : BecomeLeader(i)
#            \/ \E i \in Server, v \in Value : ClientRequest(i, v)
#            \/ \E i \in Server : AdvanceCommitIndex(i)
#            \/ \E i,j \in Server : AppendEntries(i, j)
#            \/ \E m \in DOMAIN messages : Receive(m)
#            \/ \E m \in DOMAIN messages : DuplicateMessage(m)
#            \/ \E m \in DOMAIN messages : DropMessage(m)


@enum.unique
class EventType(enum.IntEnum):
    Tick = 1
    CandidateRequestVoteRpc = 2
    LeaderAppendLogEntryRpc = 3
    ElectionTimeoutStartElection = 4
    ReceiveServerCandidateVote = 6
    SelfWinElection = 7
    AppendEntryConfirm = 9
    HeartbeatTime = 10
    ClientAppendRequest = 11
    ResetElectionTimeout = 12

    # We need to manage timers when various things happen
    ConversionToCandidate = 14
    ConversionToLeader = 15
    ConversionToFollower = 16

    StartHeartbeat = 25
    DEBUG_REQUEST = 99

    def __str__(self):
        return self.name


class Event:
    __slots__ = ["type", "msg"]

    # Any? Not rpc.RPCMessage? https://github.com/pydicom/pydicom/issues/1454
    def __init__(self, etype: EventType, msg: Any):
        self.type = etype
        self.msg = msg


# # # # # # # # # # # # # # # # # #
# Static/Constant Events          #
# # # # # # # # # # # # # # # # # #

# A follower needs the following timers: ElectionTimeout
# A Candidate needs the following timers: ElectionTimeout
# A Leader needs the following timers: Heartbeat
EVENT_SELF_WON_ELECTION = Event(EventType.SelfWinElection, None)
EVENT_CONVERSION_TO_LEADER = Event(EventType.ConversionToLeader, None)
EVENT_CONVERSION_TO_FOLLOWER = Event(EventType.ConversionToFollower, None)
EVENT_HEARTBEAT = Event(EventType.HeartbeatTime, None)
EVENT_START_HEARTBEAT = Event(EventType.StartHeartbeat, None)
