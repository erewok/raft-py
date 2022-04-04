"""
The term "Server" here may be confusing, but it's meant to follow
strictly what the Raft paper calls a "server".

A Raft `Server` may be _only_ one of the following:
  - Follower
  - Candidate
  - Leader

In addition, there may only be one leader at a time in a cluster.

The raft paper describes the valid state transitions from one server type
to another as follows:

  - Follower ->  { Candidate }
  - Candidate -> { Follower, Candidate, Leader }
  - Leader ->    { Follower }
"""
from __future__ import annotations

import json
import logging
from collections import namedtuple
from typing import Generic, List, Set, Tuple, TypeVar, Union

from raft.io import loggers, transport
from raft.models import (
    EVENT_CONVERSION_TO_FOLLOWER,
    EVENT_CONVERSION_TO_LEADER,
    EVENT_HEARTBEAT,
    EVENT_SELF_WON_ELECTION,
    EVENT_START_HEARTBEAT,
    Event,
    EventType,
    log,
    rpc,
)
from raft.models.config import Config

logger = logging.getLogger(__name__)
# In some cases, we want to trigger _new_ events _from_ events
# We may also want to issue _responses_.
# We need to disambiguate these.
ResponsesEvents = namedtuple("ResponsesEvents", ("responses", "events"))
S = TypeVar("S", bound="BaseServer")
# This is defined at the bottom
# Server = Union[Leader[S], Candidate[S], Follower[S]]


def empty_response() -> ResponsesEvents:
    return ResponsesEvents([], [])


class BaseServer(Generic[S]):
    def __init__(self, node_id: int, config: Config, storage):
        # must be persisted to storage (from raft paper)
        self.current_term = 1
        self.voted_for = None
        self.log: log.Log = log.Log()
        # volatile (from raft paper)
        self.commit_index = -1
        self.last_applied = -1
        # this is implementation specific
        self.applied: List[log.LogEntry] = []
        self.storage = storage
        self.config = config
        self.node_id = node_id
        self.all_node_ids = list(
            filter(lambda el: el != self.node_id, self.config.node_mapping.keys())
        )
        self.quorom: int = (len(self.all_node_ids) // 2) + 1
        this_node = self.config.node_mapping[self.node_id]
        self.label = this_node["label"]
        self.host: str = this_node["addr"][0]
        self.port: int = this_node["addr"][1]
        self.transfer_attrs = (
            "commit_index",
            "last_applied",
            "config",
            "node_id",
            "all_node_ids",
            "label",
            "host",
            "port",
            "transfer_attrs",
            "current_term",
            "log",
        )

    @classmethod
    def log_name(cls):
        return "Server"

    @property
    def address(self):
        return (self.host, self.port)

    def save_meta(self):
        self.storage.save_metadata(
            json.dumps({"votedFor": self.voted_for, "currentTerm": self.current_term})
        )

    def save_log_entry(self):
        self.storage.save(self.log[-1])

    def convert(self, target_class) -> S:
        logger.warning(f"Converting from {self._log_name} to {target_class.log_name()}")
        self.validate_conversion(target_class)
        new_server = target_class(self.node_id, self.config, self.storage)
        for attr in new_server.transfer_attrs:
            setattr(new_server, attr, getattr(self, attr))
        return new_server

    def validate_conversion(self, target_class):  # noqa
        return target_class in {Candidate, Follower, Leader}


# # # # # # # # # # # # # # # # # #
#
# Candidate
#
# # # # # # # # # # # # # # # # # #


class Candidate(BaseServer, Generic[S]):
    """
    A Candidate can become one of:
    - a Follower, or
    - a Leader, or
    - _another_ Candidate.

    Each _new_ Candidate will _increment_ the `current_term`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # node_ids get appended here if they vote for us
        self.votes_received: Set[int] = set((self.node_id,))
        self._log_name = self.log_name()

    @classmethod
    def log_name(cls):
        if loggers.RICH_HANDLING_ON:
            return "[bold yellow]Candidate[/]"
        return "Candidate"

    def increment_term(self):
        self.current_term += 1
        return self

    def construct_request_vote_rpcs(self) -> List[transport.Request]:
        all_msgs: List[transport.Request] = []

        for node_id in self.all_node_ids:
            addr = self.config.node_mapping[node_id]["addr"]
            msg: rpc.RPCMessage = rpc.RequestVoteRpc(
                term=self.current_term,
                candidate_id=self.node_id,
                last_log_index=len(self.log) - 1,
                last_log_term=self.log[-1].term if self.log else 0,
                dest=addr,
                source=self.address,
            )
            all_msgs.append(msg)  # type: ignore
        return all_msgs

    def handle_start_election(self, _: Event) -> ResponsesEvents:
        all_msgs = self.construct_request_vote_rpcs()
        return ResponsesEvents(all_msgs, [])

    def handle_vote_response(self, event: Event) -> ResponsesEvents:
        if event.msg.source_node_id not in self.votes_received:
            self.votes_received.add(event.msg.source_node_id)

        if len(self.votes_received) >= self.quorom:
            logger.info(f"{self._log_name} has received votes from a quorum of servers")
            # We should immediately trigger a heartbeat here
            # to assert our leader's EventType.HeartbeatTime
            # We're also supposed to commit a NOOP into the log
            events = [
                EVENT_SELF_WON_ELECTION,
                EVENT_HEARTBEAT,
            ]
            return ResponsesEvents([], events)
        return empty_response()

    def handle_event(self, event: Event) -> Tuple[Server, ResponsesEvents]:
        event_term = -2
        if event.msg and hasattr(event.msg, "term"):
            event_term = event.msg.term
        responses_events = empty_response()
        if (
            event.type == EventType.LeaderAppendLogEntryRpc
            or event_term > self.current_term
        ):
            self.current_term = event_term
            return (
                self.convert(Follower),
                ResponsesEvents([], [EVENT_CONVERSION_TO_FOLLOWER]),
            )
        if event.type == EventType.ElectionTimeoutStartElection:
            logger.info(f"{self._log_name} is calling a new election")
            new_instance = self.convert(Candidate).increment_term()
            responses_events = new_instance.handle_start_election(event)
            return new_instance, responses_events
        if event.type == EventType.SelfWinElection:
            logger.info(f"{self._log_name} has won the election")
            leader = self.convert(Leader)
            return (
                leader,
                ResponsesEvents(
                    [], [EVENT_CONVERSION_TO_LEADER, EVENT_START_HEARTBEAT]
                ),
            )
        if event.type == EventType.ReceiveServerCandidateVote:
            responses_events = self.handle_vote_response(event)
        return self, responses_events

    def validate_conversion(self, target_class):
        """
        A candidate may be converted into any other class.

        This includes _another_ Candidate if its election fails and a new one starts.
        """
        return target_class in {Candidate, Follower, Leader}


# # # # # # # # # # # # # # # # # #
#
# Follower
#
# # # # # # # # # # # # # # # # # #


class Follower(BaseServer, Generic[S]):
    """
    All servers start as Followers.

    A Follower can become a Candidate.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.known_leader_node_id = None
        self._log_name = self.log_name()

    @classmethod
    def log_name(cls):
        if loggers.RICH_HANDLING_ON:
            return "[bold green]Follower[/]"
        return "Follower"

    def handle_append_entries_message(self, event: Event) -> ResponsesEvents:
        # An RPC sent by leader to replicate log entries (see Raft §5.3)
        # If entries is an empty list, this is meant to be a heartbeat (see Raft §5.2).
        logger.info(
            f"{self._log_name} Received new append entries request with {len(event.msg.entries)} entries"
        )
        success = self.log.append_entries(  # type: ignore
            prev_term=event.msg.prev_log_term,
            prev_index=event.msg.prev_log_index,
            entries=event.msg.entries,
        )
        self.voted_for = None  # This looks like a great place for bugs
        if success:
            # We have a leader if we have received this event
            self.known_leader_node_id = event.msg.leader_id  # type: ignore
            # LEader sends commit index: try to advance ours
            new_commit_index = min(event.msg.leader_commit_index, len(self.log))
            if new_commit_index > self.commit_index:
                self.commit_index = new_commit_index
                logger.info(
                    f"{self._log_name} Committed entries count is now {self.commit_index}"
                )

                if self.commit_index > self.last_applied:
                    entries = self.log.log[
                        self.last_applied + 1 : self.commit_index + 1
                    ]
                    self.applied.extend(entries)
                    logger.info(f"{self._log_name} AppliedEntries={entries}")
                    self.last_applied = self.commit_index

        logger.info(
            f"{self._log_name} Message originating from {event.msg.source} to {event.msg.dest}"
        )
        msg: rpc.RPCMessage = rpc.AppendEntriesResponse(  # type: ignore
            term=self.current_term,
            match_index=event.msg.prev_log_index + len(event.msg.entries)
            if success
            else -1,
            source_node_id=self.node_id,
            success=success,
            dest=event.msg.source,
            source=event.msg.dest,
        )

        logger.info(
            f"{self._log_name} Append entries request was successful: {success}"
        )

        return ResponsesEvents([msg], [Event(EventType.ResetElectionTimeout, None)])

    def handle_request_vote_rpc(self, event: Event) -> ResponsesEvents:
        """
        Follower can only vote for a Candidate in the following scenarios:
        - The Follower log has an amount of information <= Candidate's log
        - The Candidate's Term is >= Follower's term
        """
        logger.info(
            (
                f"{self._log_name} Received request for votes from "
                f"{event.msg.source} with ID {event.msg.candidate_id}"
            )
        )
        logger.debug(f"{self._log_name} RequestVoteRpc={repr(event.msg)}")

        # See Raft §5.4.1:
        # "If the logs have last entries with different terms, then the log with the later
        # term is more up-to-date. If the logs end with the same term, then whichever log is
        # longer is more up-to-date"
        grant_vote = self.voted_for is None
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index].term if last_log_index >= 0 else -1
        grant_vote = all(
            [
                self.voted_for is None,
                last_log_term <= event.msg.last_log_term,
                last_log_index <= event.msg.last_log_index,
            ]
        )
        # grant_vote and last_log_term <= event.msg.last_log_term
        # grant_vote = grant_vote and last_log_index <= event.msg.last_log_index
        msg: rpc.RPCMessage = rpc.RequestVoteResponse(  # type: ignore
            term=self.current_term,
            source_node_id=self.node_id,
            vote_granted=grant_vote,
            dest=event.msg.source,
            source=event.msg.dest,
        )
        self.voted_for = event.msg.candidate_id if grant_vote else self.voted_for
        logger.info(
            f"{self._log_name} vote granted to {event.msg.candidate_id}: {grant_vote}"
        )
        # We should _not_ trigger an election in this case otherwise we're doing so unecessarily
        # _If_ we need an election, then we should pick it up next time around.
        further_events = []
        if grant_vote:
            further_events = [Event(EventType.ResetElectionTimeout, None)]
        return ResponsesEvents([msg], further_events)

    def handle_event(self, event: Event) -> Tuple[Server, ResponsesEvents]:
        event_term = None
        if event.msg and hasattr(event.msg, "term"):
            event_term = event.msg.term
        if event_term and event_term < self.current_term:
            return self, empty_response()
        if event_term and event_term > self.current_term:
            self.current_term = event_term

        responses_events = empty_response()
        if event.type == EventType.ElectionTimeoutStartElection:
            logger.info(f"{self._log_name} is calling an election")
            new_instance = self.convert(Candidate).increment_term()
            responses_events = new_instance.handle_start_election(event)
            return new_instance, responses_events
        if event.type == EventType.LeaderAppendLogEntryRpc:
            responses_events = self.handle_append_entries_message(event)
        elif event.type == EventType.CandidateRequestVoteRpc:
            responses_events = self.handle_request_vote_rpc(event)
        return self, responses_events

    def validate_conversion(self, target_class):
        if target_class == Candidate:
            return True
        raise ValueError(f"Cannot convert follower into {target_class}")


# # # # # # # # # # # # # # # # # #
#
# Leader
#
# # # # # # # # # # # # # # # # # #


class Leader(BaseServer, Generic[S]):
    """
    A Leader can become a Follower.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # volatile (from the raft paper)
        self.next_index = {k: self.last_applied + 1 for k in self.all_node_ids}
        self.match_index = {k: 0 for k in self.all_node_ids}
        # implementation specific
        self.consensus_threshold = (len(self.all_node_ids) // 2) + 1
        self._log_name = self.log_name()

    @classmethod
    def log_name(cls):
        if loggers.RICH_HANDLING_ON:
            return "[bold red]Leader[/]"
        return "Leader"

    def handle_client_append_request(self, event: Event):
        entry = log.LogEntry(self.current_term, event.msg.command)
        prev_index = len(self.log) - 1
        prev_term = self.log[prev_index].term if prev_index >= 0 else -1
        # Should always succeed on leader.
        self.log.append_entries(
            prev_index=prev_index, prev_term=prev_term, entries=[entry]
        )
        return empty_response()

    def handle_append_entries_response(self, event: Event) -> ResponsesEvents:
        """
        "success" needs to be translated into consensus.
        Log entries have to replicated on 3 machines (including the leader).
        The log entries have to be "committed" and "applied".
        Once that happens, then its ok to reply back to the client.
        """
        node_id = event.msg.source_node_id
        if event.msg.success:
            logger.info(
                f"{self._log_name} Append entries request was successful for node: {node_id}"
            )
            self.match_index[node_id] = max(
                event.msg.match_index, self.match_index[node_id]
            )
            self.next_index[node_id] = event.msg.match_index + 1

            # determine number of committed entries
            matched = sorted(self.match_index.values())
            num_committed = matched[len(matched) // 2]  # median is the answer!
            if num_committed > self.commit_index:
                self.commit_index = num_committed
                logger.info(
                    f"{self._log_name} Committed entries count is now {self.commit_index}"
                )
                # Here is where committed log entries would be "applied" to the application.
                if self.commit_index > self.last_applied:
                    entries = self.log.log[
                        self.last_applied + 1 : self.commit_index + 1
                    ]
                    self.applied.extend(entries)
                    logger.info(f"{self._log_name} AppliedEntries={entries}")
                    self.last_applied = self.commit_index
        else:
            logger.warning(
                f"{self._log_name} Append entries Failed for node: {node_id}"
            )
            self.next_index[node_id] = self.next_index[node_id] - 1
        return empty_response()

    def get_log_entries_for_node(self, node_id: int):
        expected = self.match_index[node_id]
        prev_log_idx = expected - 1
        prev_term = -1
        entries: List[log.LogEntry] = []
        if self.log:
            prev_term = self.log[prev_log_idx].term
            entries = self.log.log[expected:]
        return prev_log_idx, prev_term, entries

    def construct_append_entry_rpcs(self) -> List[transport.Request]:
        all_msgs: List[transport.Request] = []

        for node_id in self.all_node_ids:
            prev_log_idx, prev_term, entries = self.get_log_entries_for_node(node_id)
            addr = self.config.node_mapping[node_id]["addr"]
            msg: rpc.RPCMessage = rpc.AppendEntriesRpc(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=prev_log_idx,
                prev_log_term=prev_term,
                entries=entries,
                leader_commit_index=self.commit_index,
                dest=addr,
                source=self.address,
            )
            all_msgs.append(msg)  # type: ignore
        return all_msgs

    def handle_heartbeat_send(self, _: Event) -> ResponsesEvents:
        all_msgs = self.construct_append_entry_rpcs()
        return ResponsesEvents(all_msgs, [])

    def handle_event(self, event: Event) -> Tuple[Server, ResponsesEvents]:
        responses = empty_response()
        event_msg_type = event.msg.type if event.msg else "none"
        event_term = event.msg.term if event.msg and hasattr(event.msg, "term") else -1
        logger.info(
            (
                f"{self._log_name} Received Event with msg type "
                f"{event_msg_type} and term {event_term}"
            )
        )
        if event_term > self.current_term:
            # According to the paper, the server immediately steps down in this case
            logger.warning(
                (
                    f"{self._log_name} with term *{self.current_term}* is stepping down "
                    f"after message with term *{event_term}* received"
                )
            )
            self.current_term = event_term
            return (
                self.convert(Follower),
                ResponsesEvents([], [EVENT_CONVERSION_TO_FOLLOWER]),
            )

        if event.type == EventType.AppendEntryConfirm:
            responses = self.handle_append_entries_response(event)
        elif event.type == EventType.HeartbeatTime:
            responses = self.handle_heartbeat_send(event)
        elif event.type == EventType.ClientAppendRequest:
            responses = self.handle_client_append_request(event)
        return self, responses

    def validate_conversion(self, target_class):
        if target_class == Follower:
            return True
        raise ValueError(f"{self._log_name} Can only convert Leader into a Follower")


Server = Union[Leader[S], Candidate[S], Follower[S]]
