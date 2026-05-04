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

import inspect
import json
import logging
from collections import namedtuple
from typing import Any, Generic, TypeAlias, TypeVar

from raft.io import transport
from raft.models import (
    Event,
    EVENT_CONVERSION_TO_FOLLOWER,
    EVENT_CONVERSION_TO_LEADER,
    EVENT_HEARTBEAT,
    EVENT_SELF_WON_ELECTION,
    EVENT_START_HEARTBEAT,
    EventType,
    log,
    rpc,
)
from raft.models.config import Config
from raft.models.snapshot import KeyValueStateMachine, Snapshot, StateMachine

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
    def __init__(self, node_id: int, config: Config, storage, state_machine: StateMachine = None):
        # must be persisted to storage (from raft paper)
        self.current_term = 1
        self.voted_for = None
        self.log: log.Log = log.Log()
        # volatile (from raft paper)
        self.commit_index = -1
        self.last_applied = -1
        # this is implementation specific
        self.applied: list[log.LogEntry] = []
        self.storage = storage
        self.config = config
        self.node_id = node_id
        self.all_node_ids = list(filter(lambda el: el != self.node_id, self.config.node_mapping.keys()))
        self.quorom: int = (len(self.all_node_ids) // 2) + 1
        this_node = self.config.node_mapping[self.node_id]
        self.label = this_node["label"]
        self.host: str = this_node["addr"][0]
        self.port: int = this_node["addr"][1]

        # Snapshot-related attributes
        self.state_machine = state_machine or KeyValueStateMachine()
        self.last_snapshot_index = -1
        self.last_snapshot_term = -1
        self.snapshot_threshold = getattr(config, "snapshot_threshold", 1000)  # Default threshold

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
            "state_machine",
            "last_snapshot_index",
            "last_snapshot_term",
            "snapshot_threshold",
        )

        # Recovery: restore from latest snapshot if available
        self._restore_from_snapshot_if_exists()

    @classmethod
    def log_name(cls):
        return "Server"

    @property
    def address(self):
        return (self.host, self.port)

    def save_meta(self):
        self.storage.save_metadata(
            json.dumps(
                {
                    "votedFor": self.voted_for,
                    "currentTerm": self.current_term,
                    "lastSnapshotIndex": self.last_snapshot_index,
                    "lastSnapshotTerm": self.last_snapshot_term,
                }
            ).encode()
        )

    def save_log_entry(self):
        self.storage.save(self.log[-1])

    def convert(self, target_class) -> S:
        logger.warning(f"Converting from {self._log_name} to {target_class.log_name}")
        self.validate_conversion(target_class)
        new_server = target_class(self.node_id, self.config, self.storage, self.state_machine)
        for attr in new_server.transfer_attrs:
            setattr(new_server, attr, getattr(self, attr))
        return new_server

    def validate_conversion(self, target_class):  # noqa
        return target_class in {Candidate, Follower, Leader}

    def _restore_from_snapshot_if_exists(self):
        """Restore state from latest snapshot during startup"""
        try:
            metadata_result = self.storage.get_latest_snapshot_metadata()
            if inspect.iscoroutine(metadata_result):
                # Async storage can't be awaited in a sync __init__; skip restoration.
                metadata_result.close()
                return
            metadata = metadata_result
            if metadata:
                snapshot = self.storage.load_snapshot(metadata.snapshot_id)

                # Verify snapshot integrity
                if not snapshot.verify_integrity():
                    logger.warning(f"Snapshot {metadata.snapshot_id} failed integrity check, skipping")
                    return

                # Restore state machine
                self.state_machine.restore_from_snapshot(snapshot.state_machine_data)
                self.last_snapshot_index = snapshot.last_included_index
                self.last_snapshot_term = snapshot.last_included_term

                # Update commit and applied indices
                if snapshot.last_included_index > self.commit_index:
                    self.commit_index = snapshot.last_included_index
                if snapshot.last_included_index > self.last_applied:
                    self.last_applied = snapshot.last_included_index

                # Do NOT compact the log here. Compacting during recovery would shift
                # the in-memory log to 0-based while last_snapshot_index is I, causing
                # AppendEntries with prev_log_index=I to miss the sentinel entry and
                # trigger an infinite reject→decrement→snapshot cycle. Compaction is
                # deferred to the next snapshot creation when the log offset is updated.

                logger.info(
                    f"Restored from snapshot {metadata.snapshot_id} "
                    f"(index={snapshot.last_included_index}, term={snapshot.last_included_term})"
                )
        except Exception as e:
            logger.error(f"Failed to restore from snapshot: {e}")
            # Continue without snapshot - this is non-fatal

    def should_create_snapshot(self) -> bool:
        """Determine if it's time to create a snapshot"""
        entries_since_snapshot = len(self.log) - self.last_snapshot_index - 1
        return entries_since_snapshot >= self.snapshot_threshold

    def create_snapshot(self) -> str | None:
        """Create a new snapshot of the current state"""
        if not self.should_create_snapshot():
            logger.debug("Snapshot not needed yet")
            return None

        try:
            # Apply any pending committed entries to state machine
            self._apply_committed_entries()

            # Create snapshot
            snapshot_data = self.state_machine.create_snapshot()
            snapshot = Snapshot.create(
                last_included_index=self.last_applied,
                last_included_term=self.log[self.last_applied].term if self.last_applied >= 0 else -1,
                state_machine_data=snapshot_data,
                configuration={"nodes": self.config.node_mapping},
            )

            # Save snapshot
            snapshot_id = self.storage.save_snapshot(snapshot)

            # Update tracking variables and persist atomically with term/votedFor
            self.last_snapshot_index = snapshot.last_included_index
            self.last_snapshot_term = snapshot.last_included_term
            self.save_meta()

            # Compact log (remove entries now in snapshot)
            self.storage.compact_log(snapshot.last_included_index)

            # Clean up old snapshots
            self.storage.delete_old_snapshots(keep_count=3)

            logger.info(f"Created snapshot {snapshot_id} up to index {snapshot.last_included_index}")
            return snapshot_id

        except Exception as e:
            logger.error(f"Failed to create snapshot: {e}")
            return None

    def _apply_committed_entries(self):
        """Apply any committed but not yet applied log entries to the state machine"""
        if self.commit_index > self.last_applied:
            # Apply entries from last_applied+1 to commit_index
            start_index = max(0, self.last_applied + 1)
            end_index = min(len(self.log) - 1, self.commit_index)

            for i in range(start_index, end_index + 1):
                if i < len(self.log.log):
                    entry = self.log.log[i]
                    try:
                        result = self.state_machine.apply_entry(entry)
                        logger.debug(f"Applied entry {i}: {result}")
                    except Exception as e:
                        logger.error(f"Failed to apply entry {i}: {e}")

            self.last_applied = end_index
            logger.info(f"Applied entries up to index {self.last_applied}")

    def get_applied_entry_result(self, entry: log.LogEntry) -> Any:
        """Apply an entry to the state machine and return the result"""
        try:
            return self.state_machine.apply_entry(entry)
        except Exception as e:
            logger.error(f"Failed to apply entry {entry}: {e}")
            return {"success": False, "error": str(e)}


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
        self.votes_received: set[int] = set((self.node_id,))
        self._log_name = self.log_name

    @classmethod
    def log_name(cls):
        return "[bold yellow]Candidate[/]"

    def increment_term(self):
        self.current_term += 1
        return self

    def construct_request_vote_rpcs(self) -> list[transport.Request]:
        all_msgs: list[transport.Request] = []

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

    def handle_event(self, event: Event) -> tuple[Server, ResponsesEvents]:
        event_term = -2
        if event.msg and hasattr(event.msg, "term"):
            event_term = event.msg.term
        responses_events = empty_response()
        if event.type == EventType.LeaderAppendLogEntryRpc or event_term > self.current_term:
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
                ResponsesEvents([], [EVENT_CONVERSION_TO_LEADER, EVENT_START_HEARTBEAT]),
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
        self._log_name = self.log_name

    @classmethod
    def log_name(cls):
        return "[bold green]Follower[/]"

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
        # voted_for is only cleared when term changes (Raft §5.2)
        # It is NOT cleared on every successful AppendEntries
        if success:
            # We have a leader if we have received this event
            self.known_leader_node_id = event.msg.leader_id  # type: ignore
            # LEader sends commit index: try to advance ours
            new_commit_index = min(event.msg.leader_commit_index, len(self.log))
            if new_commit_index > self.commit_index:
                self.commit_index = new_commit_index
                logger.info(f"{self._log_name} Committed entries count is now {self.commit_index}")

                if self.commit_index > self.last_applied:
                    entries = self.log.log[self.last_applied + 1 : self.commit_index + 1]
                    self.applied.extend(entries)
                    logger.info(f"{self._log_name} AppliedEntries={entries}")
                    self.last_applied = self.commit_index

        msg: rpc.RPCMessage = rpc.AppendEntriesResponse(  # type: ignore
            term=self.current_term,
            match_index=event.msg.prev_log_index + len(event.msg.entries) if success else -1,
            source_node_id=self.node_id,
            success=success,
            dest=event.msg.source,
            source=event.msg.dest,
        )

        logger.info(
            f"{self._log_name} Append entries request from {event.msg.source} was successful: {success}"
        )

        return ResponsesEvents([msg], [Event(EventType.ResetElectionTimeout, None)])

    def handle_request_vote_rpc(self, event: Event) -> ResponsesEvents:
        """
        Follower can only vote for a Candidate in the following scenarios:
        - The Follower log has an amount of information <= Candidate's log
        - The Candidate's Term is >= Follower's term
        """
        logger.info(
            f"{self._log_name} Received request for votes from "
            f"{event.msg.source} with ID {event.msg.candidate_id}"
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
        logger.info(f"{self._log_name} vote granted to {event.msg.candidate_id}: {grant_vote}")
        # We should _not_ trigger an election in this case otherwise we're doing so unecessarily
        # _If_ we need an election, then we should pick it up next time around.
        further_events = []
        if grant_vote:
            further_events = [Event(EventType.ResetElectionTimeout, None)]
        return ResponsesEvents([msg], further_events)

    def handle_event(self, event: Event) -> tuple[Server, ResponsesEvents]:
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
        elif event.type == EventType.InstallSnapshotRequestRpc:
            responses_events = self.handle_install_snapshot_message(event)
        elif event.type == EventType.CandidateRequestVoteRpc:
            responses_events = self.handle_request_vote_rpc(event)
        return self, responses_events

    def handle_install_snapshot_message(self, event: Event) -> ResponsesEvents:
        """Handle InstallSnapshot RPC from leader."""
        logger.info(f"{self._log_name} Received InstallSnapshot RPC from leader {event.msg.leader_id}")

        # Reply false if term < currentTerm (§5.1)
        if event.msg.term < self.current_term:
            logger.warning(
                f"{self._log_name} InstallSnapshot term {event.msg.term} < current term {self.current_term}"
            )
            response = rpc.InstallSnapshotResponse(
                term=self.current_term,
                success=False,
                dest=event.msg.source,
                source=self.address,
            )
            return ResponsesEvents([response], [])

        # Advance term BEFORE any state changes (Raft §5.1)
        if event.msg.term > self.current_term:
            self.current_term = event.msg.term
            self.voted_for = None
            self.storage.save_metadata(
                json.dumps({"votedFor": self.voted_for, "currentTerm": self.current_term}).encode()
            )

        self.known_leader_node_id = event.msg.leader_id

        try:
            # restore_from_snapshot must be first: if it raises, no tracking state is mutated
            # and the leader's retry will find the follower in a clean state (idempotency).
            self.state_machine.restore_from_snapshot(event.msg.data)

            # All mutations below are all-or-nothing after a successful restore.
            self.last_snapshot_index = event.msg.last_included_index
            self.last_snapshot_term = event.msg.last_included_term

            if len(self.log.log) > event.msg.last_included_index:
                self.log.log = self.log.log[event.msg.last_included_index + 1 :]
            else:
                self.log.log = []

            self.last_applied = max(self.last_applied, event.msg.last_included_index)

            last_index = event.msg.last_included_index
            logger.info(f"{self._log_name} Successfully installed snapshot up to index {last_index}")

            response = rpc.InstallSnapshotResponse(
                term=self.current_term,
                success=True,
                bytes_stored=len(event.msg.data),
                dest=event.msg.source,
                source=self.address,
            )

        except Exception as e:
            logger.error(f"{self._log_name} Failed to install snapshot: {e}")
            response = rpc.InstallSnapshotResponse(
                term=self.current_term,
                success=False,
                dest=event.msg.source,
                source=self.address,
            )

        return ResponsesEvents([response], [])

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
        # Tracks last_included_index of the snapshot sent to each node so that
        # handle_install_snapshot_response uses the sent index, not the current one.
        # Not in transfer_attrs — in-flight snapshot tracking is scoped to this
        # leader's term; a new leader after step-down starts fresh.
        self.snapshot_sent_index: dict[int, int] = {}
        # implementation specific
        self.consensus_threshold = (len(self.all_node_ids) // 2) + 1
        self._log_name = self.log_name

    @classmethod
    def log_name(cls):
        return "[bold red]Leader[/]"

    def handle_client_append_request(self, event: Event):
        entry = log.LogEntry(self.current_term, event.msg.command)
        prev_index = len(self.log) - 1
        prev_term = self.log[prev_index].term if prev_index >= 0 else -1
        # Should always succeed on leader.
        self.log.append_entries(prev_index=prev_index, prev_term=prev_term, entries=[entry])
        return empty_response()

    def handle_append_entries_response(self, event: Event) -> ResponsesEvents:
        """
        "success" needs to be translated into consensus.
        Log entries have to replicated on 3 machines (including the leader).
        The log entries have to be "committed" and "applied".
        Once that happens, then it is ok to reply back to the client.
        """
        node_id = event.msg.source_node_id
        if event.msg.success:
            logger.info(f"{self._log_name} Append entries request was successful for node: {node_id}")
            self.match_index[node_id] = max(event.msg.match_index, self.match_index[node_id])
            self.next_index[node_id] = event.msg.match_index + 1

            # determine number of committed entries
            matched = sorted(self.match_index.values())
            num_committed = matched[len(matched) // 2]  # median is the answer!
            if num_committed > self.commit_index:
                self.commit_index = num_committed
                logger.info(f"{self._log_name} Committed entries count is now {self.commit_index}")
                # Apply committed log entries to the state machine
                if self.commit_index > self.last_applied:
                    self._apply_committed_entries()

                    # Check if we should create a snapshot
                    if self.should_create_snapshot():
                        snapshot_id = self.create_snapshot()
                        if snapshot_id:
                            logger.info(f"{self._log_name} Created snapshot {snapshot_id}")

                    # Keep the old behavior for backward compatibility
                    entries = self.log.log[
                        max(0, self.last_applied - len(self.applied) + 1) : self.commit_index + 1
                    ]
                    self.applied.extend(entries[-len(entries) :])
                    logger.info(f"{self._log_name} AppliedEntries={entries}")
        else:
            logger.warning(f"{self._log_name} Append entries Failed for node: {node_id}")
            self.next_index[node_id] = self.next_index[node_id] - 1
        return empty_response()

    def handle_install_snapshot_response(self, event: Event) -> ResponsesEvents:
        """Handle response to InstallSnapshot RPC."""
        node_id = event.msg.source_node_id if hasattr(event.msg, "source_node_id") else -1

        # Per Raft §5.1: if response has higher term, step down
        if hasattr(event.msg, "term") and event.msg.term > self.current_term:
            logger.info(
                f"{self._log_name} InstallSnapshot response has higher term {event.msg.term}, "
                f"stepping down from leader to follower"
            )
            self.current_term = event.msg.term
            self.voted_for = None
            self.storage.save_metadata(
                json.dumps({"votedFor": self.voted_for, "currentTerm": self.current_term}).encode()
            )
            return empty_response()

        if not hasattr(event.msg, "success"):
            logger.warning(f"{self._log_name} Invalid InstallSnapshot response from node {node_id}")
            return empty_response()

        if event.msg.success:
            # Use the index we recorded at send time, not the current latest snapshot.
            # A new snapshot may have been created since the RPC was sent; using the
            # current latest would incorrectly advance match_index past what the
            # follower actually installed.
            sent_index = self.snapshot_sent_index.pop(node_id, None)
            if sent_index is not None:
                self.next_index[node_id] = sent_index + 1
                self.match_index[node_id] = sent_index
                logger.info(
                    f"{self._log_name} Node {node_id} successfully installed snapshot, "
                    f"next_index={self.next_index[node_id]}"
                )
            else:
                logger.warning(f"{self._log_name} No in-flight snapshot index recorded for node {node_id}")
        else:
            logger.warning(f"{self._log_name} InstallSnapshot failed for node {node_id}")
            # Could implement retry logic here

        return empty_response()

    def get_log_entries_for_node(self, node_id: int):
        expected = self.match_index[node_id]
        prev_log_idx = expected - 1
        prev_term = -1
        entries: list[log.LogEntry] = []
        if self.log:
            prev_term = self.log[prev_log_idx].term
            entries = self.log.log[expected:]
        return prev_log_idx, prev_term, entries

    def construct_append_entry_rpcs(self) -> list[transport.Request]:
        all_msgs: list[transport.Request] = []

        for node_id in self.all_node_ids:
            addr = self.config.node_mapping[node_id]["addr"]

            # Check if follower needs a snapshot
            if self.next_index[node_id] <= self.last_snapshot_index:
                # Follower is too far behind, send snapshot instead
                snapshot_msg = self.construct_install_snapshot_rpc(node_id, addr)
                if snapshot_msg:
                    all_msgs.append(snapshot_msg)  # type: ignore
            else:
                # Normal case: send log entries
                prev_log_idx, prev_term, entries = self.get_log_entries_for_node(node_id)
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

    def construct_install_snapshot_rpc(self, node_id: int, addr: tuple) -> rpc.InstallSnapshotRpc | None:
        """Construct InstallSnapshot RPC for a follower that needs a snapshot."""
        # Get the latest snapshot
        latest_snapshot_metadata = self.storage.get_latest_snapshot_metadata()
        if not latest_snapshot_metadata:
            logger.warning(f"{self._log_name} No snapshot available for node {node_id}")
            return None

        # Load the snapshot data
        snapshot = self.storage.load_snapshot(latest_snapshot_metadata.snapshot_id)
        if not snapshot:
            logger.warning(f"{self._log_name} Could not load snapshot {latest_snapshot_metadata.snapshot_id}")
            return None

        # For now, send the entire snapshot in one message (no chunking)
        # In production, you'd want to chunk large snapshots
        snapshot_data = snapshot.state_machine_data

        # Record the index we're sending so the response handler uses the correct value
        # even if a newer snapshot is created before the response arrives.
        self.snapshot_sent_index[node_id] = snapshot.last_included_index

        logger.info(
            f"{self._log_name} Sending snapshot to node {node_id} "
            f"(last_included_index={snapshot.last_included_index})"
        )

        return rpc.InstallSnapshotRpc(
            term=self.current_term,
            leader_id=self.node_id,
            last_included_index=snapshot.last_included_index,
            last_included_term=snapshot.last_included_term,
            data=snapshot_data,
            dest=addr,
            source=self.address,
        )

    def handle_heartbeat_send(self, _: Event) -> ResponsesEvents:
        all_msgs = self.construct_append_entry_rpcs()
        return ResponsesEvents(all_msgs, [])

    def handle_event(self, event: Event) -> tuple[Server, ResponsesEvents]:
        responses = empty_response()
        event_msg_type = event.msg.type if event.msg else "none"
        event_term = event.msg.term if event.msg and hasattr(event.msg, "term") else -1
        logger.info(f"{self._log_name} Received Event with msg type {event_msg_type} and term {event_term}")
        if event_term > self.current_term:
            # According to the paper, the server immediately steps down in this case
            logger.warning(
                f"{self._log_name} with term *{self.current_term}* is stepping down "
                f"after message with term *{event_term}* received"
            )
            self.current_term = event_term
            return (
                self.convert(Follower),
                ResponsesEvents([], [EVENT_CONVERSION_TO_FOLLOWER]),
            )

        if event.type == EventType.AppendEntryConfirm:
            responses = self.handle_append_entries_response(event)
        elif event.type == EventType.InstallSnapshotConfirm:
            responses = self.handle_install_snapshot_response(event)
        elif event.type == EventType.HeartbeatTime:
            responses = self.handle_heartbeat_send(event)
        elif event.type == EventType.ClientAppendRequest:
            responses = self.handle_client_append_request(event)
        return self, responses

    def validate_conversion(self, target_class):
        if target_class == Follower:
            return True
        raise ValueError(f"{self._log_name} Can only convert Leader into a Follower")


Server: TypeAlias = Leader[S] | Candidate[S] | Follower[S]
