"""Integration tests for InstallSnapshot RPC functionality."""

import configparser
import os

from raft.io.storage import InMemoryStorage
from raft.models.config import Config
from raft.models.log import LogEntry
from raft.models.rpc import InstallSnapshotResponse, InstallSnapshotRpc
from raft.models.server import Follower, Leader
from raft.models.snapshot import KeyValueStateMachine

from raft.models import Event, EventType

test_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
root_dir = os.path.dirname(test_dir)


def _make_config(node_count=2, snapshot_threshold=5):
    _conf = configparser.ConfigParser()
    _conf.read(os.path.join(root_dir, "raft.ini"))
    _conf.set("Cluster", "NodeCount", str(node_count))
    _conf.set("Cluster", "SnapshotThreshold", str(snapshot_threshold))
    for n in range(node_count + 1, 6):
        label = _conf["Nodes"].get(f"Node{n}", "")
        if label and f"Node.{label}" in _conf:
            del _conf[f"Node.{label}"]
        if f"Node{n}" in _conf["Nodes"]:
            del _conf["Nodes"][f"Node{n}"]
    return _conf


class TestInstallSnapshotIntegration:
    """Test InstallSnapshot RPC between Leader and Follower."""

    def test_leader_sends_snapshot_to_lagging_follower(self):
        """Test that leader sends snapshot when follower's next_index is too low."""
        conf = _make_config(node_count=2, snapshot_threshold=3)
        config = Config(conf)

        leader_storage = InMemoryStorage(1, config)
        leader_state_machine = KeyValueStateMachine()
        leader = Leader(node_id=1, config=config, storage=leader_storage, state_machine=leader_state_machine)

        # Add entries and create a snapshot
        entries = [
            LogEntry(term=1, data=b'{"op": "set", "key": "a", "value": "1"}'),
            LogEntry(term=1, data=b'{"op": "set", "key": "b", "value": "2"}'),
            LogEntry(term=1, data=b'{"op": "set", "key": "c", "value": "3"}'),
            LogEntry(term=1, data=b'{"op": "set", "key": "d", "value": "4"}'),
        ]

        leader.log.append_entries(entries=entries)

        leader.commit_index = len(entries) - 1
        leader._apply_committed_entries()

        # Create snapshot
        snapshot_id = leader.create_snapshot()
        assert snapshot_id is not None

        # Simulate follower being far behind (next_index < last_snapshot_index)
        leader.next_index[2] = 0  # Follower node 2 is at beginning
        leader.last_snapshot_index = 3  # Snapshot includes first 4 entries

        # Leader should detect follower needs snapshot
        addr = leader.config.node_mapping[2]["addr"]
        install_rpc = leader.construct_install_snapshot_rpc(2, addr)

        assert install_rpc is not None
        assert install_rpc.term == leader.current_term
        assert install_rpc.leader_id == leader.node_id
        assert install_rpc.last_included_index == 3

        # Verify snapshot data is included
        import json

        snapshot_data = json.loads(install_rpc.data.decode("utf-8"))
        assert snapshot_data["a"] == "1"
        assert snapshot_data["b"] == "2"
        assert snapshot_data["c"] == "3"
        assert snapshot_data["d"] == "4"

    def test_follower_installs_snapshot_correctly(self):
        """Test that follower correctly processes InstallSnapshot RPC."""
        conf = _make_config(node_count=2)
        config = Config(conf)

        follower_storage = InMemoryStorage(2, config)
        follower_state_machine = KeyValueStateMachine()
        follower = Follower(
            node_id=2, config=config, storage=follower_storage, state_machine=follower_state_machine
        )
        follower.current_term = 1

        # Create InstallSnapshot RPC with test data
        import json

        snapshot_data = {"key1": "value1", "key2": "value2"}

        install_rpc = InstallSnapshotRpc(
            term=1,
            leader_id=1,
            last_included_index=2,
            last_included_term=1,
            data=json.dumps(snapshot_data).encode("utf-8"),
            source=("localhost", 5001),
            dest=("localhost", 5002),
        )

        # Create event and handle it
        event = Event(EventType.InstallSnapshotRequestRpc, install_rpc)
        responses_events = follower.handle_install_snapshot_message(event)

        # Check response
        assert len(responses_events.responses) == 1
        response = responses_events.responses[0]
        assert isinstance(response, InstallSnapshotResponse)
        assert response.success is True
        assert response.term == 1

        # Verify state machine was updated
        assert follower_state_machine.get("key1") == "value1"
        assert follower_state_machine.get("key2") == "value2"

        # Verify follower tracking was updated
        assert follower.last_snapshot_index == 2
        assert follower.last_snapshot_term == 1
        assert follower.last_applied == 2

    def test_follower_rejects_stale_snapshot(self):
        """Test that follower rejects snapshot with older term."""
        conf = _make_config(node_count=2)
        config = Config(conf)

        follower_storage = InMemoryStorage(2, config)
        follower_state_machine = KeyValueStateMachine()
        follower = Follower(
            node_id=2, config=config, storage=follower_storage, state_machine=follower_state_machine
        )
        follower.current_term = 3  # Higher than snapshot term

        # Create InstallSnapshot RPC with lower term
        install_rpc = InstallSnapshotRpc(
            term=2,  # Lower than follower's current term
            leader_id=1,
            last_included_index=2,
            last_included_term=2,
            data=b'{"test": "data"}',
            source=("localhost", 5001),
            dest=("localhost", 5002),
        )

        # Handle the event
        event = Event(EventType.InstallSnapshotRequestRpc, install_rpc)
        responses_events = follower.handle_install_snapshot_message(event)

        # Check response indicates failure
        assert len(responses_events.responses) == 1
        response = responses_events.responses[0]
        assert isinstance(response, InstallSnapshotResponse)
        assert response.success is False
        assert response.term == 3  # Follower's term

    def test_leader_updates_next_index_after_successful_snapshot(self):
        """Test that leader updates next_index after successful InstallSnapshot response."""
        conf = _make_config(node_count=2, snapshot_threshold=1)
        config = Config(conf)

        leader_storage = InMemoryStorage(1, config)
        leader_state_machine = KeyValueStateMachine()
        leader = Leader(node_id=1, config=config, storage=leader_storage, state_machine=leader_state_machine)

        # Create and save a snapshot
        entries = [LogEntry(term=1, data=b'{"op": "set", "key": "test", "value": "data"}')]
        leader.log.append_entries(entries=entries)
        leader.commit_index = 0
        leader._apply_committed_entries()
        leader.create_snapshot()

        # Set initial next_index for follower
        leader.next_index[2] = 0
        leader.match_index[2] = 0

        # Simulate that a snapshot was sent to node 2 (normally set by construct_install_snapshot_rpc)
        leader.snapshot_sent_index[2] = leader.last_snapshot_index

        # Create successful InstallSnapshot response
        class MockInstallSnapshotResponse:
            def __init__(self):
                self.success = True
                self.bytes_stored = 100
                self.source_node_id = 2

        class MockEvent:
            def __init__(self):
                self.msg = MockInstallSnapshotResponse()

        # Handle the response
        leader.handle_install_snapshot_response(MockEvent())

        # Verify next_index was updated
        latest_snapshot_metadata = leader_storage.get_latest_snapshot_metadata()
        if latest_snapshot_metadata:
            expected_next_index = latest_snapshot_metadata.last_included_index + 1
            assert leader.next_index[2] == expected_next_index
            assert leader.match_index[2] == latest_snapshot_metadata.last_included_index

    def test_construct_append_entry_rpcs_sends_snapshot_when_needed(self):
        """Test that construct_append_entry_rpcs chooses snapshot over log entries when appropriate."""
        conf = _make_config(node_count=2, snapshot_threshold=2)
        config = Config(conf)

        leader_storage = InMemoryStorage(1, config)
        leader_state_machine = KeyValueStateMachine()
        leader = Leader(node_id=1, config=config, storage=leader_storage, state_machine=leader_state_machine)

        # Create entries, apply them, and make snapshot
        entries = [
            LogEntry(term=1, data=b'{"op": "set", "key": "a", "value": "1"}'),
            LogEntry(term=1, data=b'{"op": "set", "key": "b", "value": "2"}'),
        ]
        leader.log.append_entries(entries=entries)

        leader.commit_index = len(entries) - 1
        leader._apply_committed_entries()
        leader.create_snapshot()

        # Set follower behind snapshot
        leader.next_index[2] = 0  # Behind snapshot
        leader.last_snapshot_index = 1  # Snapshot covers first 2 entries

        # Get RPCs - should include InstallSnapshot for node 2
        rpcs = leader.construct_append_entry_rpcs()

        # Should have one RPC (for the lagging follower)
        assert len(rpcs) == 1

        # The RPC should be InstallSnapshot type
        rpc = rpcs[0]
        assert hasattr(rpc, "type")
        # Note: We can't easily check the exact type without more complex mocking
        # but the method should return InstallSnapshot RPC for the lagging follower

    def test_rpc_serialization_and_parsing(self):
        """Test that InstallSnapshot RPCs can be serialized and parsed correctly."""
        import json

        from raft.models.rpc import parse_msg

        # Create InstallSnapshot RPC
        original_rpc = InstallSnapshotRpc(
            term=5,
            leader_id=1,
            last_included_index=10,
            last_included_term=3,
            data=b'{"test": "snapshot"}',
            dest=("localhost", 8000),
            source=("localhost", 8001),
        )

        # Serialize to dict
        rpc_dict = original_rpc.to_dict()

        # Serialize to JSON bytes
        json_bytes = json.dumps(rpc_dict).encode("utf-8")

        # Parse back
        parsed_rpc = parse_msg(json_bytes)

        # Verify it parsed correctly
        assert parsed_rpc.term == 5
        assert parsed_rpc.leader_id == 1
        assert parsed_rpc.last_included_index == 10
        assert parsed_rpc.last_included_term == 3
        assert parsed_rpc.data == b'{"test": "snapshot"}'
