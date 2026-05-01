"""Tests for server snapshot integration."""

import configparser
import json
import os

from raft.io.storage import InMemoryStorage
from raft.models.config import Config
from raft.models.log import LogEntry
from raft.models.server import Follower, Leader
from raft.models.snapshot import KeyValueStateMachine

test_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
root_dir = os.path.dirname(test_dir)


def _make_config(node_count=3, snapshot_threshold=1000):
    """Create a Config object for testing."""
    _conf = configparser.ConfigParser()
    _conf.read(os.path.join(root_dir, "raft.ini"))
    _conf.set("Cluster", "NodeCount", str(node_count))
    _conf.set("Cluster", "SnapshotThreshold", str(snapshot_threshold))
    # Remove extra node sections to avoid errors
    for n in range(node_count + 1, 6):
        if f"Node.{_conf['Nodes'].get(f'Node{n}', '')}" in _conf:
            del _conf[f"Node.{_conf['Nodes'].get(f'Node{n}', '')}"]
        if f"Node{n}" in _conf["Nodes"]:
            del _conf["Nodes"][f"Node{n}"]
    return Config(_conf)


class TestServerSnapshotIntegration:
    """Test snapshot functionality in server classes."""

    def test_leader_creates_snapshot_when_threshold_exceeded(self):
        """Test that a leader creates snapshots when log size exceeds threshold."""
        # Setup server with small snapshot threshold
        config = _make_config(node_count=3, snapshot_threshold=3)
        storage = InMemoryStorage(1, config)
        state_machine = KeyValueStateMachine()

        leader = Leader(node_id=1, config=config, storage=storage, state_machine=state_machine)

        # Add some entries to the log and apply them
        entries = [
            LogEntry(term=1, data=json.dumps({"op": "set", "key": "a", "value": "1"}).encode()),
            LogEntry(term=1, data=json.dumps({"op": "set", "key": "b", "value": "2"}).encode()),
            LogEntry(term=1, data=json.dumps({"op": "set", "key": "c", "value": "3"}).encode()),
            LogEntry(term=1, data=json.dumps({"op": "set", "key": "d", "value": "4"}).encode()),
        ]

        for entry in entries:
            leader.log.append_entries([entry])

        # Set commit_index to apply all entries
        leader.commit_index = len(entries) - 1

        # Initially no snapshots
        assert len(storage.list_snapshots()) == 0

        # Apply committed entries - this should trigger snapshot creation
        leader._apply_committed_entries()

        # Check that snapshot should be created
        assert leader.should_create_snapshot()

        # Create snapshot
        snapshot_id = leader.create_snapshot()
        assert snapshot_id is not None

        # Verify snapshot was saved
        snapshots = storage.list_snapshots()
        assert len(snapshots) == 1

        # Verify state machine state was captured
        snapshot = storage.load_snapshot(snapshot_id)
        assert snapshot is not None
        snapshot_data = json.loads(snapshot.state_machine_data.decode())
        assert snapshot_data["a"] == "1"
        assert snapshot_data["b"] == "2"
        assert snapshot_data["c"] == "3"
        assert snapshot_data["d"] == "4"

    def test_server_restores_from_snapshot_on_init(self):
        """Test that servers restore state from snapshots during initialization."""
        # Setup storage with existing snapshot
        config = _make_config(node_count=3)
        storage = InMemoryStorage(1, config)
        state_machine = KeyValueStateMachine()

        # Create a leader and make a snapshot
        leader = Leader(node_id=1, config=config, storage=storage, state_machine=state_machine)

        # Add entries and create snapshot
        entries = [
            LogEntry(term=1, data=json.dumps({"op": "set", "key": "restore_test", "value": "success"}).encode()),
        ]

        for entry in entries:
            leader.log.append_entries([entry])

        leader.commit_index = len(entries) - 1
        leader._apply_committed_entries()
        leader.create_snapshot()

        # Create a new server instance - should restore from snapshot
        new_state_machine = KeyValueStateMachine()
        follower = Follower(node_id=2, config=config, storage=storage, state_machine=new_state_machine)

        # Verify restoration occurred
        assert follower.last_snapshot_index >= 0
        assert new_state_machine.get("restore_test") == "success"

    def test_log_compaction_after_snapshot(self):
        """Test that logs are compacted after snapshot creation."""
        config = _make_config(node_count=3)
        storage = InMemoryStorage(1, config)
        state_machine = KeyValueStateMachine()

        leader = Leader(node_id=1, config=config, storage=storage, state_machine=state_machine)

        # Add many entries
        entries = [
            LogEntry(term=1, data=json.dumps({"op": "set", "key": f"key_{i}", "value": f"val_{i}"}).encode()) for i in range(10)
        ]

        for entry in entries:
            leader.log.append_entries([entry])

        leader.commit_index = len(entries) - 1
        leader._apply_committed_entries()

        # Create snapshot
        snapshot_id = leader.create_snapshot()

        # Compact log (keeping some entries for safety)
        keep_entries = 2
        storage.compact_log(leader.last_applied - keep_entries)

        # Verify log was compacted but some entries remain
        remaining_log = storage.load_log()
        assert len(remaining_log.log) <= keep_entries + 1  # +1 for index 0

        # Verify snapshot contains all the data
        snapshot = storage.load_snapshot(snapshot_id)
        snapshot_data = json.loads(snapshot.state_machine_data.decode())
        for i in range(10):
            assert snapshot_data[f"key_{i}"] == f"val_{i}"

    def test_state_machine_integration_with_get_applied_entry_result(self):
        """Test that get_applied_entry_result works with state machine."""
        config = _make_config(node_count=3)
        storage = InMemoryStorage(1, config)
        state_machine = KeyValueStateMachine()

        leader = Leader(node_id=1, config=config, storage=storage, state_machine=state_machine)

        # Add a GET entry
        get_entry = LogEntry(term=1, data=json.dumps({"op": "get", "key": "test_key"}).encode())
        leader.log.append_entries([get_entry])

        # Set a value first
        state_machine.apply_entry(
            LogEntry(term=1, data=json.dumps({"op": "set", "key": "test_key", "value": "test_value"}).encode())
        )

        # Test get_applied_entry_result
        result = leader.get_applied_entry_result(get_entry)
        assert result == "test_value"

        # Test with non-existent key
        get_missing = LogEntry(term=1, data=json.dumps({"op": "get", "key": "missing_key"}).encode())
        result = leader.get_applied_entry_result(get_missing)
        assert result is None
