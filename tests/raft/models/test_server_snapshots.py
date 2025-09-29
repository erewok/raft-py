"""Tests for server snapshot integration."""

from raft.io.storage import InMemoryStorage
from raft.models.config import Config
from raft.models.log import LogEntry
from raft.models.server import Follower, Leader
from raft.models.snapshot import KeyValueStateMachine


class TestServerSnapshotIntegration:
    """Test snapshot functionality in server classes."""

    def test_leader_creates_snapshot_when_threshold_exceeded(self):
        """Test that a leader creates snapshots when log size exceeds threshold."""
        # Setup server with small snapshot threshold
        storage = InMemoryStorage()
        state_machine = KeyValueStateMachine()
        config = Config(nodes=[1, 2, 3], log_compaction_threshold=3)

        leader = Leader(node_id=1, config=config, storage=storage, state_machine=state_machine)

        # Add some entries to the log and apply them
        entries = [
            LogEntry(term=1, command={"op": "set", "key": "a", "value": "1"}),
            LogEntry(term=1, command={"op": "set", "key": "b", "value": "2"}),
            LogEntry(term=1, command={"op": "set", "key": "c", "value": "3"}),
            LogEntry(term=1, command={"op": "set", "key": "d", "value": "4"}),
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
        assert snapshot.data["a"] == "1"
        assert snapshot.data["b"] == "2"
        assert snapshot.data["c"] == "3"
        assert snapshot.data["d"] == "4"

    def test_server_restores_from_snapshot_on_init(self):
        """Test that servers restore state from snapshots during initialization."""
        # Setup storage with existing snapshot
        storage = InMemoryStorage()
        state_machine = KeyValueStateMachine()
        config = Config(nodes=[1, 2, 3])

        # Create a leader and make a snapshot
        leader = Leader(node_id=1, config=config, storage=storage, state_machine=state_machine)

        # Add entries and create snapshot
        entries = [
            LogEntry(term=1, command={"op": "set", "key": "restore_test", "value": "success"}),
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
        storage = InMemoryStorage()
        state_machine = KeyValueStateMachine()
        config = Config(nodes=[1, 2, 3])

        leader = Leader(node_id=1, config=config, storage=storage, state_machine=state_machine)

        # Add many entries
        entries = [
            LogEntry(term=1, command={"op": "set", "key": f"key_{i}", "value": f"val_{i}"}) for i in range(10)
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
        for i in range(10):
            assert snapshot.data[f"key_{i}"] == f"val_{i}"

    def test_state_machine_integration_with_get_applied_entry_result(self):
        """Test that get_applied_entry_result works with state machine."""
        storage = InMemoryStorage()
        state_machine = KeyValueStateMachine()
        config = Config(nodes=[1, 2, 3])

        leader = Leader(node_id=1, config=config, storage=storage, state_machine=state_machine)

        # Add a GET entry
        get_entry = LogEntry(term=1, command={"op": "get", "key": "test_key"})
        leader.log.append_entries([get_entry])

        # Set a value first
        state_machine.apply_entry(
            LogEntry(term=1, command={"op": "set", "key": "test_key", "value": "test_value"})
        )

        # Test get_applied_entry_result
        result = leader.get_applied_entry_result(get_entry)
        assert result == "test_value"

        # Test with non-existent key
        get_missing = LogEntry(term=1, command={"op": "get", "key": "missing_key"})
        result = leader.get_applied_entry_result(get_missing)
        assert result is None
