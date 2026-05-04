"""
Tests for the snapshot module.

These tests verify the basic functionality of snapshot data structures
and state machine implementations.
"""

import json

from raft.models.log import LogEntry
from raft.models.snapshot import (
    KeyValueStateMachine,
    NoOpStateMachine,
    Snapshot,
    SnapshotMetadata,
)


class TestSnapshot:
    """Test the Snapshot dataclass"""

    def test_create_snapshot(self):
        """Test creating a snapshot with computed checksum and timestamp"""
        data = b'{"key": "value"}'
        config = {"nodes": {1: "node1", 2: "node2"}}

        snapshot = Snapshot.create(
            last_included_index=5, last_included_term=2, state_machine_data=data, configuration=config
        )

        assert snapshot.last_included_index == 5
        assert snapshot.last_included_term == 2
        assert snapshot.state_machine_data == data
        assert snapshot.configuration == config
        assert snapshot.timestamp > 0
        assert len(snapshot.checksum) == 64  # SHA256 hex string

    def test_verify_integrity(self):
        """Test snapshot integrity verification"""
        data = b'{"key": "value"}'
        config = {"nodes": {1: "node1"}}

        snapshot = Snapshot.create(
            last_included_index=5, last_included_term=2, state_machine_data=data, configuration=config
        )

        # Valid snapshot should verify correctly
        assert snapshot.verify_integrity() is True

        # Corrupt the data
        snapshot.state_machine_data = b'{"corrupted": "data"}'
        assert snapshot.verify_integrity() is False

    def test_snapshot_serialization(self):
        """Test snapshot to_dict and from_dict"""
        data = b'{"key": "value"}'
        config = {"nodes": {1: "node1"}}

        original = Snapshot.create(
            last_included_index=5, last_included_term=2, state_machine_data=data, configuration=config
        )

        # Convert to dict and back
        snapshot_dict = original.to_dict()
        restored = Snapshot.from_dict(snapshot_dict)

        assert restored.last_included_index == original.last_included_index
        assert restored.last_included_term == original.last_included_term
        assert restored.state_machine_data == original.state_machine_data
        assert restored.configuration == original.configuration
        assert restored.timestamp == original.timestamp
        assert restored.checksum == original.checksum


class TestSnapshotMetadata:
    """Test the SnapshotMetadata dataclass"""

    def test_metadata_serialization(self):
        """Test metadata to_dict and from_dict"""
        metadata = SnapshotMetadata(
            snapshot_id="snap_123",
            last_included_index=5,
            last_included_term=2,
            size_bytes=1024,
            created_at=1234567890.0,
            file_path="/path/to/snapshot",
        )

        # Convert to dict and back
        metadata_dict = metadata.to_dict()
        restored = SnapshotMetadata.from_dict(metadata_dict)

        assert restored.snapshot_id == metadata.snapshot_id
        assert restored.last_included_index == metadata.last_included_index
        assert restored.last_included_term == metadata.last_included_term
        assert restored.size_bytes == metadata.size_bytes
        assert restored.created_at == metadata.created_at
        assert restored.file_path == metadata.file_path


class TestKeyValueStateMachine:
    """Test the KeyValueStateMachine implementation"""

    def test_apply_set_entry(self):
        """Test applying a SET operation"""
        sm = KeyValueStateMachine()

        cmd = json.dumps({"op": "set", "key": "foo", "value": "bar"}).encode()
        entry = LogEntry(term=1, data=cmd)

        result = sm.apply_entry(entry)

        assert result["success"] is True
        assert result["key"] == "foo"
        assert result["value"] == "bar"
        assert sm.data["foo"] == "bar"

    def test_apply_get_entry(self):
        """Test applying a GET operation"""
        sm = KeyValueStateMachine()
        sm.data["foo"] = "bar"

        cmd = json.dumps({"op": "get", "key": "foo"}).encode()
        entry = LogEntry(term=1, data=cmd)

        result = sm.apply_entry(entry)

        assert result["success"] is True
        assert result["key"] == "foo"
        assert result["value"] == "bar"

    def test_apply_delete_entry(self):
        """Test applying a DELETE operation"""
        sm = KeyValueStateMachine()
        sm.data["foo"] = "bar"

        cmd = json.dumps({"op": "delete", "key": "foo"}).encode()
        entry = LogEntry(term=1, data=cmd)

        result = sm.apply_entry(entry)

        assert result["success"] is True
        assert result["key"] == "foo"
        assert result["deleted_value"] == "bar"
        assert "foo" not in sm.data

    def test_apply_invalid_entry(self):
        """Test applying an invalid operation"""
        sm = KeyValueStateMachine()

        # Invalid JSON
        entry = LogEntry(term=1, data=b"invalid json")
        result = sm.apply_entry(entry)

        assert result["success"] is False
        assert "error" in result

    def test_snapshot_and_restore(self):
        """Test creating and restoring from snapshot"""
        sm = KeyValueStateMachine()
        sm.data = {"foo": "bar", "baz": "qux"}

        # Create snapshot
        snapshot_data = sm.create_snapshot()
        assert isinstance(snapshot_data, bytes)

        # Restore to new state machine
        sm2 = KeyValueStateMachine()
        sm2.restore_from_snapshot(snapshot_data)

        assert sm2.data == sm.data
        assert sm2.data["foo"] == "bar"
        assert sm2.data["baz"] == "qux"

    def test_get_state_size(self):
        """Test getting state size"""
        sm = KeyValueStateMachine()

        initial_size = sm.get_state_size()
        assert initial_size > 0

        sm.data["key"] = "value"
        new_size = sm.get_state_size()
        assert new_size > initial_size

    def test_external_access_methods(self):
        """Test external access methods"""
        sm = KeyValueStateMachine()

        # Test set and get
        sm.set("foo", "bar")
        assert sm.get("foo") == "bar"
        assert sm.get("nonexistent") is None

        # Test delete
        deleted = sm.delete("foo")
        assert deleted == "bar"
        assert sm.get("foo") is None

        # Test keys and size
        sm.set("a", "1")
        sm.set("b", "2")

        assert set(sm.keys()) == {"a", "b"}
        assert sm.size() == 2


class TestNoOpStateMachine:
    """Test the NoOpStateMachine implementation"""

    def test_apply_entry(self):
        """Test applying entries to NoOp state machine"""
        sm = NoOpStateMachine()

        entry = LogEntry(term=1, data=b"test command")
        result = sm.apply_entry(entry)

        assert result["applied_count"] == 1
        assert result["entry_term"] == 1
        assert result["entry_command"] == "test command"
        assert sm.applied_count == 1

    def test_snapshot_and_restore(self):
        """Test creating and restoring from snapshot"""
        sm = NoOpStateMachine()

        # Apply some entries
        for i in range(5):
            entry = LogEntry(term=1, data=f"command {i}".encode())
            sm.apply_entry(entry)

        assert sm.applied_count == 5

        # Create snapshot
        snapshot_data = sm.create_snapshot()

        # Restore to new state machine
        sm2 = NoOpStateMachine()
        sm2.restore_from_snapshot(snapshot_data)

        assert sm2.applied_count == 5

    def test_get_state_size(self):
        """Test getting state size"""
        sm = NoOpStateMachine()

        size = sm.get_state_size()
        assert size > 0
        assert isinstance(size, int)
