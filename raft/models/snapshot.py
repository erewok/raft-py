"""
Snapshot-related data structures and interfaces for Raft implementation.

This module provides the core components for snapshotting cluster state,
including snapshot data structures, state machine interface, and storage
abstractions for persistent snapshots.
"""

import hashlib
import json
import time
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any

from .log import LogEntry


@dataclass
class Snapshot:
    """
    A snapshot represents the state of the state machine at a specific point in time,
    along with metadata about the last log entry included in the snapshot.
    """

    last_included_index: int  # Index of last log entry included in snapshot
    last_included_term: int  # Term of last log entry included in snapshot
    state_machine_data: bytes  # Serialized state machine state
    configuration: dict  # Cluster configuration (nodes, etc.)
    timestamp: float  # When snapshot was created
    checksum: str  # Data integrity verification

    @classmethod
    def create(
        cls, last_included_index: int, last_included_term: int, state_machine_data: bytes, configuration: dict
    ) -> "Snapshot":
        """Create a new snapshot with computed checksum and timestamp"""
        timestamp = time.time()
        checksum = hashlib.sha256(state_machine_data).hexdigest()

        return cls(
            last_included_index=last_included_index,
            last_included_term=last_included_term,
            state_machine_data=state_machine_data,
            configuration=configuration,
            timestamp=timestamp,
            checksum=checksum,
        )

    def verify_integrity(self) -> bool:
        """Verify that the snapshot data hasn't been corrupted"""
        computed_checksum = hashlib.sha256(self.state_machine_data).hexdigest()
        return computed_checksum == self.checksum

    def to_dict(self) -> dict:
        """Convert snapshot to dictionary for serialization"""
        return {
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "state_machine_data": self.state_machine_data.hex(),  # hex encode for JSON
            "configuration": self.configuration,
            "timestamp": self.timestamp,
            "checksum": self.checksum,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Snapshot":
        """Create snapshot from dictionary"""
        return cls(
            last_included_index=data["last_included_index"],
            last_included_term=data["last_included_term"],
            state_machine_data=bytes.fromhex(data["state_machine_data"]),
            configuration=data["configuration"],
            timestamp=data["timestamp"],
            checksum=data["checksum"],
        )


@dataclass
class SnapshotMetadata:
    """
    Metadata about a snapshot, used for tracking and management without
    loading the full snapshot data.
    """

    snapshot_id: str
    last_included_index: int
    last_included_term: int
    size_bytes: int
    created_at: float
    file_path: str

    def to_dict(self) -> dict:
        """Convert metadata to dictionary for serialization"""
        return {
            "snapshot_id": self.snapshot_id,
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "size_bytes": self.size_bytes,
            "created_at": self.created_at,
            "file_path": self.file_path,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SnapshotMetadata":
        """Create metadata from dictionary"""
        return cls(
            snapshot_id=data["snapshot_id"],
            last_included_index=data["last_included_index"],
            last_included_term=data["last_included_term"],
            size_bytes=data["size_bytes"],
            created_at=data["created_at"],
            file_path=data["file_path"],
        )


class StateMachine:
    """
    Abstract base class for state machines that can be snapshotted.

    The state machine represents the application-specific logic that processes
    committed log entries and maintains the application state.
    """

    @abstractmethod
    def apply_entry(self, entry: LogEntry) -> Any:
        """
        Apply a log entry to the state machine.

        Args:
            entry: The log entry to apply

        Returns:
            The result of applying the entry (application-specific)
        """
        raise NotImplementedError("Implement `apply_entry`")

    @abstractmethod
    def create_snapshot(self) -> bytes:
        """
        Create a snapshot of the current state machine state.

        Returns:
            Serialized state machine data as bytes
        """
        raise NotImplementedError("Implement `create_snapshot`")

    @abstractmethod
    def restore_from_snapshot(self, snapshot_data: bytes):
        """
        Restore the state machine state from snapshot data.

        Args:
            snapshot_data: Serialized state machine data
        """
        raise NotImplementedError("Implement `restore_from_snapshot`")

    @abstractmethod
    def get_state_size(self) -> int:
        """
        Get the approximate size of the current state in bytes.

        Returns:
            Approximate size of state in bytes
        """
        raise NotImplementedError("Implement `get_state_size`")


class KeyValueStateMachine(StateMachine):
    """
    Example key-value store state machine implementation.

    This serves as a reference implementation and can be used for testing
    the snapshot functionality.
    """

    def __init__(self):
        self.data: dict[str, str] = {}

    def apply_entry(self, entry: LogEntry) -> Any:
        """
        Apply a log entry containing a JSON-encoded key-value operation.

        Expected command format:
        - Set: {"op": "set", "key": "foo", "value": "bar"}
        - Delete: {"op": "delete", "key": "foo"}
        - Get: {"op": "get", "key": "foo"}
        """
        try:
            cmd = json.loads(entry.command.decode())
            op = cmd.get("op")
            key = cmd.get("key")

            if op == "set":
                value = cmd.get("value", "")
                self.data[key] = value
                return {"success": True, "key": key, "value": value}
            elif op == "delete":
                deleted_value = self.data.pop(key, None)
                return {"success": True, "key": key, "deleted_value": deleted_value}
            elif op == "get":
                value = self.data.get(key)
                return {"success": True, "key": key, "value": value}
            else:
                return {"success": False, "error": f"Unknown operation: {op}"}

        except (json.JSONDecodeError, KeyError) as e:
            return {"success": False, "error": f"Invalid command format: {e}"}

    def create_snapshot(self) -> bytes:
        """Create a JSON snapshot of the current key-value data"""
        return json.dumps(self.data).encode()

    def restore_from_snapshot(self, snapshot_data: bytes):
        """Restore key-value data from JSON snapshot"""
        try:
            self.data = json.loads(snapshot_data.decode())
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid snapshot data: {e}") from e

    def get_state_size(self) -> int:
        """Get approximate size of the key-value store"""
        return len(json.dumps(self.data).encode())

    def get(self, key: str) -> str | None:
        """Get a value by key (for external access)"""
        return self.data.get(key)

    def set(self, key: str, value: str):
        """Set a key-value pair (for external access)"""
        self.data[key] = value

    def delete(self, key: str) -> str | None:
        """Delete a key and return its value (for external access)"""
        return self.data.pop(key, None)

    def keys(self) -> list[str]:
        """Get all keys (for external access)"""
        return list(self.data.keys())

    def size(self) -> int:
        """Get number of key-value pairs"""
        return len(self.data)


class NoOpStateMachine(StateMachine):
    """
    A no-operation state machine for testing or minimal implementations.

    This state machine doesn't maintain any state and simply logs
    applied entries.
    """

    def __init__(self):
        self.applied_count = 0

    def apply_entry(self, entry: LogEntry) -> Any:
        """Log the applied entry and increment counter"""
        self.applied_count += 1
        return {
            "applied_count": self.applied_count,
            "entry_term": entry.term,
            "entry_command": entry.command.decode(),
        }

    def create_snapshot(self) -> bytes:
        """Create a minimal snapshot with just the applied count"""
        return json.dumps({"applied_count": self.applied_count}).encode()

    def restore_from_snapshot(self, snapshot_data: bytes):
        """Restore the applied count from snapshot"""
        try:
            data = json.loads(snapshot_data.decode())
            self.applied_count = data.get("applied_count", 0)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid snapshot data: {e}") from e

    def get_state_size(self) -> int:
        """Get size of the minimal state"""
        return len(self.create_snapshot())
