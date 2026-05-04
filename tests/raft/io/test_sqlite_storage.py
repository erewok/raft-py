"""
Tests for SQLiteStorage implementation.

These tests verify that the SQLiteStorage class correctly implements
all BaseStorage interface methods with proper ACID transactions,
performance optimizations, and error handling.
"""

import os
import tempfile
import time
from threading import Thread

import pytest
from sqlite3 import IntegrityError

from raft.io.storage import SqliteStorage
from raft.models.config import Config
from raft.models.snapshot import KeyValueStateMachine, Snapshot


def create_mock_config(data_directory="/tmp"):
    """Create a mock config for testing"""
    import configparser

    conf = configparser.ConfigParser()
    conf.add_section("Cluster")
    conf.set("Cluster", "Debug", "True")
    conf.set("Cluster", "DataDirectory", data_directory)
    conf.set("Cluster", "HeartbeatInterval", "5")
    conf.set("Cluster", "ElectionTimeout", "1000")
    conf.set("Cluster", "NodeCount", "3")
    conf.set("Cluster", "StorageClass", "SqliteStorage")

    # Add node mapping
    conf.add_section("Nodes")
    conf.set("Nodes", "Node1", "A")
    conf.set("Nodes", "Node2", "B")
    conf.set("Nodes", "Node3", "C")

    # Add individual nodes
    for i, label in enumerate(["A", "B", "C"], 1):
        section = f"Node.{label}"
        conf.add_section(section)
        conf.set(section, "Label", label)
        conf.set(section, "Id", str(i))
        conf.set(section, "Port", str(3110 + i))
        conf.set(section, "Host", "127.0.0.1")

    return Config(conf)


class TestSqliteStorage:
    """Test SQLite storage functionality"""

    def setup_method(self):
        """Set up test environment with temporary directory"""
        self.temp_dir = tempfile.mkdtemp()
        self.config = create_mock_config(self.temp_dir)

    def teardown_method(self):
        """Clean up temporary directory"""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_initialization_and_schema_creation(self):
        """Test SQLite storage initialization and schema creation"""
        storage = SqliteStorage(1, self.config)

        # Verify database file was created
        assert os.path.exists(storage.db_path)

        # Verify schema was created by checking tables exist
        conn = storage._get_connection()
        cursor = conn.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name IN ('metadata', 'log_entries', 'snapshots')
        """)
        tables = [row[0] for row in cursor.fetchall()]

        assert "metadata" in tables
        assert "log_entries" in tables
        assert "snapshots" in tables

        storage.close()

    def test_save_and_load_metadata(self):
        """Test metadata save and load operations"""
        storage = SqliteStorage(1, self.config)

        test_metadata = b'{"current_term": 5, "voted_for": 2}'

        # Save metadata
        storage.save_metadata(test_metadata)

        # Verify it was saved (check directly in database)
        conn = storage._get_connection()
        cursor = conn.execute("SELECT value FROM metadata WHERE key = ?", (f"node_{storage.node_id}",))
        row = cursor.fetchone()

        assert row is not None
        assert row["value"] == test_metadata

        storage.close()

    def test_save_and_load_log_entries(self):
        """Test log entry operations"""
        storage = SqliteStorage(1, self.config)

        # Save multiple log entries
        entries = [
            b'{"term": 1, "command": "set key1 value1"}',
            b'{"term": 1, "command": "set key2 value2"}',
            b'{"term": 2, "command": "set key3 value3"}',
        ]

        for entry in entries:
            storage.save_log_entry(entry)

        # Load all entries
        loaded_entries = storage.load_log()
        assert len(loaded_entries) == 3

        # Verify entries match (order should be preserved)
        for i, entry in enumerate(entries):
            assert loaded_entries[i] == entry

        # Test individual entry retrieval
        entry_1 = storage.get_log_entry(1)  # SQLite uses 1-based indexing
        assert entry_1 == entries[0]

        # Test range retrieval
        entries_from_2 = storage.get_log_entries_from(2)
        assert len(entries_from_2) == 2
        assert entries_from_2[0] == entries[1]
        assert entries_from_2[1] == entries[2]

        storage.close()

    def test_save_and_load_snapshot(self):
        """Test snapshot save and load operations"""
        storage = SqliteStorage(1, self.config)

        # Create a test snapshot
        sm = KeyValueStateMachine()
        sm.set("key1", "value1")
        sm.set("key2", "value2")

        snapshot = Snapshot.create(
            last_included_index=5,
            last_included_term=2,
            state_machine_data=sm.create_snapshot(),
            configuration={"nodes": {1: "node1", 2: "node2"}},
        )

        # Save snapshot
        snapshot_id = storage.save_snapshot(snapshot)
        assert snapshot_id.startswith("sqlite_snapshot_")

        # Load snapshot
        loaded_snapshot = storage.load_snapshot(snapshot_id)
        assert loaded_snapshot.last_included_index == 5
        assert loaded_snapshot.last_included_term == 2
        assert loaded_snapshot.verify_integrity()

        # Verify state machine data
        sm2 = KeyValueStateMachine()
        sm2.restore_from_snapshot(loaded_snapshot.state_machine_data)
        assert sm2.get("key1") == "value1"
        assert sm2.get("key2") == "value2"

        storage.close()

    def test_snapshot_metadata_operations(self):
        """Test snapshot metadata retrieval operations"""
        storage = SqliteStorage(1, self.config)

        # Initially no snapshots
        assert storage.get_latest_snapshot_metadata() is None
        assert len(storage.list_snapshots()) == 0

        # Create multiple snapshots with different timestamps
        snapshots = []
        for i in range(3):
            sm = KeyValueStateMachine()
            sm.set(f"key_{i}", f"value_{i}")

            snapshot = Snapshot.create(
                last_included_index=i + 1,
                last_included_term=1,
                state_machine_data=sm.create_snapshot(),
                configuration={"nodes": {1: "node1"}},
            )

            snapshot_id = storage.save_snapshot(snapshot)
            snapshots.append(snapshot_id)
            time.sleep(0.01)  # Ensure different timestamps

        # Test latest snapshot metadata
        latest_metadata = storage.get_latest_snapshot_metadata()
        assert latest_metadata is not None
        assert latest_metadata.last_included_index == 3  # Last snapshot

        # Test listing snapshots (should be ordered newest first)
        all_snapshots = storage.list_snapshots()
        assert len(all_snapshots) == 3

        # Verify ordering (newest first)
        assert all_snapshots[0].last_included_index == 3
        assert all_snapshots[1].last_included_index == 2
        assert all_snapshots[2].last_included_index == 1

        storage.close()

    def test_delete_snapshot(self):
        """Test snapshot deletion"""
        storage = SqliteStorage(1, self.config)

        # Create a snapshot
        sm = KeyValueStateMachine()
        snapshot = Snapshot.create(
            last_included_index=1,
            last_included_term=1,
            state_machine_data=sm.create_snapshot(),
            configuration={},
        )
        snapshot_id = storage.save_snapshot(snapshot)

        # Verify it exists
        assert storage.load_snapshot(snapshot_id) is not None

        # Delete it
        assert storage.delete_snapshot(snapshot_id) is True

        # Verify it's gone
        with pytest.raises(KeyError):
            storage.load_snapshot(snapshot_id)

        # Deleting non-existent snapshot should return False
        assert storage.delete_snapshot("non_existent") is False

        storage.close()

    def test_delete_old_snapshots(self):
        """Test bulk deletion of old snapshots"""
        storage = SqliteStorage(1, self.config)

        # Create 5 snapshots
        for i in range(5):
            sm = KeyValueStateMachine()
            snapshot = Snapshot.create(
                last_included_index=i + 1,
                last_included_term=1,
                state_machine_data=sm.create_snapshot(),
                configuration={},
            )
            storage.save_snapshot(snapshot)
            time.sleep(0.01)  # Ensure different timestamps

        # Keep only 2 most recent
        deleted_count = storage.delete_old_snapshots(keep_count=2)
        assert deleted_count == 3

        # Verify only 2 snapshots remain
        remaining_snapshots = storage.list_snapshots()
        assert len(remaining_snapshots) == 2

        # Verify the kept snapshots are the most recent ones
        assert remaining_snapshots[0].last_included_index == 5
        assert remaining_snapshots[1].last_included_index == 4

        storage.close()

    def test_log_compaction(self):
        """Test log entry compaction"""
        storage = SqliteStorage(1, self.config)

        # Add 10 log entries
        for i in range(10):
            storage.save_log_entry(f"entry_{i}".encode())

        # Verify all entries exist
        entries = storage.load_log()
        assert len(entries) == 10

        # Compact up to index 5 (SQLite uses 1-based indexing)
        deleted_count = storage.compact_log(5)
        assert deleted_count == 5

        # Verify only entries 6-10 remain
        remaining_entries = storage.load_log()
        assert len(remaining_entries) == 5

        storage.close()

    def test_storage_statistics(self):
        """Test storage statistics functionality"""
        storage = SqliteStorage(1, self.config)

        # Initial stats
        stats = storage.get_storage_stats()
        assert stats["log_entries_count"] == 0
        assert stats["snapshots_count"] == 0
        assert stats["database_size_bytes"] > 0  # Database file exists
        assert stats["database_path"] == storage.db_path

        # Add some data
        storage.save_log_entry(b"test_entry")

        sm = KeyValueStateMachine()
        snapshot = Snapshot.create(
            last_included_index=1,
            last_included_term=1,
            state_machine_data=sm.create_snapshot(),
            configuration={},
        )
        storage.save_snapshot(snapshot)

        # Updated stats
        updated_stats = storage.get_storage_stats()
        assert updated_stats["log_entries_count"] == 1
        assert updated_stats["snapshots_count"] == 1
        assert (
            updated_stats["database_size_bytes"] >= stats["database_size_bytes"]
        )  # May be same due to SQLite page allocation

        storage.close()

    def test_concurrent_access(self):
        """Test thread-safe concurrent access"""
        storage = SqliteStorage(1, self.config)

        def write_entries(start_idx, count):
            """Write log entries from a separate thread"""
            for i in range(count):
                entry = f"thread_entry_{start_idx}_{i}".encode()
                storage.save_log_entry(entry)

        def write_snapshots(start_idx, count):
            """Write snapshots from a separate thread"""
            for i in range(count):
                sm = KeyValueStateMachine()
                sm.set(f"thread_key_{start_idx}_{i}", f"value_{i}")
                snapshot = Snapshot.create(
                    last_included_index=start_idx * 100 + i,
                    last_included_term=1,
                    state_machine_data=sm.create_snapshot(),
                    configuration={},
                )
                storage.save_snapshot(snapshot)

        # Start multiple threads
        threads = []
        for i in range(3):
            # Each thread writes 5 entries and 2 snapshots
            entry_thread = Thread(target=write_entries, args=(i, 5))
            snapshot_thread = Thread(target=write_snapshots, args=(i, 2))

            threads.extend([entry_thread, snapshot_thread])

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all to complete
        for thread in threads:
            thread.join()

        # Verify all data was written
        entries = storage.load_log()
        assert len(entries) == 15  # 3 threads * 5 entries each

        snapshots = storage.list_snapshots()
        assert len(snapshots) == 6  # 3 threads * 2 snapshots each

        storage.close()

    def test_transaction_rollback_on_error(self):
        """Test that transactions are properly rolled back on errors"""
        storage = SqliteStorage(1, self.config)

        # Create a valid snapshot first
        sm = KeyValueStateMachine()
        snapshot = Snapshot.create(
            last_included_index=1,
            last_included_term=1,
            state_machine_data=sm.create_snapshot(),
            configuration={},
        )
        snapshot_id = storage.save_snapshot(snapshot)

        # Verify initial state
        initial_count = len(storage.list_snapshots())
        assert initial_count == 1

        # Attempt to create an invalid snapshot that should fail
        # (We'll simulate this by trying to insert duplicate primary key)
        with pytest.raises(IntegrityError):  # Should raise some database error
            with storage._transaction() as conn:
                # This should succeed initially
                conn.execute(
                    """
                    INSERT INTO snapshots (
                        snapshot_id, last_included_index, last_included_term,
                        state_machine_data, configuration, timestamp,
                        checksum, size_bytes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    ("new_snapshot_id", 2, 1, b"data", "{}", time.time(), "checksum", 4),
                )

                # This should fail with duplicate key error
                conn.execute(
                    """
                    INSERT INTO snapshots (
                        snapshot_id, last_included_index, last_included_term,
                        state_machine_data, configuration, timestamp,
                        checksum, size_bytes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        snapshot_id,  # Duplicate ID - should fail
                        3,
                        1,
                        b"data2",
                        "{}",
                        time.time(),
                        "checksum2",
                        5,
                    ),
                )

        # Verify the transaction was rolled back - should still have only 1 snapshot
        final_count = len(storage.list_snapshots())
        assert final_count == initial_count  # No new snapshots added

        storage.close()

    def test_connection_cleanup(self):
        """Test proper connection cleanup"""
        storage = SqliteStorage(1, self.config)

        # Use the storage to create connections
        storage.save_metadata(b"test")

        # Verify connection exists
        assert hasattr(storage._local, "connection")
        assert storage._local.connection is not None

        # Close storage
        storage.close()

        # Verify connection was closed
        assert storage._local.connection is None

    def test_database_file_path(self):
        """Test that database files are created with correct paths"""
        storage1 = SqliteStorage(1, self.config)
        storage2 = SqliteStorage(2, self.config)

        # Verify different nodes get different database files
        assert storage1.db_path != storage2.db_path
        assert "node_1.db" in storage1.db_path
        assert "node_2.db" in storage2.db_path

        # Verify both files exist
        assert os.path.exists(storage1.db_path)
        assert os.path.exists(storage2.db_path)

        storage1.close()
        storage2.close()
