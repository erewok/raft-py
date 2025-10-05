"""
Tests for AsyncSqliteStorage implementation.

These tests verify that the AsyncSqliteStorage class correctly implements
all BaseStorage interface methods with proper async/await patterns,
ACID transactions, and trio compatibility.
"""

import os
import tempfile

import pytest
import trio
from raft.io.storage import AsyncSqliteStorage
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
    conf.set("Cluster", "StorageClass", "AsyncSqliteStorage")

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


class TestAsyncSqliteStorage:
    """Test async SQLite storage functionality"""

    def setup_method(self):
        """Set up test environment with temporary directory"""
        self.temp_dir = tempfile.mkdtemp()
        self.config = create_mock_config(self.temp_dir)

    def teardown_method(self):
        """Clean up temporary directory"""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_initialization_and_schema_creation(self):
        """Test async SQLite storage initialization and schema creation"""
        storage = AsyncSqliteStorage(1, self.config)

        # Initialize schema by saving metadata
        await storage.save_metadata(b"test metadata")

        # Verify database file was created
        assert os.path.exists(storage.db_path)

        # Verify schema was created by checking tables exist
        def _check_schema():
            conn = storage._get_connection()
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name IN ('metadata', 'log_entries', 'snapshots')
            """)
            tables = [row[0] for row in cursor.fetchall()]
            return tables

        tables = await trio.to_thread.run_sync(_check_schema)

        assert "metadata" in tables
        assert "log_entries" in tables
        assert "snapshots" in tables

    async def test_metadata_save_and_retrieve_functionality(self):
        """Test saving and retrieving metadata"""
        storage = AsyncSqliteStorage(1, self.config)

        # Save metadata
        test_data = b"test metadata content"
        await storage.save_metadata(test_data)

        # Verify metadata was saved by checking the database directly
        def _check_metadata():
            conn = storage._get_connection()
            cursor = conn.execute("""
                SELECT value FROM metadata WHERE key = 'raft_metadata'
            """)
            row = cursor.fetchone()
            return row

        row = await trio.to_thread.run_sync(_check_metadata)
        assert row is not None
        assert row["value"] == test_data

    async def test_log_entry_storage_and_retrieval(self):
        """Test storing log entries"""
        storage = AsyncSqliteStorage(1, self.config)

        # Create mock log entries
        class MockLogEntry:
            def __init__(self, term, data):
                self.term = term
                self.data = data

        entries = [
            MockLogEntry(1, b"entry 1 data"),
            MockLogEntry(1, b"entry 2 data"),
            MockLogEntry(2, b"entry 3 data"),
        ]

        # Save entries
        for entry in entries:
            await storage.save_log_entry(entry)

        # Verify entries were saved
        def _check_entries():
            conn = storage._get_connection()
            cursor = conn.execute("""
                SELECT term, data FROM log_entries ORDER BY id
            """)
            return [(row["term"], row["data"]) for row in cursor.fetchall()]

        saved_entries = await trio.to_thread.run_sync(_check_entries)

        assert len(saved_entries) == 3
        assert saved_entries[0] == (1, b"entry 1 data")
        assert saved_entries[1] == (1, b"entry 2 data")
        assert saved_entries[2] == (2, b"entry 3 data")

    async def test_snapshot_save_and_load_functionality(self):
        """Test saving and loading snapshots"""
        storage = AsyncSqliteStorage(1, self.config)

        # Create test snapshot
        state_machine = KeyValueStateMachine()
        state_machine.set("key1", "value1")
        state_machine.set("key2", "value2")

        snapshot = Snapshot.create(
            last_included_index=100,
            last_included_term=5,
            state_machine_data=state_machine.create_snapshot(),
            configuration={"nodes": ["A", "B", "C"]},
        )

        # Save snapshot
        snapshot_id = await storage.save_snapshot(snapshot)
        assert snapshot_id.startswith("async_sqlite_snapshot_")

        # Load snapshot
        loaded_snapshot = await storage.load_snapshot(snapshot_id)

        # Verify snapshot data
        assert loaded_snapshot.last_included_index == 100
        assert loaded_snapshot.last_included_term == 5
        assert loaded_snapshot.configuration == {"nodes": ["A", "B", "C"]}

        # Verify state machine data
        loaded_state_machine = KeyValueStateMachine()
        loaded_state_machine.restore_from_snapshot(loaded_snapshot.state_machine_data)
        assert loaded_state_machine.data.get("key1") == "value1"
        assert loaded_state_machine.data.get("key2") == "value2"

    async def test_snapshot_metadata_operations(self):
        """Test snapshot metadata retrieval operations"""
        storage = AsyncSqliteStorage(1, self.config)

        # Initially no snapshots
        latest = await storage.get_latest_snapshot_metadata()
        assert latest is None

        snapshots_list = await storage.list_snapshots()
        assert len(snapshots_list) == 0

        # Create and save test snapshots
        state_machine = KeyValueStateMachine()
        state_machine.set("key", "value")

        snapshots = []
        for i in range(3):
            snapshot = Snapshot.create(
                last_included_index=100 + i,
                last_included_term=5 + i,
                state_machine_data=state_machine.create_snapshot(),
                configuration={"nodes": ["A", "B", "C"]},
            )

            # Wait a bit to ensure different timestamps
            await trio.sleep(0.001)

            snapshot_id = await storage.save_snapshot(snapshot)
            snapshots.append((snapshot_id, snapshot))

        # Test latest snapshot metadata
        latest = await storage.get_latest_snapshot_metadata()
        assert latest is not None
        assert latest.last_included_index == 102  # Should be the last one
        assert latest.last_included_term == 7

        # Test list snapshots (should be ordered newest first)
        snapshots_list = await storage.list_snapshots()
        assert len(snapshots_list) == 3

        # Should be ordered by creation time, newest first
        assert snapshots_list[0].last_included_index == 102
        assert snapshots_list[1].last_included_index == 101
        assert snapshots_list[2].last_included_index == 100

    async def test_snapshot_deletion(self):
        """Test snapshot deletion functionality"""
        storage = AsyncSqliteStorage(1, self.config)

        # Create and save test snapshot
        state_machine = KeyValueStateMachine()
        snapshot = Snapshot.create(
            last_included_index=100,
            last_included_term=5,
            state_machine_data=state_machine.create_snapshot(),
            configuration=None,
        )

        snapshot_id = await storage.save_snapshot(snapshot)

        # Verify snapshot exists
        loaded = await storage.load_snapshot(snapshot_id)
        assert loaded.last_included_index == 100

        # Delete snapshot
        deleted = await storage.delete_snapshot(snapshot_id)
        assert deleted is True

        # Verify snapshot is gone
        with pytest.raises(KeyError):
            await storage.load_snapshot(snapshot_id)

        # Try to delete non-existent snapshot
        deleted = await storage.delete_snapshot("non_existent_id")
        assert deleted is False

    async def test_delete_old_snapshots(self):
        """Test deleting old snapshots while keeping recent ones"""
        storage = AsyncSqliteStorage(1, self.config)

        # Create multiple snapshots
        state_machine = KeyValueStateMachine()
        snapshot_ids = []

        for i in range(5):
            snapshot = Snapshot.create(
                last_included_index=100 + i,
                last_included_term=5,
                state_machine_data=state_machine.create_snapshot(),
                configuration=None,
            )

            # Wait to ensure different creation times
            await trio.sleep(0.001)

            snapshot_id = await storage.save_snapshot(snapshot)
            snapshot_ids.append(snapshot_id)

        # Delete old snapshots, keep 3
        deleted_count = await storage.delete_old_snapshots(keep_count=3)
        assert deleted_count == 2  # Should delete 2 oldest

        # Verify only 3 remain
        remaining = await storage.list_snapshots()
        assert len(remaining) == 3

        # Verify the correct ones remain (newest 3)
        remaining_indices = [s.last_included_index for s in remaining]
        assert 104 in remaining_indices  # Newest
        assert 103 in remaining_indices
        assert 102 in remaining_indices
        assert 101 not in remaining_indices  # Should be deleted
        assert 100 not in remaining_indices  # Should be deleted

    async def test_log_compaction(self):
        """Test log compaction functionality"""
        storage = AsyncSqliteStorage(1, self.config)

        # Create mock log entries
        class MockLogEntry:
            def __init__(self, term, data):
                self.term = term
                self.data = data

        # Save multiple entries
        for i in range(10):
            entry = MockLogEntry(1, f"entry {i} data".encode())
            await storage.save_log_entry(entry)

        # Compact log up to index 5
        deleted_count = await storage.compact_log(up_to_index=5)
        assert deleted_count == 5

        # Verify remaining entries
        async with await storage._get_connection() as conn:
            cursor = await conn.execute("SELECT COUNT(*) as count FROM log_entries")
            row = await cursor.fetchone()
            assert row["count"] == 5  # Should have 5 remaining

    async def test_database_stats(self):
        """Test database statistics functionality"""
        storage = AsyncSqliteStorage(1, self.config)

        # Get initial stats
        stats = await storage.get_database_stats()
        assert stats["log_entries_count"] == 0
        assert stats["snapshots_count"] == 0
        assert stats["metadata_count"] == 0
        assert stats["node_id"] == 1
        assert stats["node_label"] == "A"
        assert "database_size_bytes" in stats
        assert "database_path" in stats

        # Add some data
        await storage.save_metadata(b"test")

        class MockLogEntry:
            def __init__(self, term, data):
                self.term = term
                self.data = data

        await storage.save_log_entry(MockLogEntry(1, b"test data"))

        state_machine = KeyValueStateMachine()
        snapshot = Snapshot.create(100, 5, state_machine.create_snapshot(), None)
        await storage.save_snapshot(snapshot)

        # Get updated stats
        stats = await storage.get_database_stats()
        assert stats["log_entries_count"] == 1
        assert stats["snapshots_count"] == 1
        assert stats["metadata_count"] == 1
        assert stats["database_size_bytes"] > 0

    async def test_concurrent_access(self):
        """Test concurrent access to async storage"""
        storage = AsyncSqliteStorage(1, self.config)

        class MockLogEntry:
            def __init__(self, term, data):
                self.term = term
                self.data = data

        async def save_entries(start_idx, count):
            """Save multiple entries concurrently"""
            for i in range(count):
                entry = MockLogEntry(1, f"concurrent entry {start_idx + i}".encode())
                await storage.save_log_entry(entry)

        # Run concurrent operations
        async with trio.open_nursery() as nursery:
            nursery.start_soon(save_entries, 0, 10)
            nursery.start_soon(save_entries, 10, 10)
            nursery.start_soon(save_entries, 20, 10)

        # Verify all entries were saved
        def _check_count():
            conn = storage._get_connection()
            cursor = conn.execute("SELECT COUNT(*) as count FROM log_entries")
            row = cursor.fetchone()
            return row["count"]

        count = await trio.to_thread.run_sync(_check_count)
        assert count == 30  # All 30 entries should be saved

    async def test_error_handling(self):
        """Test error handling for invalid operations"""
        storage = AsyncSqliteStorage(1, self.config)

        # Test loading non-existent snapshot
        with pytest.raises(KeyError):
            await storage.load_snapshot("non_existent_snapshot_id")


# Run tests with trio
def test_async_sqlite_storage_initialization():
    """Wrapper to run async test with trio"""

    async def run_test():
        test_instance = TestAsyncSqliteStorage()
        test_instance.setup_method()
        try:
            await test_instance.test_initialization_and_schema_creation()
        finally:
            test_instance.teardown_method()

    trio.run(run_test)


def test_async_sqlite_storage_metadata():
    """Wrapper to run async test with trio"""

    async def run_test():
        test_instance = TestAsyncSqliteStorage()
        test_instance.setup_method()
        try:
            await test_instance.test_metadata_save_and_retrieve_functionality()
        finally:
            test_instance.teardown_method()

    trio.run(run_test)


def test_async_sqlite_storage_log_entries():
    """Wrapper to run async test with trio"""

    async def run_test():
        test_instance = TestAsyncSqliteStorage()
        test_instance.setup_method()
        try:
            await test_instance.test_log_entry_storage_and_retrieval()
        finally:
            test_instance.teardown_method()

    trio.run(run_test)


def test_async_sqlite_storage_snapshots():
    """Wrapper to run async test with trio"""

    async def run_test():
        test_instance = TestAsyncSqliteStorage()
        test_instance.setup_method()
        try:
            await test_instance.test_snapshot_save_and_load_functionality()
        finally:
            test_instance.teardown_method()

    trio.run(run_test)


def test_async_sqlite_storage_snapshot_metadata():
    """Wrapper to run async test with trio"""

    async def run_test():
        test_instance = TestAsyncSqliteStorage()
        test_instance.setup_method()
        try:
            await test_instance.test_snapshot_metadata_operations()
        finally:
            test_instance.teardown_method()

    trio.run(run_test)


def test_async_sqlite_storage_snapshot_deletion():
    """Wrapper to run async test with trio"""

    async def run_test():
        test_instance = TestAsyncSqliteStorage()
        test_instance.setup_method()
        try:
            await test_instance.test_snapshot_deletion()
        finally:
            test_instance.teardown_method()

    trio.run(run_test)


def test_async_sqlite_storage_delete_old_snapshots():
    """Wrapper to run async test with trio"""

    async def run_test():
        test_instance = TestAsyncSqliteStorage()
        test_instance.setup_method()
        try:
            await test_instance.test_delete_old_snapshots()
        finally:
            test_instance.teardown_method()

    trio.run(run_test)


def test_async_sqlite_storage_log_compaction():
    """Wrapper to run async test with trio"""

    async def run_test():
        test_instance = TestAsyncSqliteStorage()
        test_instance.setup_method()
        try:
            await test_instance.test_log_compaction()
        finally:
            test_instance.teardown_method()

    trio.run(run_test)


def test_async_sqlite_storage_database_stats():
    """Wrapper to run async test with trio"""

    async def run_test():
        test_instance = TestAsyncSqliteStorage()
        test_instance.setup_method()
        try:
            await test_instance.test_database_stats()
        finally:
            test_instance.teardown_method()

    trio.run(run_test)


def test_async_sqlite_storage_concurrent_access():
    """Wrapper to run async test with trio"""

    async def run_test():
        test_instance = TestAsyncSqliteStorage()
        test_instance.setup_method()
        try:
            await test_instance.test_concurrent_access()
        finally:
            test_instance.teardown_method()

    trio.run(run_test)


def test_async_sqlite_storage_error_handling():
    """Wrapper to run async test with trio"""

    async def run_test():
        test_instance = TestAsyncSqliteStorage()
        test_instance.setup_method()
        try:
            await test_instance.test_error_handling()
        finally:
            test_instance.teardown_method()

    trio.run(run_test)
