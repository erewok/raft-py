#!/usr/bin/env python3
"""
Async SQLite Storage Demo for Raft Implementation

This script demonstrates the trio-compatible async SQLite storage backend,
showcasing async/await patterns, ACID transactions, and performance features.
"""

import json
import logging
import tempfile
import time

import trio
from raft.io.storage import AsyncSqliteStorage
from raft.models.config import Config
from raft.models.log import LogEntry
from raft.models.snapshot import KeyValueStateMachine, Snapshot

# Configure logging for the demo
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def create_demo_config(data_directory):
    """Create a demo configuration for async SQLite storage."""
    import configparser

    conf = configparser.ConfigParser()
    conf.add_section("Cluster")
    conf.set("Cluster", "Debug", "True")
    conf.set("Cluster", "DataDirectory", data_directory)
    conf.set("Cluster", "HeartbeatInterval", "5")
    conf.set("Cluster", "ElectionTimeout", "1000")
    conf.set("Cluster", "NodeCount", "3")
    conf.set("Cluster", "StorageClass", "AsyncSqliteStorage")
    conf.set("Cluster", "SnapshotThreshold", "5")  # Low threshold for demo
    conf.set("Cluster", "MaxSnapshotsToKeep", "3")
    conf.set("Cluster", "StateMachineClass", "KeyValueStateMachine")

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


class MockLogEntry:
    """Simple mock log entry for demo purposes."""

    def __init__(self, term, data):
        self.term = term
        self.data = data


async def main():
    """Async main function demonstrating AsyncSqliteStorage features."""
    logger.info("🔄 Async SQLite Storage Demo for Raft Implementation")
    logger.info("=" * 60)

    # Create temporary directory for this demo
    with tempfile.TemporaryDirectory() as temp_dir:
        logger.info(f"📁 Using temporary directory: {temp_dir}")

        # Create configuration
        config = create_demo_config(temp_dir)
        logger.info("⚙️  Configuration created successfully")

        # Initialize storage
        storage = AsyncSqliteStorage(node_id=1, config=config)
        logger.info("💾 AsyncSqliteStorage initialized")

        # 1. Basic metadata operations
        logger.info("1. Metadata operations:")
        metadata = b'{"node_id": 1, "cluster": "demo", "version": "1.0"}'
        await storage.save_metadata(metadata)
        logger.info("   ✓ Metadata saved")

        # 2. Log entry operations
        logger.info("\\n2. Log entry operations:")

        # Create some log entries
        log_entries = [
            MockLogEntry(1, b'{"op": "set", "key": "user1", "value": "Alice"}'),
            MockLogEntry(1, b'{"op": "set", "key": "user2", "value": "Bob"}'),
            MockLogEntry(2, b'{"op": "set", "key": "user3", "value": "Charlie"}'),
            MockLogEntry(2, b'{"op": "delete", "key": "user2"}'),
            MockLogEntry(3, b'{"op": "set", "key": "user4", "value": "Diana"}'),
        ]

        for i, entry in enumerate(log_entries):
            await storage.save_log_entry(entry)
            logger.info(f"   ✓ Log entry {i + 1} saved (term {entry.term})")

        # 3. State machine and snapshot operations
        logger.info("\\n3. Snapshot operations:")

        # Create state machine
        state_machine = KeyValueStateMachine()
        state_machine.data = {"user1": "Alice", "user3": "Charlie", "user4": "Diana"}

        # Create snapshot
        snapshot = Snapshot.create(
            last_included_index=5,
            last_included_term=3,
            state_machine_data=state_machine.create_snapshot(),
            configuration={"nodes": ["A", "B", "C"]},
        )

        snapshot_id = await storage.save_snapshot(snapshot)
        logger.info(f"   ✓ Snapshot saved with ID: {snapshot_id}")

        # Load snapshot back
        loaded_snapshot = await storage.load_snapshot(snapshot_id)
        logger.info(f"   ✓ Snapshot loaded successfully")
        logger.info(f"     - Last included index: {loaded_snapshot.last_included_index}")
        logger.info(f"     - Last included term: {loaded_snapshot.last_included_term}")

        # Verify state machine data
        loaded_state_machine = KeyValueStateMachine()
        loaded_state_machine.restore_from_snapshot(loaded_snapshot.state_machine_data)
        logger.info(f"   ✓ State machine restored with {len(loaded_state_machine.data)} entries")

        # 4. Snapshot metadata operations
        logger.info("\\n4. Snapshot metadata operations:")

        # Get latest snapshot metadata
        latest = await storage.get_latest_snapshot_metadata()
        if latest:
            logger.info(f"   ✓ Latest snapshot: {latest.snapshot_id}")
            logger.info(f"     - Size: {latest.size_bytes} bytes")
            logger.info(f"     - Created at: {latest.created_at}")

        # List all snapshots
        all_snapshots = await storage.list_snapshots()
        logger.info(f"   ✓ Found {len(all_snapshots)} snapshots total")

        # 5. Create additional snapshots for testing
        logger.info("\\n5. Creating additional snapshots:")

        additional_snapshots = []
        for i in range(4):  # Create 4 more snapshots
            # Wait a tiny bit to ensure different timestamps
            await trio.sleep(0.001)

            # Modify state machine
            state_machine.data[f"extra_user_{i}"] = f"User{i}"

            snapshot = Snapshot.create(
                last_included_index=10 + i,
                last_included_term=4,
                state_machine_data=state_machine.create_snapshot(),
                configuration={"nodes": ["A", "B", "C"]},
            )

            snap_id = await storage.save_snapshot(snapshot)
            additional_snapshots.append(snap_id)
            logger.info(f"   ✓ Created snapshot {i + 1}: {snap_id}")

        # 6. Test snapshot cleanup
        logger.info("\\n6. Snapshot cleanup operations:")

        snapshots_before = await storage.list_snapshots()
        logger.info(f"   📊 Snapshots before cleanup: {len(snapshots_before)}")

        deleted_count = await storage.delete_old_snapshots(keep_count=3)
        logger.info(f"   🗑️  Deleted {deleted_count} old snapshots")

        snapshots_after = await storage.list_snapshots()
        logger.info(f"   📊 Snapshots after cleanup: {len(snapshots_after)}")

        # 7. Log compaction
        logger.info("\\n7. Log compaction:")

        stats_before = await storage.get_database_stats()
        entries_before = stats_before["log_entries_count"]
        logger.info(f"   📊 Log entries before compaction: {entries_before}")

        compacted_count = await storage.compact_log(up_to_index=3)
        logger.info(f"   🗜️  Compacted {compacted_count} log entries")

        stats_after = await storage.get_database_stats()
        entries_after = stats_after["log_entries_count"]
        logger.info(f"   📊 Log entries after compaction: {entries_after}")

        # 8. Database statistics
        logger.info("\\n8. Database statistics:")
        stats = await storage.get_database_stats()

        logger.info(f"   📊 Database statistics:")
        logger.info(f"     - Log entries: {stats['log_entries_count']}")
        logger.info(f"     - Snapshots: {stats['snapshots_count']}")
        logger.info(f"     - Metadata records: {stats['metadata_count']}")
        logger.info(f"     - Database size: {stats['database_size_bytes']} bytes")
        logger.info(f"     - Database path: {stats['database_path']}")
        logger.info(f"     - Node ID: {stats['node_id']}")
        logger.info(f"     - Node label: {stats['node_label']}")

        # 9. Concurrent operations test
        logger.info("\\n9. Concurrent operations test:")

        async def concurrent_writer(start_idx, count, nursery_name):
            """Write log entries concurrently."""
            for i in range(count):
                entry = MockLogEntry(4, f"concurrent_{nursery_name}_{start_idx + i}".encode())
                await storage.save_log_entry(entry)

        start_time = time.time()

        # Run multiple concurrent writers
        async with trio.open_nursery() as nursery:
            nursery.start_soon(concurrent_writer, 0, 20, "A")
            nursery.start_soon(concurrent_writer, 20, 20, "B")
            nursery.start_soon(concurrent_writer, 40, 20, "C")

        concurrent_time = time.time() - start_time
        logger.info(f"   ✓ Completed 60 concurrent writes in {concurrent_time:.3f} seconds")

        # 10. Performance demonstration
        logger.info("\\n10. Performance demonstration:")

        # Batch insert test
        start_time = time.time()

        async def batch_insert():
            for i in range(100):
                entry = MockLogEntry(5, f"perf_test_entry_{i}".encode())
                await storage.save_log_entry(entry)

        await batch_insert()

        insert_time = time.time() - start_time
        logger.info(f"    ⚡ Inserted 100 log entries in {insert_time:.3f} seconds")
        logger.info(f"    📈 Rate: {100 / insert_time:.1f} entries/second")

        # Batch query test using stats (since we don't have load_log on async)
        start_time = time.time()

        for _ in range(100):
            await storage.get_database_stats()

        query_time = time.time() - start_time
        logger.info(f"    ⚡ Completed 100 database stat queries in {query_time:.3f} seconds")
        logger.info(f"    📈 Rate: {100 / query_time:.1f} queries/second")

        # Final statistics
        final_stats = await storage.get_database_stats()
        logger.info("📊 Final database state:")
        logger.info(f"   - Total log entries: {final_stats['log_entries_count']}")
        logger.info(f"   - Total snapshots: {final_stats['snapshots_count']}")
        logger.info(f"   - Database size: {final_stats['database_size_bytes']} bytes")


if __name__ == "__main__":
    trio.run(main)
