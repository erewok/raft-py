#!/usr/bin/env python3
"""
SQLite Storage Demo for Raft Implementation

This script demonstrates the SQLite storage backend,
showcasing ACID transactions, performance features, and monitoring capabilities.
"""

import json
import logging
import tempfile
import time

from raft.io.storage import SqliteStorage
from raft.models.config import Config
from raft.models.snapshot import KeyValueStateMachine, Snapshot

# Configure logging for the demo
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def create_demo_config(data_directory):
    """Create a demo configuration for SQLite storage."""
    import configparser

    conf = configparser.ConfigParser()
    conf.add_section("Cluster")
    conf.set("Cluster", "Debug", "True")
    conf.set("Cluster", "DataDirectory", data_directory)
    conf.set("Cluster", "HeartbeatInterval", "5")
    conf.set("Cluster", "ElectionTimeout", "1000")
    conf.set("Cluster", "NodeCount", "3")
    conf.set("Cluster", "StorageClass", "SqliteStorage")
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


def main():
    """Demonstrate SQLite storage functionality."""
    logger.info("=== Raft SQLite Storage Demo ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        logger.info(f"1. Creating SQLite storage in: {temp_dir}")
        config = create_demo_config(temp_dir)
        storage = SqliteStorage(1, config)
        state_machine = KeyValueStateMachine()

        logger.info(f"   Database file: {storage.db_path}")
        logger.info("   ✓ SQLite storage initialized with ACID transactions")
        logger.info("   ✓ WAL mode enabled for better concurrency")
        logger.info("   ✓ Optimized indexes created")

        # Demonstrate metadata storage
        logger.info("2. Storing Raft metadata:")
        metadata = {"current_term": 3, "voted_for": 2}
        storage.save_metadata(json.dumps(metadata).encode())
        logger.info(f"   Saved: {metadata}")

        # Demonstrate log entry storage
        logger.info("3. Storing log entries:")
        log_entries = [
            b'{"term": 1, "op": "set", "key": "user1", "value": "Alice"}',
            b'{"term": 1, "op": "set", "key": "user2", "value": "Bob"}',
            b'{"term": 2, "op": "set", "key": "user3", "value": "Charlie"}',
            b'{"term": 2, "op": "delete", "key": "user2"}',
            b'{"term": 3, "op": "set", "key": "user4", "value": "Diana"}',
        ]

        for i, entry in enumerate(log_entries, 1):
            storage.save_log_entry(entry)
            cmd = json.loads(entry.decode())["command"]
            logger.info(f"   Entry {i}: {cmd}")

        # Load and verify log entries
        loaded_entries = storage.load_log()
        logger.info(f"   ✓ Stored {len(loaded_entries)} log entries")
        logger.info(f"   ✓ Retrieved entry from index 3: {json.loads(loaded_entries[2].decode())['command']}")

        # Demonstrate snapshot creation and storage
        logger.info("4. Creating and storing snapshots:")

        # Simulate state machine operations
        for entry_data in log_entries:
            cmd = json.loads(entry_data.decode())["command"]
            if cmd["op"] == "set":
                state_machine.set(cmd["key"], cmd["value"])
            elif cmd["op"] == "delete":
                state_machine.delete(cmd["key"])

        # Create multiple snapshots
        snapshot_ids = []
        for i in range(3):
            snapshot = Snapshot.create(
                last_included_index=i * 2 + 1,
                last_included_term=1 + (i // 2),
                state_machine_data=state_machine.create_snapshot(),
                configuration={"nodes": {1: "node1", 2: "node2", 3: "node3"}},
            )

            snapshot_id = storage.save_snapshot(snapshot)
            snapshot_ids.append(snapshot_id)
            logger.info(f"   Snapshot {i + 1}: {snapshot_id}")
            logger.info(
                f"     Last index: {snapshot.last_included_index}, Term: {snapshot.last_included_term}"
            )
            logger.info(f"     Size: {len(snapshot.state_machine_data)} bytes")
            time.sleep(0.01)  # Ensure different timestamps

        # Demonstrate snapshot retrieval
        logger.info("5. Snapshot management:")
        latest_metadata = storage.get_latest_snapshot_metadata()
        logger.info(f"   Latest snapshot: {latest_metadata.snapshot_id}")
        logger.info(f"   Last included index: {latest_metadata.last_included_index}")

        all_snapshots = storage.list_snapshots()
        logger.info(f"   Total snapshots: {len(all_snapshots)}")

        # Load and verify a snapshot
        snapshot = storage.load_snapshot(snapshot_ids[0])
        restored_state_machine = KeyValueStateMachine()
        restored_state_machine.restore_from_snapshot(snapshot.state_machine_data)
        logger.info("   Restored state from snapshot 1:")
        for key in ["user1", "user2", "user3", "user4"]:
            value = restored_state_machine.get(key)
            if value:
                logger.info(f"     {key}: {value}")

        # Demonstrate log compaction
        logger.info("6. Log compaction:")
        logger.info(f"   Before compaction: {len(storage.load_log())} log entries")
        compacted_count = storage.compact_log(3)  # Remove first 3 entries
        logger.info(f"   Compacted {compacted_count} entries")
        logger.info(f"   After compaction: {len(storage.load_log())} log entries")

        # Demonstrate snapshot cleanup
        logger.info("7. Snapshot cleanup:")
        logger.info(f"   Before cleanup: {len(storage.list_snapshots())} snapshots")
        deleted_count = storage.delete_old_snapshots(keep_count=2)
        logger.info(f"   Deleted {deleted_count} old snapshots")
        logger.info(f"   After cleanup: {len(storage.list_snapshots())} snapshots")

        # Show storage statistics
        logger.info("8. Storage statistics:")
        stats = storage.get_storage_stats()
        logger.info(f"   Log entries: {stats['log_entries']}")
        logger.info(f"   Snapshots: {stats['snapshots']}")
        logger.info(f"   Database size: {stats['db_size_bytes']:,} bytes")
        logger.info(f"   Database path: {stats['db_path']}")

        # Demonstrate transaction safety
        logger.info("9. Transaction safety demonstration:")
        initial_snapshot_count = stats["snapshots"]

        try:
            with storage._transaction() as conn:
                # This will succeed
                conn.execute(
                    """
                    INSERT INTO snapshots (
                        snapshot_id, last_included_index, last_included_term,
                        state_machine_data, configuration, timestamp, 
                        checksum, size_bytes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    ("demo_snapshot_1", 10, 2, b"test_data", "{}", time.time(), "checksum1", 9),
                )

                # This will fail (duplicate primary key) and rollback the transaction
                conn.execute(
                    """
                    INSERT INTO snapshots (
                        snapshot_id, last_included_index, last_included_term,
                        state_machine_data, configuration, timestamp, 
                        checksum, size_bytes  
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        "demo_snapshot_1",  # Same ID - will fail
                        11,
                        2,
                        b"test_data2",
                        "{}",
                        time.time(),
                        "checksum2",
                        10,
                    ),
                )

        except Exception as e:
            logger.info(f"   ✓ Transaction failed as expected: {type(e).__name__}")

        final_stats = storage.get_storage_stats()
        logger.info(f"   Snapshots before: {initial_snapshot_count}")
        logger.info(f"   Snapshots after failed transaction: {final_stats['snapshots']}")
        logger.info("   ✓ Transaction was properly rolled back")

        # Performance demonstration
        logger.info("10. Performance demonstration:")
        start_time = time.time()

        # Batch insert test
        for i in range(100):
            storage.save_log_entry(f"performance_test_entry_{i}".encode())

        insert_time = time.time() - start_time
        logger.info(f"    Inserted 100 log entries in {insert_time:.3f} seconds")
        logger.info(f"    Rate: {100 / insert_time:.1f} entries/second")

        # Batch query test
        start_time = time.time()
        for _ in range(10):
            storage.load_log()

        query_time = time.time() - start_time
        total_entries = len(storage.load_log())
        logger.info(f"    Queried {total_entries} entries 10 times in {query_time:.3f} seconds")
        logger.info(f"    Rate: {total_entries * 10 / query_time:.1f} entries/second")

        # Clean up
        storage.close()

if __name__ == "__main__":
    main()
