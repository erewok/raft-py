#!/usr/bin/env python3
"""Demonstration of Raft snapshot functionality with configurable backends."""

import logging

from raft.config_helper import create_raft_components, load_config_from_file

from raft.models.log import LogEntry
from raft.models.server import Leader

# Configure logging for the demo
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    """Demonstrate snapshot functionality with configurable backends."""
    logger.info("=== Raft Configurable Snapshot Demo ===")

    try:
        # Load configuration from raft.ini
        logger.info("1. Loading configuration from raft.ini...")
        config = load_config_from_file()
        logger.info(f"   Storage: {config.storage_class}")
        logger.info(f"   State Machine: {config.state_machine_class}")
        logger.info(f"   Snapshot Threshold: {config.snapshot_threshold}")

        # Create components based on configuration
        logger.info("2. Creating components for node 1...")
        storage, state_machine = create_raft_components(1, config)
        logger.info(f"   Created: {type(storage).__name__} and {type(state_machine).__name__}")

    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        logger.info("Falling back to default configuration...")
        # Fallback to manual configuration for demo
        from raft.io.storage import InMemoryStorage
        from raft.models.config import Config
        from raft.models.snapshot import KeyValueStateMachine

        storage = InMemoryStorage()
        state_machine = KeyValueStateMachine()
        config = Config(nodes=[1, 2, 3], log_compaction_threshold=3)

    leader = Leader(node_id=1, config=config, storage=storage, state_machine=state_machine)

    logger.info("1. Initial state:")
    logger.info(f"   Log entries: {len(leader.log.log)}")
    logger.info(f"   Snapshots: {len(storage.list_snapshots())}")
    logger.info(f"   State machine size: {state_machine.get_state_size()}")

    # Add some entries
    logger.info("2. Adding log entries and applying them:")
    import json
    from raft.models.snapshot import KeyValueStateMachine
    entries = [
        LogEntry(term=1, data=json.dumps({"op": "set", "key": "user1", "value": "Alice"}).encode()),
        LogEntry(term=1, data=json.dumps({"op": "set", "key": "user2", "value": "Bob"}).encode()),
        LogEntry(term=1, data=json.dumps({"op": "set", "key": "user3", "value": "Charlie"}).encode()),
        LogEntry(term=1, data=json.dumps({"op": "set", "key": "user4", "value": "David"}).encode()),
        LogEntry(term=1, data=json.dumps({"op": "set", "key": "user5", "value": "Eve"}).encode()),
    ]

    for i, entry in enumerate(entries):
        prev_index = len(leader.log.log) - 1
        prev_term = leader.log.log[prev_index].term if prev_index >= 0 else -1
        leader.log.append_entries(prev_index=prev_index, prev_term=prev_term, entries=[entry])
        leader.commit_index = i
        leader._apply_committed_entries()
        logger.info(f"   Applied entry {i + 1}: {json.loads(entry.command.decode())}")

    logger.info(f"   Log entries: {len(leader.log.log)}")
    logger.info(f"   State machine contains: {list(state_machine.data.keys())}")

    # Check if we should create snapshot
    logger.info("3. Checking if snapshot should be created:")
    should_snapshot = leader.should_create_snapshot()
    logger.info(f"   Should create snapshot: {should_snapshot}")
    logger.info(f"   Log size: {len(leader.log.log)}")
    logger.info(f"   Snapshot threshold: {leader.snapshot_threshold}")

    # Force snapshot creation for demo purposes by lowering threshold
    logger.info("4. Forcing snapshot creation for demo:")
    original_threshold = leader.snapshot_threshold
    leader.snapshot_threshold = 3  # Lower threshold for demo

    snapshot_id = leader.create_snapshot()
    if snapshot_id:
        logger.info(f"   Created snapshot: {snapshot_id}")

        # Show snapshot details
        snapshot = storage.load_snapshot(snapshot_id)
        logger.info(f"   Snapshot last included index: {snapshot.last_included_index}")
        logger.info(f"   Snapshot last included term: {snapshot.last_included_term}")
        logger.info(f"   Data integrity verified: {snapshot.verify_integrity()}")
    else:
        logger.info("   Failed to create snapshot")

    # Restore original threshold
    leader.snapshot_threshold = original_threshold

    # Demonstrate log compaction
    logger.info("5. Log compaction simulation:")
    logger.info(f"   Before compaction - Log entries: {len(leader.log.log)}")

    # Simulate compacting log (keeping last 2 entries)
    keep_entries = 2
    compact_before_index = leader.last_applied - keep_entries
    if compact_before_index > 0:
        compacted_count = storage.compact_log(compact_before_index)
        logger.info(f"   After compaction - Log entries: {len(leader.log.log)}")
        logger.info(f"   Compacted {compacted_count} entries before index: {compact_before_index}")

    # Demonstrate restoration
    logger.info("6. State restoration from snapshot:")
    new_state_machine = KeyValueStateMachine()
    logger.info(f"   New state machine before restore: {new_state_machine.get_state_size()} items")

    # Restore from latest snapshot
    latest_snapshot_meta = storage.get_latest_snapshot_metadata()
    if latest_snapshot_meta:
        snapshot = storage.load_snapshot(latest_snapshot_meta.snapshot_id)
        new_state_machine.restore_from_snapshot(snapshot.state_machine_data)
        logger.info(f"   After restore: {new_state_machine.get_state_size()} items")
        logger.info(f"   Restored data: {list(new_state_machine.data.keys())}")

        # Verify data integrity
        for key in ["user1", "user2", "user3", "user4", "user5"]:
            original_value = state_machine.get(key)
            restored_value = new_state_machine.get(key)
            status = "✓" if original_value == restored_value else "✗"
            logger.info(f"   {key}: {original_value} -> {restored_value} ({status})")
    else:
        logger.info("   No snapshots available for restoration")

    logger.info("=== Demo Complete ===")


if __name__ == "__main__":
    main()
