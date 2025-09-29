#!/usr/bin/env python3
"""Demonstration of Raft snapshot functionality with configurable backends."""

from raft.config_helper import create_raft_components, load_config_from_file
from raft.models.log import LogEntry
from raft.models.server import Leader


def main():
    """Demonstrate snapshot functionality with configurable backends."""
    print("=== Raft Configurable Snapshot Demo ===\n")
    
    try:
        # Load configuration from raft.ini
        print("1. Loading configuration from raft.ini...")
        config = load_config_from_file()
        print(f"   Storage: {config.storage_class}")
        print(f"   State Machine: {config.state_machine_class}")
        print(f"   Snapshot Threshold: {config.snapshot_threshold}")
        
        # Create components based on configuration
        print("\n2. Creating components for node 1...")
        storage, state_machine = create_raft_components(1, config)
        print(f"   Created: {type(storage).__name__} and {type(state_machine).__name__}")
        
    except Exception as e:
        print(f"Error loading configuration: {e}")
        print("Falling back to default configuration...")
        # Fallback to manual configuration for demo
        from raft.io.storage import InMemoryStorage
        from raft.models.config import Config
        from raft.models.snapshot import KeyValueStateMachine
        
        storage = InMemoryStorage()
        state_machine = KeyValueStateMachine()
        config = Config(nodes=[1, 2, 3], log_compaction_threshold=3)    leader = Leader(node_id=1, config=config, storage=storage, state_machine=state_machine)

    print("1. Initial state:")
    print(f"   Log entries: {len(leader.log.log)}")
    print(f"   Snapshots: {len(storage.list_snapshots())}")
    print(f"   State machine size: {state_machine.get_state_size()}")

    # Add some entries
    print("\n2. Adding log entries and applying them:")
    entries = [
        LogEntry(term=1, command={"op": "set", "key": "user1", "value": "Alice"}),
        LogEntry(term=1, command={"op": "set", "key": "user2", "value": "Bob"}),
        LogEntry(term=1, command={"op": "set", "key": "user3", "value": "Charlie"}),
        LogEntry(term=1, command={"op": "set", "key": "user4", "value": "David"}),
        LogEntry(term=1, command={"op": "set", "key": "user5", "value": "Eve"}),
    ]

    for i, entry in enumerate(entries):
        leader.log.append_entries([entry])
        leader.commit_index = i
        leader._apply_committed_entries()
        print(f"   Applied entry {i + 1}: {entry.command}")

    print(f"\n   Log entries: {len(leader.log.log)}")
    print(f"   State machine contains: {list(state_machine.data.keys())}")

    # Check if we should create snapshot
    print("\n3. Checking if snapshot should be created:")
    should_snapshot = leader.should_create_snapshot()
    print(f"   Should create snapshot: {should_snapshot}")
    print(f"   Log size: {len(leader.log.log)}")
    print(f"   Snapshot threshold: {config.log_compaction_threshold}")

    # Create snapshot
    if should_snapshot:
        print("\n4. Creating snapshot:")
        snapshot_id = leader.create_snapshot()
        print(f"   Created snapshot: {snapshot_id}")

        # Show snapshot details
        snapshot = storage.load_snapshot(snapshot_id)
        print(f"   Snapshot metadata: {snapshot.metadata}")
        print(f"   Snapshot data: {snapshot.data}")
        print(f"   Data integrity verified: {snapshot.verify_integrity()}")

    # Demonstrate log compaction
    print("\n5. Log compaction simulation:")
    print(f"   Before compaction - Log entries: {len(leader.log.log)}")

    # Simulate compacting log (keeping last 2 entries)
    keep_entries = 2
    compact_before_index = leader.last_applied - keep_entries
    if compact_before_index > 0:
        storage.compact_log(compact_before_index)
        print(f"   After compaction - Log entries: {len(storage.load_log().log)}")
        print(f"   Compacted entries before index: {compact_before_index}")

    # Demonstrate restoration
    print("\n6. State restoration from snapshot:")
    new_state_machine = KeyValueStateMachine()
    print(f"   New state machine before restore: {new_state_machine.get_state_size()} items")

    # Restore from latest snapshot
    latest_snapshot_meta = storage.get_latest_snapshot_metadata()
    if latest_snapshot_meta:
        snapshot = storage.load_snapshot(latest_snapshot_meta.snapshot_id)
        new_state_machine.restore_snapshot(snapshot.data)
        print(f"   After restore: {new_state_machine.get_state_size()} items")
        print(f"   Restored data: {list(new_state_machine.data.keys())}")

        # Verify data integrity
        for key in ["user1", "user2", "user3", "user4", "user5"]:
            original_value = state_machine.get(key)
            restored_value = new_state_machine.get(key)
            status = "✓" if original_value == restored_value else "✗"
            print(f"   {key}: {original_value} -> {restored_value} ({status})")

    print("\n=== Demo Complete ===")


if __name__ == "__main__":
    main()
