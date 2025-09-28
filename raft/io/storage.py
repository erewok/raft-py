import json
import logging
import os
import time
from abc import abstractmethod
from typing import Any

import trio

from raft.models.config import Config
from raft.models.snapshot import Snapshot, SnapshotMetadata

logger = logging.getLogger("raft.io.storage")


class BaseStorage:
    @abstractmethod
    def save_metadata(self, value: bytes):
        raise NotImplementedError("Implement `save_metadata`")

    @abstractmethod
    def save_log_entry(self, entry):
        raise NotImplementedError("Implement `save_log_entry`")

    # Snapshot-related methods
    @abstractmethod
    def save_snapshot(self, snapshot: Snapshot) -> str:
        """
        Save a snapshot and return a unique snapshot ID.

        Args:
            snapshot: The snapshot to save

        Returns:
            A unique identifier for the saved snapshot
        """
        raise NotImplementedError("Implement `save_snapshot`")

    @abstractmethod
    def load_snapshot(self, snapshot_id: str) -> Snapshot:
        """
        Load a snapshot by its ID.

        Args:
            snapshot_id: The unique identifier of the snapshot to load

        Returns:
            The loaded snapshot

        Raises:
            KeyError: If snapshot with given ID doesn't exist
        """
        raise NotImplementedError("Implement `load_snapshot`")

    @abstractmethod
    def get_latest_snapshot_metadata(self) -> SnapshotMetadata | None:
        """
        Get metadata of the most recent snapshot.

        Returns:
            Metadata of the latest snapshot, or None if no snapshots exist
        """
        raise NotImplementedError("Implement `get_latest_snapshot_metadata`")

    @abstractmethod
    def list_snapshots(self) -> list[SnapshotMetadata]:
        """
        List all available snapshots, ordered by creation time (newest first).

        Returns:
            List of snapshot metadata, newest first
        """
        raise NotImplementedError("Implement `list_snapshots`")

    @abstractmethod
    def delete_snapshot(self, snapshot_id: str) -> bool:
        """
        Delete a snapshot by its ID.

        Args:
            snapshot_id: The unique identifier of the snapshot to delete

        Returns:
            True if snapshot was deleted, False if it didn't exist
        """
        raise NotImplementedError("Implement `delete_snapshot`")

    @abstractmethod
    def delete_old_snapshots(self, keep_count: int = 3) -> int:
        """
        Delete old snapshots, keeping only the most recent ones.

        Args:
            keep_count: Number of most recent snapshots to keep

        Returns:
            Number of snapshots deleted
        """
        raise NotImplementedError("Implement `delete_old_snapshots`")

    @abstractmethod
    def compact_log(self, up_to_index: int) -> int:
        """
        Remove log entries up to the specified index (inclusive).
        This is called after creating a snapshot to free up space.

        Args:
            up_to_index: Remove log entries up to and including this index

        Returns:
            Number of log entries removed
        """
        raise NotImplementedError("Implement `compact_log`")


class InMemoryStorage(BaseStorage):
    def __init__(self, node_id: int, _: Config):
        self.log: list[bytes] = []
        self.metadata: dict[str, Any] = {"node_id": node_id}
        self.snapshots: dict[str, Snapshot] = {}  # snapshot_id -> Snapshot
        self.snapshot_metadata: dict[str, SnapshotMetadata] = {}  # snapshot_id -> SnapshotMetadata

    def save_metadata(self, value: bytes):
        self.metadata["stored"] = value
        self.metadata["updated"] = time.time()

    def save_log_entry(self, entry):
        self.log.append(entry)

    def save_snapshot(self, snapshot: Snapshot) -> str:
        """Save a snapshot in memory and return a unique snapshot ID"""
        snapshot_id = f"mem_snapshot_{int(time.time() * 1000000)}_{snapshot.last_included_index}"

        # Store the snapshot
        self.snapshots[snapshot_id] = snapshot

        # Create and store metadata
        metadata = SnapshotMetadata(
            snapshot_id=snapshot_id,
            last_included_index=snapshot.last_included_index,
            last_included_term=snapshot.last_included_term,
            size_bytes=len(snapshot.state_machine_data),
            created_at=snapshot.timestamp,
            file_path=f"memory://{snapshot_id}",
        )
        self.snapshot_metadata[snapshot_id] = metadata

        logger.info(f"Saved snapshot {snapshot_id} in memory")
        return snapshot_id

    def load_snapshot(self, snapshot_id: str) -> Snapshot:
        """Load a snapshot from memory by ID"""
        if snapshot_id not in self.snapshots:
            raise KeyError(f"Snapshot {snapshot_id} not found")
        return self.snapshots[snapshot_id]

    def get_latest_snapshot_metadata(self) -> SnapshotMetadata | None:
        """Get metadata of the most recent snapshot"""
        if not self.snapshot_metadata:
            return None

        # Sort by creation time, get the latest
        latest = max(self.snapshot_metadata.values(), key=lambda m: m.created_at)
        return latest

    def list_snapshots(self) -> list[SnapshotMetadata]:
        """List all snapshots, ordered by creation time (newest first)"""
        return sorted(self.snapshot_metadata.values(), key=lambda m: m.created_at, reverse=True)

    def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot by ID"""
        if snapshot_id not in self.snapshots:
            return False

        del self.snapshots[snapshot_id]
        del self.snapshot_metadata[snapshot_id]
        logger.info(f"Deleted snapshot {snapshot_id} from memory")
        return True

    def delete_old_snapshots(self, keep_count: int = 3) -> int:
        """Delete old snapshots, keeping only the most recent ones"""
        snapshots_by_time = self.list_snapshots()  # Already sorted newest first

        if len(snapshots_by_time) <= keep_count:
            return 0  # Nothing to delete

        # Delete the oldest snapshots
        to_delete = snapshots_by_time[keep_count:]
        deleted_count = 0

        for metadata in to_delete:
            if self.delete_snapshot(metadata.snapshot_id):
                deleted_count += 1

        logger.info(f"Deleted {deleted_count} old snapshots from memory")
        return deleted_count

    def compact_log(self, up_to_index: int) -> int:
        """Remove log entries up to the specified index (inclusive)"""
        if up_to_index < 0 or up_to_index >= len(self.log):
            return 0

        # Remove entries from index 0 to up_to_index (inclusive)
        entries_to_remove = up_to_index + 1
        self.log = self.log[entries_to_remove:]

        logger.info(f"Compacted {entries_to_remove} log entries from memory")
        return entries_to_remove


class FileStorage(BaseStorage):
    def __init__(self, node_id: int, config: Config):
        self.data_directory = config.data_directory
        self.node_label = config.node_mapping[node_id]["label"]

        self.stored_item_count = 0
        self.metadata_filepath = os.path.join(self.data_directory, "metadata")
        self.storage_directory = os.path.join(self.data_directory, self.node_label)
        self.data_filepath = os.path.join(self.storage_directory, "data")
        # Make sure these directories exist
        os.makedirs(self.storage_directory, exist_ok=True)
        os.makedirs(self.data_filepath, exist_ok=True)

        self.set_stored_item_count()

    def set_stored_item_count(self):
        maxdir = max(sorted(os.listdir(self.data_filepath)))
        maxdir_abspath = os.path.join(self.data_filepath, maxdir)
        max_file = max(sorted(os.listdir(maxdir_abspath)))
        max_file_abspath = os.path.join(maxdir_abspath, max_file)
        with open(max_file_abspath, "rb") as fl:
            line_count = sum(1 for _ in fl)

        self.stored_item_count = int(f"{maxdir}{max_file}{line_count:03}")

    @property
    def data_storage_filepath(self):
        """
        Break up the item count into:
           dir -> filename -> line in file

        Thus, item 100_456000_000 goes in:
           dir: "100"
           file: "456"
           line: 000
        """
        item_count = f"{self.stored_item_count:012}"
        dirname = item_count[:3]
        filename = item_count[3:9]
        return os.path.join(self.data_filepath, dirname, filename)

    def save_metadata(self, value: bytes):
        with open(self.metadata_filepath, "wb") as fl:
            fl.write(value)

    def save_log_entry(self, entry: bytes):
        self.stored_item_count += 1
        with open(self.data_storage_filepath, "ab") as fl:
            fl.write(entry)

    @property
    def snapshots_directory(self):
        """Directory where snapshots are stored"""
        snapshots_dir = os.path.join(self.storage_directory, "snapshots")
        os.makedirs(snapshots_dir, exist_ok=True)
        return snapshots_dir

    def save_snapshot(self, snapshot: Snapshot) -> str:
        """Save a snapshot to disk and return a unique snapshot ID"""
        snapshot_id = f"file_snapshot_{int(time.time() * 1000000)}_{snapshot.last_included_index}"

        # Save snapshot data
        snapshot_file = os.path.join(self.snapshots_directory, f"{snapshot_id}.snapshot")
        with open(snapshot_file, "wb") as f:
            snapshot_dict = snapshot.to_dict()
            f.write(json.dumps(snapshot_dict).encode())

        # Save metadata
        metadata = SnapshotMetadata(
            snapshot_id=snapshot_id,
            last_included_index=snapshot.last_included_index,
            last_included_term=snapshot.last_included_term,
            size_bytes=len(snapshot.state_machine_data),
            created_at=snapshot.timestamp,
            file_path=snapshot_file,
        )

        metadata_file = os.path.join(self.snapshots_directory, f"{snapshot_id}.metadata")
        with open(metadata_file, "w") as f:
            json.dump(metadata.to_dict(), f, indent=2)

        logger.info(f"Saved snapshot {snapshot_id} to disk at {snapshot_file}")
        return snapshot_id

    def load_snapshot(self, snapshot_id: str) -> Snapshot:
        """Load a snapshot from disk by ID"""
        snapshot_file = os.path.join(self.snapshots_directory, f"{snapshot_id}.snapshot")

        if not os.path.exists(snapshot_file):
            raise KeyError(f"Snapshot {snapshot_id} not found")

        with open(snapshot_file, "rb") as f:
            snapshot_dict = json.loads(f.read().decode())
            return Snapshot.from_dict(snapshot_dict)

    def _load_snapshot_metadata(self, snapshot_id: str) -> SnapshotMetadata | None:
        """Load snapshot metadata from disk"""
        metadata_file = os.path.join(self.snapshots_directory, f"{snapshot_id}.metadata")

        if not os.path.exists(metadata_file):
            return None

        with open(metadata_file) as f:
            metadata_dict = json.load(f)
            return SnapshotMetadata.from_dict(metadata_dict)

    def get_latest_snapshot_metadata(self) -> SnapshotMetadata | None:
        """Get metadata of the most recent snapshot"""
        if not os.path.exists(self.snapshots_directory):
            return None

        metadata_files = [f for f in os.listdir(self.snapshots_directory) if f.endswith(".metadata")]
        if not metadata_files:
            return None

        latest_metadata = None
        latest_time = 0

        for metadata_file in metadata_files:
            snapshot_id = metadata_file.replace(".metadata", "")
            metadata = self._load_snapshot_metadata(snapshot_id)
            if metadata and metadata.created_at > latest_time:
                latest_time = metadata.created_at
                latest_metadata = metadata

        return latest_metadata

    def list_snapshots(self) -> list[SnapshotMetadata]:
        """List all snapshots, ordered by creation time (newest first)"""
        if not os.path.exists(self.snapshots_directory):
            return []

        metadata_files = [f for f in os.listdir(self.snapshots_directory) if f.endswith(".metadata")]
        snapshots = []

        for metadata_file in metadata_files:
            snapshot_id = metadata_file.replace(".metadata", "")
            metadata = self._load_snapshot_metadata(snapshot_id)
            if metadata:
                snapshots.append(metadata)

        return sorted(snapshots, key=lambda m: m.created_at, reverse=True)

    def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot by ID"""
        snapshot_file = os.path.join(self.snapshots_directory, f"{snapshot_id}.snapshot")
        metadata_file = os.path.join(self.snapshots_directory, f"{snapshot_id}.metadata")

        deleted = False

        if os.path.exists(snapshot_file):
            os.remove(snapshot_file)
            deleted = True

        if os.path.exists(metadata_file):
            os.remove(metadata_file)
            deleted = True

        if deleted:
            logger.info(f"Deleted snapshot {snapshot_id} from disk")

        return deleted

    def delete_old_snapshots(self, keep_count: int = 3) -> int:
        """Delete old snapshots, keeping only the most recent ones"""
        snapshots_by_time = self.list_snapshots()  # Already sorted newest first

        if len(snapshots_by_time) <= keep_count:
            return 0  # Nothing to delete

        # Delete the oldest snapshots
        to_delete = snapshots_by_time[keep_count:]
        deleted_count = 0

        for metadata in to_delete:
            if self.delete_snapshot(metadata.snapshot_id):
                deleted_count += 1

        logger.info(f"Deleted {deleted_count} old snapshots from disk")
        return deleted_count

    def compact_log(self, up_to_index: int) -> int:
        """Remove log entries up to the specified index (inclusive)"""
        # This is a simplified implementation - in a real system you'd want
        # more sophisticated log management
        # For now, we'll just log the operation since the existing log structure
        # is complex and would need major refactoring
        logger.info(f"Log compaction requested up to index {up_to_index}")
        # TODO: Implement actual log compaction for FileStorage
        return 0


class AsyncFileStorage(BaseStorage):
    def __init__(self, node_id: int, config: Config):
        self.data_directory = config.data_directory
        self.node_label = config.node_mapping[node_id]["label"]

        self.stored_item_count = 0
        self.metadata_filepath = os.path.join(self.data_directory, "metadata")
        self.storage_directory = os.path.join(self.data_directory, self.node_label)
        self.data_filepath = os.path.join(self.storage_directory, "data")
        # Make sure these directories exist
        os.makedirs(self.storage_directory, exist_ok=True)
        os.makedirs(self.data_filepath, exist_ok=True)

    async def set_stored_item_count(self):
        maxdir = max(sorted(os.listdir(self.data_filepath)))
        maxdir_abspath = os.path.join(self.data_filepath, maxdir)
        max_file = max(sorted(os.listdir(maxdir_abspath)))
        max_file_abspath = os.path.join(maxdir_abspath, max_file)
        async with await trio.open_file(max_file_abspath, "rb") as fl:
            line_count = 0
            async for _ in fl:
                line_count += 1

        self.stored_item_count = int(f"{maxdir}{max_file}{line_count:03}")

    @property
    def data_storage_filepath(self):
        """
        Break up the item count into:
           dir -> filename -> line in file

        Thus, item 100_456000_000 goes in:
           dir: "100"
           file: "456"
           line: 000
        """
        item_count = f"{self.stored_item_count:012}"
        dirname = item_count[:3]
        filename = item_count[3:9]
        return os.path.join(self.data_filepath, dirname, filename)

    async def save_metadata(self, value: bytes):
        async with await trio.open_file(self.metadata_filepath, "wb") as fl:
            await fl.write(value)

    async def save_log_entry(self, entry: bytes):
        self.stored_item_count += 1
        async with await trio.open_file(self.data_storage_filepath, "ab") as fl:
            await fl.write(entry)

    @property
    def snapshots_directory(self):
        """Directory where snapshots are stored"""
        snapshots_dir = os.path.join(self.storage_directory, "snapshots")
        os.makedirs(snapshots_dir, exist_ok=True)
        return snapshots_dir

    async def save_snapshot(self, snapshot: Snapshot) -> str:
        """Save a snapshot to disk asynchronously and return a unique snapshot ID"""
        snapshot_id = f"async_file_snapshot_{int(time.time() * 1000000)}_{snapshot.last_included_index}"

        # Save snapshot data
        snapshot_file = os.path.join(self.snapshots_directory, f"{snapshot_id}.snapshot")
        async with await trio.open_file(snapshot_file, "wb") as f:
            snapshot_dict = snapshot.to_dict()
            await f.write(json.dumps(snapshot_dict).encode())

        # Save metadata
        metadata = SnapshotMetadata(
            snapshot_id=snapshot_id,
            last_included_index=snapshot.last_included_index,
            last_included_term=snapshot.last_included_term,
            size_bytes=len(snapshot.state_machine_data),
            created_at=snapshot.timestamp,
            file_path=snapshot_file,
        )

        metadata_file = os.path.join(self.snapshots_directory, f"{snapshot_id}.metadata")
        async with await trio.open_file(metadata_file, "w") as f:
            await f.write(json.dumps(metadata.to_dict(), indent=2))

        logger.info(f"Saved snapshot {snapshot_id} to disk at {snapshot_file}")
        return snapshot_id

    async def load_snapshot(self, snapshot_id: str) -> Snapshot:
        """Load a snapshot from disk asynchronously by ID"""
        snapshot_file = os.path.join(self.snapshots_directory, f"{snapshot_id}.snapshot")

        if not os.path.exists(snapshot_file):
            raise KeyError(f"Snapshot {snapshot_id} not found")

        async with await trio.open_file(snapshot_file, "rb") as f:
            content = await f.read()
            snapshot_dict = json.loads(content.decode())
            return Snapshot.from_dict(snapshot_dict)

    async def _load_snapshot_metadata(self, snapshot_id: str) -> SnapshotMetadata | None:
        """Load snapshot metadata from disk asynchronously"""
        metadata_file = os.path.join(self.snapshots_directory, f"{snapshot_id}.metadata")

        if not os.path.exists(metadata_file):
            return None

        async with await trio.open_file(metadata_file) as f:
            content = await f.read()
            metadata_dict = json.loads(content)
            return SnapshotMetadata.from_dict(metadata_dict)

    async def get_latest_snapshot_metadata(self) -> SnapshotMetadata | None:
        """Get metadata of the most recent snapshot asynchronously"""
        if not os.path.exists(self.snapshots_directory):
            return None

        metadata_files = [f for f in os.listdir(self.snapshots_directory) if f.endswith(".metadata")]
        if not metadata_files:
            return None

        latest_metadata = None
        latest_time = 0

        for metadata_file in metadata_files:
            snapshot_id = metadata_file.replace(".metadata", "")
            metadata = await self._load_snapshot_metadata(snapshot_id)
            if metadata and metadata.created_at > latest_time:
                latest_time = metadata.created_at
                latest_metadata = metadata

        return latest_metadata

    async def list_snapshots(self) -> list[SnapshotMetadata]:
        """List all snapshots asynchronously, ordered by creation time (newest first)"""
        if not os.path.exists(self.snapshots_directory):
            return []

        metadata_files = [f for f in os.listdir(self.snapshots_directory) if f.endswith(".metadata")]
        snapshots = []

        for metadata_file in metadata_files:
            snapshot_id = metadata_file.replace(".metadata", "")
            metadata = await self._load_snapshot_metadata(snapshot_id)
            if metadata:
                snapshots.append(metadata)

        return sorted(snapshots, key=lambda m: m.created_at, reverse=True)

    async def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot by ID asynchronously"""
        snapshot_file = os.path.join(self.snapshots_directory, f"{snapshot_id}.snapshot")
        metadata_file = os.path.join(self.snapshots_directory, f"{snapshot_id}.metadata")

        deleted = False

        if os.path.exists(snapshot_file):
            os.remove(snapshot_file)
            deleted = True

        if os.path.exists(metadata_file):
            os.remove(metadata_file)
            deleted = True

        if deleted:
            logger.info(f"Deleted snapshot {snapshot_id} from disk")

        return deleted

    async def delete_old_snapshots(self, keep_count: int = 3) -> int:
        """Delete old snapshots asynchronously, keeping only the most recent ones"""
        snapshots_by_time = await self.list_snapshots()  # Already sorted newest first

        if len(snapshots_by_time) <= keep_count:
            return 0  # Nothing to delete

        # Delete the oldest snapshots
        to_delete = snapshots_by_time[keep_count:]
        deleted_count = 0

        for metadata in to_delete:
            if await self.delete_snapshot(metadata.snapshot_id):
                deleted_count += 1

        logger.info(f"Deleted {deleted_count} old snapshots from disk")
        return deleted_count

    async def compact_log(self, up_to_index: int) -> int:
        """Remove log entries up to the specified index (inclusive) asynchronously"""
        # This is a simplified implementation - in a real system you'd want
        # more sophisticated log management
        # For now, we'll just log the operation since the existing log structure
        # is complex and would need major refactoring
        logger.info(f"Async log compaction requested up to index {up_to_index}")
        # TODO: Implement actual log compaction for AsyncFileStorage
        return 0
