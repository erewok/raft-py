import json
import logging
import os
import sqlite3
import threading
import time
from abc import abstractmethod
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Any

import trio

from raft.models.config import Config
from raft.models.snapshot import Snapshot, SnapshotMetadata

logger = logging.getLogger("raft.io.storage")


def get_migration_files():
    """Get migration SQL files from the migrations directory."""
    # Always use the migrations directory relative to the project root
    migrations_dir = Path(__file__).parent.parent.parent / "migrations"
    if migrations_dir.exists():
        return sorted(migrations_dir.glob("*.sql"))
    else:
        raise RuntimeError(f"Migrations directory not found at {migrations_dir}")


sql_files = get_migration_files()


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
            up_to_index: 0-based index. Entries at positions 0..up_to_index
                         (inclusive) are removed. Callers must pass a 0-based
                         index matching the Log class convention (last_applied,
                         last_included_index) — NOT a 1-based Raft log index.

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
        """Remove log entries 0..up_to_index (inclusive). up_to_index is 0-based."""
        if up_to_index < 0 or up_to_index >= len(self.log):
            return 0

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
        dirs = sorted(os.listdir(self.data_filepath))
        if not dirs:
            self.stored_item_count = 0
            return
        maxdir = max(dirs)
        maxdir_abspath = os.path.join(self.data_filepath, maxdir)
        files = sorted(os.listdir(maxdir_abspath))
        if not files:
            self.stored_item_count = 0
            return
        max_file = max(files)
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
        """Remove log entries 0..up_to_index (inclusive). up_to_index is 0-based.

        Reads all entries from the hierarchical file structure, keeps only
        entries after up_to_index, rewrites the files, and updates the
        stored_item_count counter.
        """
        if up_to_index < 0:
            return 0

        # Collect all log entry files
        all_entries: list[bytes] = []
        files_to_remove: list[str] = []

        data_dir = self.data_filepath
        if not os.path.exists(data_dir):
            return 0

        for dirname in sorted(os.listdir(data_dir)):
            dirpath = os.path.join(data_dir, dirname)
            if not os.path.isdir(dirpath):
                continue
            for filename in sorted(os.listdir(dirpath)):
                filepath = os.path.join(dirpath, filename)
                if os.path.isfile(filepath):
                    files_to_remove.append(filepath)
                    with open(filepath, "rb") as f:
                        for line in f:
                            stripped = line.strip()
                            if stripped:
                                all_entries.append(stripped)

        # Keep entries after up_to_index (0-based index)
        entries_to_keep = all_entries[up_to_index + 1 :]
        removed_count = len(all_entries) - len(entries_to_keep)

        if removed_count <= 0:
            return 0

        # Remove old files
        for filepath in files_to_remove:
            try:
                os.remove(filepath)
            except OSError:
                pass

        # Rewrite kept entries into new files
        if entries_to_keep:
            for entry in entries_to_keep:
                self.save_log_entry(entry)
        else:
            # Reset counter if all entries were compacted
            self.stored_item_count = 0

        logger.info(
            f"Compacted FileStorage log: removed {removed_count} entries, {len(entries_to_keep)} remaining"
        )
        return removed_count


class SqliteStorage(BaseStorage):
    """
    SQLite storage backend with ACID transactions,
    connection pooling, and optimized queries for Raft operations.
    """

    def __init__(self, node_id: int, config: Config):
        self.node_id = node_id
        self.data_directory = config.data_directory
        self.db_path = os.path.join(self.data_directory, f"node_{node_id}.db")
        try:
            self.node_label = config.node_mapping[node_id]["label"]
        except (KeyError, Exception):
            self.node_label = f"node_{node_id}"

        # Ensure data directory exists
        os.makedirs(self.data_directory, exist_ok=True)

        # Thread-local storage for connections to ensure thread safety
        self._local = threading.local()

        # Initialize database schema
        self.init_database()

        logger.info(f"Initialized SQLite storage for node {node_id} at {self.db_path}")

    def _get_connection(self) -> sqlite3.Connection:
        """Get a thread-local database connection with optimized settings."""
        if not hasattr(self._local, "connection") or self._local.connection is None:
            conn = sqlite3.connect(
                self.db_path,
                isolation_level=None,  # Autocommit mode for explicit transaction control
                timeout=30.0,  # 30 second timeout
                check_same_thread=False,
            )

            # Enable WAL mode for better concurrent access
            conn.execute("PRAGMA journal_mode=WAL")
            # Optimize for performance
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
            conn.execute("PRAGMA temp_store=MEMORY")
            # Enable foreign key constraints
            conn.execute("PRAGMA foreign_keys=ON")

            # Custom row factory for easier data access
            conn.row_factory = sqlite3.Row

            self._local.connection = conn

        return self._local.connection

    @contextmanager
    def _transaction(self):
        """Context manager for database transactions with proper error handling."""
        conn = self._get_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def init_database(self):
        """Initialize database schema with proper indexes and constraints."""
        migration_files = get_migration_files()
        if not migration_files:
            raise RuntimeError("Missing SQL Migrations; cannot proceed with DB init")

        # executescript handles its own transactions, so don't use _transaction() here
        conn = self._get_connection()
        for fl in migration_files:
            sql_content = fl.read_text().strip()
            conn.executescript(sql_content)

    def save_metadata(self, value: bytes):
        """Save Raft metadata (current term, voted for) with ACID guarantees."""
        with self._transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", (f"node_{self.node_id}", value)
            )
        logger.debug(f"Saved metadata for node {self.node_id}")

    def save_log_entry(self, entry: bytes):
        """Save a log entry with automatic indexing and deduplication."""
        try:
            term = json.loads(entry).get("term", 1)
        except (json.JSONDecodeError, AttributeError):
            logger.warning("save_log_entry: could not parse term from entry, defaulting to 1")
            term = 1
        with self._transaction() as conn:
            conn.execute(
                "INSERT INTO log_entries (entry_data, term) VALUES (?, ?)",
                (entry, term),
            )

    def load_log(self):
        """Load all log entries efficiently with single query."""
        conn = self._get_connection()
        cursor = conn.execute("SELECT entry_data FROM log_entries ORDER BY log_index")
        return [row["entry_data"] for row in cursor.fetchall()]

    def get_log_entry(self, index: int) -> bytes | None:
        """Get a specific log entry by index."""
        conn = self._get_connection()
        cursor = conn.execute("SELECT entry_data FROM log_entries WHERE log_index = ?", (index,))
        row = cursor.fetchone()
        return row["entry_data"] if row else None

    def get_log_entries_from(self, start_index: int) -> list[bytes]:
        """Get log entries starting from a specific index."""
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT entry_data FROM log_entries WHERE log_index >= ? ORDER BY log_index", (start_index,)
        )
        return [row["entry_data"] for row in cursor.fetchall()]

    def save_snapshot(self, snapshot: Snapshot) -> str:
        """Save a snapshot with ACID guarantees and integrity checking."""
        snapshot_id = f"sqlite_snapshot_{int(time.time() * 1000000)}_{snapshot.last_included_index}"

        with self._transaction() as conn:
            conn.execute(
                """
                INSERT INTO snapshots (
                    snapshot_id, last_included_index, last_included_term,
                    state_machine_data, configuration, timestamp, 
                    checksum, size_bytes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    snapshot_id,
                    snapshot.last_included_index,
                    snapshot.last_included_term,
                    snapshot.state_machine_data,
                    json.dumps(snapshot.configuration),
                    snapshot.timestamp,
                    snapshot.checksum,
                    len(snapshot.state_machine_data),
                ),
            )

        logger.info(f"Saved snapshot {snapshot_id} to SQLite database")
        return snapshot_id

    def load_snapshot(self, snapshot_id: str) -> Snapshot:
        """Load a snapshot by ID with integrity verification."""
        conn = self._get_connection()
        cursor = conn.execute("SELECT * FROM snapshots WHERE snapshot_id = ?", (snapshot_id,))
        row = cursor.fetchone()

        if not row:
            raise KeyError(f"Snapshot {snapshot_id} not found")

        snapshot = Snapshot(
            last_included_index=row["last_included_index"],
            last_included_term=row["last_included_term"],
            state_machine_data=row["state_machine_data"],
            configuration=json.loads(row["configuration"]),
            timestamp=row["timestamp"],
            checksum=row["checksum"],
        )

        # Verify integrity
        if not snapshot.verify_integrity():
            logger.warning(f"Snapshot {snapshot_id} failed integrity check")

        return snapshot

    def get_latest_snapshot_metadata(self) -> SnapshotMetadata | None:
        """Get metadata of the most recent snapshot efficiently."""
        conn = self._get_connection()
        cursor = conn.execute("""
            SELECT snapshot_id, last_included_index, last_included_term,
                   size_bytes, created_at
            FROM snapshots 
            ORDER BY created_at DESC 
            LIMIT 1
        """)
        row = cursor.fetchone()

        if not row:
            return None

        return SnapshotMetadata(
            snapshot_id=row["snapshot_id"],
            last_included_index=row["last_included_index"],
            last_included_term=row["last_included_term"],
            size_bytes=row["size_bytes"],
            created_at=row["created_at"],
            file_path=f"sqlite://{self.db_path}#{row['snapshot_id']}",
        )

    def list_snapshots(self) -> list[SnapshotMetadata]:
        """List all snapshots, ordered by creation time (newest first)."""
        conn = self._get_connection()
        cursor = conn.execute("""
            SELECT snapshot_id, last_included_index, last_included_term,
                   size_bytes, created_at
            FROM snapshots 
            ORDER BY created_at DESC
        """)

        snapshots = []
        for row in cursor.fetchall():
            snapshots.append(
                SnapshotMetadata(
                    snapshot_id=row["snapshot_id"],
                    last_included_index=row["last_included_index"],
                    last_included_term=row["last_included_term"],
                    size_bytes=row["size_bytes"],
                    created_at=row["created_at"],
                    file_path=f"sqlite://{self.db_path}#{row['snapshot_id']}",
                )
            )

        return snapshots

    def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot by ID with transaction safety."""
        with self._transaction() as conn:
            cursor = conn.execute("DELETE FROM snapshots WHERE snapshot_id = ?", (snapshot_id,))
            deleted = cursor.rowcount > 0

        if deleted:
            logger.info(f"Deleted snapshot {snapshot_id} from SQLite database")

        return deleted

    def delete_old_snapshots(self, keep_count: int = 3) -> int:
        """Delete old snapshots efficiently with single transaction."""
        with self._transaction() as conn:
            # Get snapshot IDs to delete (all except the newest keep_count)
            cursor = conn.execute(
                """
                SELECT snapshot_id FROM snapshots 
                ORDER BY created_at DESC 
                LIMIT -1 OFFSET ?
            """,
                (keep_count,),
            )

            to_delete = [row["snapshot_id"] for row in cursor.fetchall()]

            deleted_count = 0
            for snapshot_id in to_delete:
                cursor = conn.execute("DELETE FROM snapshots WHERE snapshot_id = ?", (snapshot_id,))
                if cursor.rowcount > 0:
                    deleted_count += 1

        if deleted_count > 0:
            logger.info(f"Deleted {deleted_count} old snapshots from SQLite database")

        return deleted_count

    def compact_log(self, up_to_index: int) -> int:
        """Delete all rows with log_index <= up_to_index. Note: SQLite log_index is 1-based
        (auto-increment), so callers passing 0-based indices will see an off-by-one offset.
        This is a pre-existing divergence from InMemoryStorage and FileStorage conventions."""
        with self._transaction() as conn:
            cursor = conn.execute("DELETE FROM log_entries WHERE log_index <= ?", (up_to_index,))
            deleted_count = cursor.rowcount

        # Optimize database after cleanup (VACUUM must be outside transaction)
        if deleted_count > 0:
            conn = self._get_connection()
            conn.execute("VACUUM")
            logger.info(f"Compacted {deleted_count} log entries up to index {up_to_index}")

        return deleted_count

    def get_storage_stats(self) -> dict[str, Any]:
        """Get storage statistics for monitoring and diagnostics."""
        conn = self._get_connection()

        cursor = conn.execute("SELECT COUNT(*) as count FROM log_entries")
        log_count = cursor.fetchone()["count"]

        cursor = conn.execute("SELECT COUNT(*) as count FROM snapshots")
        snapshot_count = cursor.fetchone()["count"]

        cursor = conn.execute(
            "SELECT COUNT(*) as count FROM metadata WHERE key = ?", (f"node_{self.node_id}",)
        )
        metadata_count = cursor.fetchone()["count"]

        cursor = conn.execute(
            "SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()"
        )
        db_size = cursor.fetchone()["size"]

        return {
            "log_entries_count": log_count,
            "snapshots_count": snapshot_count,
            "metadata_count": metadata_count,
            "node_id": self.node_id,
            "node_label": self.node_label,
            "database_size_bytes": db_size,
            "database_path": self.db_path,
        }

    def vacuum(self):
        """Reclaim unused space in the SQLite database."""
        conn = self._get_connection()
        conn.execute("VACUUM")
        conn.commit()
        logger.info(f"Vacuumed SQLite database for node {self.node_id}")

    def close(self):
        """Close database connections and clean up resources."""
        if hasattr(self._local, "connection") and self._local.connection:
            self._local.connection.close()
            self._local.connection = None
        logger.info(f"Closed SQLite storage for node {self.node_id}")

    def __del__(self):
        """Ensure connections are closed when storage is garbage collected."""
        try:
            self.close()
        except Exception:
            pass  # Ignore errors during cleanup


class AsyncSqliteStorage(SqliteStorage):
    """
    Async SQLite-based persistent storage for Raft implementation using trio.

    Provides async/await *wrapper* around SqliteStorage methods.
    Uses trio.to_thread for async database operations.
    """

    def __init__(self, node_id: int, config: Config):
        super().__init__(node_id, config)

    async def save_metadata(self, value: bytes):
        """Save metadata to the database."""
        return await trio.to_thread.run_sync(super().save_metadata, value)

    async def save_log_entry(self, entry):
        """Save a log entry to the database."""

        return await trio.to_thread.run_sync(super().save_log_entry, entry)

    async def save_snapshot(self, snapshot: Snapshot) -> str:
        """Save a snapshot and return a unique snapshot ID."""

        return await trio.to_thread.run_sync(super().save_snapshot, snapshot)

    async def load_snapshot(self, snapshot_id: str) -> Snapshot:
        """Load a snapshot by its ID."""

        return await trio.to_thread.run_sync(super().load_snapshot, snapshot_id)

    async def get_latest_snapshot_metadata(self) -> SnapshotMetadata | None:
        """Get metadata of the most recent snapshot."""
        return await trio.to_thread.run_sync(super().get_latest_snapshot_metadata)

    async def list_snapshots(self) -> list[SnapshotMetadata]:
        """List all available snapshots, ordered by creation time (newest first)."""

        return await trio.to_thread.run_sync(super().list_snapshots)

    async def delete_snapshot(self, snapshot_id: str) -> bool:
        """Delete a snapshot by its ID."""

        return await trio.to_thread.run_sync(super().delete_snapshot, snapshot_id)

    async def delete_old_snapshots(self, keep_count: int = 3) -> int:
        """Delete old snapshots, keeping only the most recent ones."""
        parent_method = partial(super().delete_old_snapshots, keep_count=keep_count)
        return await trio.to_thread.run_sync(parent_method)

    async def compact_log(self, up_to_index: int) -> int:
        """Remove log entries up to the specified index (inclusive)."""
        # SqliteStorage.compact_log already runs VACUUM after deletion,
        # so no need to call self.vacuum() separately
        return await trio.to_thread.run_sync(super().compact_log, up_to_index)

    async def vacuum(self):
        return await trio.to_thread.run_sync(super().vacuum)

    async def get_storage_stats(self) -> dict[str, Any]:
        """Get database statistics (async version)."""

        return await trio.to_thread.run_sync(super().get_storage_stats)


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
        return await trio.to_thread.run_sync(super().compact_log, up_to_index)
