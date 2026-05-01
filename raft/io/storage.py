import json
import logging
import os
import time
from abc import abstractmethod
from typing import Any

try:
    import trio
except ImportError:
    trio = None

from raft.models.config import Config

logger = logging.getLogger("raft.io.storage")


class BaseStorage:
    @abstractmethod
    def save_metadata(self, value: bytes):
        raise NotImplementedError("Implement `save_metadata`")

    @abstractmethod
    def save_log_entry(self, entry):
        raise NotImplementedError("Implement `save_log_entry`")

    def save_snapshot(self, snapshot_data: bytes, last_included_index: int, last_included_term: int):
        """Persist a snapshot. Default is a no-op for in-memory storage."""
        pass

    def load_snapshot(self) -> tuple[bytes, int, int] | None:
        """Load the latest snapshot. Returns (data, last_included_index, last_included_term) or None."""
        return None

    def clear_log(self):
        """Clear all log entries from storage."""
        pass


class InMemoryStorage(BaseStorage):
    def __init__(self, node_id: int, _: Config):
        self.log: list[bytes] = []
        self.metadata: dict[str, Any] = {"node_id": node_id}
        self.snapshot: tuple[bytes, int, int] | None = None

    def save_metadata(self, value: bytes):
        self.metadata["stored"] = value
        self.metadata["updated"] = time.time()

    def save_log_entry(self, entry):
        self.log.append(entry)

    def save_snapshot(self, snapshot_data: bytes, last_included_index: int, last_included_term: int):
        self.snapshot = (snapshot_data, last_included_index, last_included_term)

    def load_snapshot(self) -> tuple[bytes, int, int] | None:
        return self.snapshot

    def clear_log(self):
        self.log.clear()


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

    def save_snapshot(self, snapshot_data: bytes, last_included_index: int, last_included_term: int):
        snapshot_dir = os.path.join(self.storage_directory, "snapshots")
        os.makedirs(snapshot_dir, exist_ok=True)
        snapshot_file = os.path.join(snapshot_dir, f"{last_included_index:012}_{last_included_term:012}")
        with open(snapshot_file, "wb") as fl:
            fl.write(snapshot_data)
        # Persist metadata with snapshot info
        self.storage_metadata = {
            "last_included_index": last_included_index,
            "last_included_term": last_included_term,
        }
        with open(os.path.join(snapshot_dir, "latest"), "wb") as fl:
            fl.write(json.dumps(self.storage_metadata).encode())

    def load_snapshot(self) -> tuple[bytes, int, int] | None:
        snapshot_dir = os.path.join(self.storage_directory, "snapshots")
        if not os.path.exists(snapshot_dir):
            return None
        latest_path = os.path.join(snapshot_dir, "latest")
        if not os.path.exists(latest_path):
            return None
        with open(latest_path, "rb") as fl:
            meta = json.loads(fl.read().decode())
        last_idx = meta["last_included_index"]
        last_term = meta["last_included_term"]
        snapshot_file = os.path.join(snapshot_dir, f"{last_idx:012}_{last_term:012}")
        if not os.path.exists(snapshot_file):
            return None
        with open(snapshot_file, "rb") as fl:
            return (fl.read(), last_idx, last_term)

    def clear_log(self):
        # Remove all data files
        if os.path.exists(self.data_filepath):
            for root, dirs, files in os.walk(self.data_filepath):
                for f in files:
                    os.remove(os.path.join(root, f))


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

    async def save_snapshot(self, snapshot_data: bytes, last_included_index: int, last_included_term: int):
        snapshot_dir = os.path.join(self.storage_directory, "snapshots")
        os.makedirs(snapshot_dir, exist_ok=True)
        snapshot_file = os.path.join(snapshot_dir, f"{last_included_index:012}_{last_included_term:012}")
        async with await trio.open_file(snapshot_file, "wb") as fl:
            await fl.write(snapshot_data)
        self.storage_metadata = {
            "last_included_index": last_included_index,
            "last_included_term": last_included_term,
        }
        async with await trio.open_file(os.path.join(snapshot_dir, "latest"), "wb") as fl:
            await fl.write(json.dumps(self.storage_metadata).encode())

    async def load_snapshot(self) -> tuple[bytes, int, int] | None:
        snapshot_dir = os.path.join(self.storage_directory, "snapshots")
        if not os.path.exists(snapshot_dir):
            return None
        latest_path = os.path.join(snapshot_dir, "latest")
        if not os.path.exists(latest_path):
            return None
        async with await trio.open_file(latest_path, "rb") as fl:
            meta = json.loads(await fl.read())
        last_idx = meta["last_included_index"]
        last_term = meta["last_included_term"]
        snapshot_file = os.path.join(snapshot_dir, f"{last_idx:012}_{last_term:012}")
        if not os.path.exists(snapshot_file):
            return None
        async with await trio.open_file(snapshot_file, "rb") as fl:
            return (await fl.read(), last_idx, last_term)

    async def clear_log(self):
        if os.path.exists(self.data_filepath):
            for root, dirs, files in os.walk(self.data_filepath):
                for f in files:
                    os.remove(os.path.join(root, f))
