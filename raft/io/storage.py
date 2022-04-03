import logging
import os
import time
from typing import Any, Dict, List

from raft.models.helpers import Config


logger = logging.getLogger("raft.io.storage")


try:
    import trio
except ImportError:
    logger.info("Missing async features: install with `async` to enable")
    trio = None


class InMemoryStorage:
    def __init__(self, node_id: int, _: Config):
        self.log: List[bytes] = []
        self.metadata: Dict[str, Any] = {"node_id": node_id}

    def save_metadata(self, value: bytes):
        self.metadata["stored"] = value
        self.metadata["updated"] = time.time()

    def save_log_entry(self, entry):
        self.log.append(entry)


class FileStorage:
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
            line_count = sum((1 for _ in fl))

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


class AsyncFileStorage:
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
