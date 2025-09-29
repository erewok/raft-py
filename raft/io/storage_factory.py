"""Storage factory for creating storage backends based on configuration."""

import logging
import os

from raft.io.storage import AsyncFileStorage, BaseStorage, FileStorage, InMemoryStorage
from raft.models.config import Config

logger = logging.getLogger(__name__)


class StorageFactory:
    """Factory for creating storage backends based on configuration."""

    @staticmethod
    def create_storage(node_id: int, config: Config) -> BaseStorage:
        """Create a storage backend based on the configuration.

        Args:
            node_id: The node ID for this storage instance
            config: The Raft configuration object

        Returns:
            BaseStorage: An instance of the configured storage backend

        Raises:
            ValueError: If the storage class is not supported
            ImportError: If required dependencies are not available
        """
        storage_class = config.storage_class
        data_directory = config.data_directory

        # Ensure data directory exists
        if storage_class in ["FileStorage", "AsyncFileStorage", "RocksDBStorage"]:
            node_data_dir = os.path.join(data_directory, f"node_{node_id}")
            os.makedirs(node_data_dir, exist_ok=True)

        if storage_class == "InMemoryStorage":
            logger.info(f"Creating InMemoryStorage for node {node_id}")
            return InMemoryStorage(node_id, config)

        elif storage_class == "FileStorage":
            logger.info(f"Creating FileStorage for node {node_id} at {node_data_dir}")
            return FileStorage(node_id, config)

        elif storage_class == "AsyncFileStorage":
            logger.info(f"Creating AsyncFileStorage for node {node_id} at {node_data_dir}")
            return AsyncFileStorage(node_id, config)

        elif storage_class == "RocksDBStorage":
            logger.info(f"Creating RocksDBStorage for node {node_id} at {node_data_dir}")
            return StorageFactory._create_rocksdb_storage(node_id, config)

        else:
            raise ValueError(
                f"Unsupported storage class: {storage_class}. "
                f"Supported classes: InMemoryStorage, FileStorage, AsyncFileStorage, RocksDBStorage"
            )

    @staticmethod
    def _create_rocksdb_storage(node_id: int, config: Config) -> BaseStorage:
        """Create a RocksDB storage backend.

        This is separated out to handle the optional dependency gracefully.
        """
        try:
            from raft.io.storage_rocksdb import RocksDBStorage

            return RocksDBStorage(node_id, config)
        except ImportError as e:
            raise ImportError(
                "RocksDBStorage requires the 'python-rocksdb' package. "
                "Install it with: pip install python-rocksdb"
            ) from e

    @staticmethod
    def get_available_storage_classes() -> list[str]:
        """Get a list of available storage classes."""
        available = ["InMemoryStorage", "FileStorage", "AsyncFileStorage"]

        # Check if RocksDB is available
        try:
            import rocksdb  # noqa: F401

            available.append("RocksDBStorage")
        except ImportError:
            pass

        return available

    @staticmethod
    def validate_storage_config(config: Config) -> None:
        """Validate that the storage configuration is valid.

        Args:
            config: The Raft configuration object

        Raises:
            ValueError: If the configuration is invalid
        """
        available_classes = StorageFactory.get_available_storage_classes()

        if config.storage_class not in available_classes:
            raise ValueError(
                f"Storage class '{config.storage_class}' is not available. "
                f"Available classes: {', '.join(available_classes)}"
            )

        # Validate data directory for persistent storage
        if config.storage_class in ["FileStorage", "AsyncFileStorage", "RocksDBStorage"]:
            if not config.data_directory:
                raise ValueError(f"DataDirectory must be specified for {config.storage_class}")

            # Try to create the directory to ensure it's writable
            try:
                os.makedirs(config.data_directory, exist_ok=True)
            except (OSError, PermissionError) as e:
                raise ValueError(
                    f"Cannot create or access data directory '{config.data_directory}': {e}"
                ) from e
