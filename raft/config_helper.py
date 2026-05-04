"""Configuration helper for creating Raft components based on config files."""

import configparser
import logging

from raft.io.storage_factory import StorageFactory
from raft.models.config import Config
from raft.models.state_machine_factory import StateMachineFactory

logger = logging.getLogger(__name__)


def load_config_from_file(config_file: str = "raft.ini") -> Config:
    """Load Raft configuration from an INI file.

    Args:
        config_file: Path to the configuration file

    Returns:
        Config: The loaded configuration object

    Raises:
        FileNotFoundError: If the config file doesn't exist
        ValueError: If the configuration is invalid
    """
    conf = configparser.ConfigParser()
    conf.read(config_file)

    if not conf.sections():
        raise FileNotFoundError(f"Configuration file '{config_file}' not found or empty")

    config = Config(conf)

    # Validate the configuration
    StorageFactory.validate_storage_config(config)
    StateMachineFactory.validate_state_machine_config(config)

    logger.info(f"Loaded configuration from {config_file}")
    logger.info(f"Storage: {config.storage_class}, State Machine: {config.state_machine_class}")
    logger.info(
        f"Snapshot threshold: {config.snapshot_threshold}, Max snapshots: {config.max_snapshots_to_keep}"
    )

    return config


def create_raft_components(node_id: int, config: Config):
    """Create storage and state machine components for a Raft node.

    Args:
        node_id: The ID of this Raft node
        config: The Raft configuration object

    Returns:
        tuple: (storage, state_machine) components
    """
    # Create storage backend
    storage = StorageFactory.create_storage(node_id, config)

    # Create state machine
    state_machine = StateMachineFactory.create_state_machine(config)

    logger.info(
        f"Created components for node {node_id}: "
        f"storage={type(storage).__name__}, "
        f"state_machine={type(state_machine).__name__}"
    )

    return storage, state_machine


def print_available_components():
    """Log available storage and state machine classes."""
    logger.info("Available Storage Classes:")
    for storage_class in StorageFactory.get_available_storage_classes():
        logger.info(f"  - {storage_class}")

    logger.info("Available State Machine Classes:")
    for sm_class in StateMachineFactory.get_available_state_machine_classes():
        logger.info(f"  - {sm_class}")


if __name__ == "__main__":
    # Example usage
    logger.info("Raft Configuration Helper")
    logger.info("=" * 50)

    print_available_components()

    try:
        config = load_config_from_file()
        storage, state_machine = create_raft_components(1, config)
        logger.info("Successfully created components:")
        logger.info(f"  Storage: {type(storage).__name__}")
        logger.info(f"  State Machine: {type(state_machine).__name__}")
    except Exception as e:
        logger.error(f"Error: {e}")
