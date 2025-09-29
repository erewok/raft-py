"""State machine factory for creating state machines based on configuration."""

import logging

from raft.models.config import Config
from raft.models.snapshot import KeyValueStateMachine, NoOpStateMachine, StateMachine

logger = logging.getLogger(__name__)


class StateMachineFactory:
    """Factory for creating state machines based on configuration."""

    @staticmethod
    def create_state_machine(config: Config) -> StateMachine:
        """Create a state machine based on the configuration.

        Args:
            config: The Raft configuration object

        Returns:
            StateMachine: An instance of the configured state machine

        Raises:
            ValueError: If the state machine class is not supported
        """
        state_machine_class = config.state_machine_class

        if state_machine_class == "NoOpStateMachine":
            logger.info("Creating NoOpStateMachine")
            return NoOpStateMachine()

        elif state_machine_class == "KeyValueStateMachine":
            logger.info("Creating KeyValueStateMachine")
            return KeyValueStateMachine()

        elif state_machine_class == "RocksDBStateMachine":
            logger.info("Creating RocksDBStateMachine")
            return StateMachineFactory._create_rocksdb_state_machine(config)

        else:
            raise ValueError(
                f"Unsupported state machine class: {state_machine_class}. "
                f"Supported classes: NoOpStateMachine, KeyValueStateMachine, RocksDBStateMachine"
            )

    @staticmethod
    def _create_rocksdb_state_machine(config: Config) -> StateMachine:
        """Create a RocksDB state machine.

        This is separated out to handle the optional dependency gracefully.
        """
        try:
            from raft.models.snapshot_rocksdb import RocksDBStateMachine

            return RocksDBStateMachine(config.data_directory)
        except ImportError as e:
            raise ImportError(
                "RocksDBStateMachine requires the 'python-rocksdb' package. "
                "Install it with: pip install python-rocksdb"
            ) from e

    @staticmethod
    def get_available_state_machine_classes() -> list[str]:
        """Get a list of available state machine classes."""
        available = ["NoOpStateMachine", "KeyValueStateMachine"]

        # Check if RocksDB is available
        try:
            import rocksdb  # noqa: F401

            available.append("RocksDBStateMachine")
        except ImportError:
            pass

        return available

    @staticmethod
    def validate_state_machine_config(config: Config) -> None:
        """Validate that the state machine configuration is valid.

        Args:
            config: The Raft configuration object

        Raises:
            ValueError: If the configuration is invalid
        """
        available_classes = StateMachineFactory.get_available_state_machine_classes()

        if config.state_machine_class not in available_classes:
            raise ValueError(
                f"State machine class '{config.state_machine_class}' is not available. "
                f"Available classes: {', '.join(available_classes)}"
            )
