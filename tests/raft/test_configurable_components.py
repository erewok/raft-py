"""Tests for configurable storage and state machine factories."""

import configparser
import os
import tempfile

from raft.io.storage_factory import StorageFactory
from raft.models.config import Config
from raft.models.state_machine_factory import StateMachineFactory


class TestConfigurableComponents:
    """Test configurable storage and state machine creation."""

    def test_inmemory_storage_creation(self):
        """Test creating InMemoryStorage from configuration."""
        # Create minimal config
        conf = configparser.ConfigParser()
        conf.add_section("Cluster")
        conf.set("Cluster", "Debug", "True")
        conf.set("Cluster", "DataDirectory", "./test_data")
        conf.set("Cluster", "HeartbeatInterval", "5")
        conf.set("Cluster", "ElectionTimeout", "1000")
        conf.set("Cluster", "NodeCount", "3")
        conf.set("Cluster", "StorageClass", "InMemoryStorage")

        config = Config(conf)

        # Create storage
        storage = StorageFactory.create_storage(1, config)

        # Verify
        assert type(storage).__name__ == "InMemoryStorage"
        assert hasattr(storage, "save_snapshot")
        assert hasattr(storage, "load_snapshot")

    def test_file_storage_creation(self):
        """Test creating FileStorage from configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create config
            conf = configparser.ConfigParser()
            conf.add_section("Cluster")
            conf.set("Cluster", "Debug", "True")
            conf.set("Cluster", "DataDirectory", temp_dir)
            conf.set("Cluster", "HeartbeatInterval", "5")
            conf.set("Cluster", "ElectionTimeout", "1000")
            conf.set("Cluster", "NodeCount", "3")
            conf.set("Cluster", "StorageClass", "FileStorage")

            config = Config(conf)

            # Create storage
            storage = StorageFactory.create_storage(1, config)

            # Verify
            assert type(storage).__name__ == "FileStorage"
            assert hasattr(storage, "save_snapshot")

            # Verify directory was created
            node_dir = os.path.join(temp_dir, "node_1")
            assert os.path.exists(node_dir)

    def test_sqlite_storage_creation(self):
        """Test creating SqliteStorage from configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create config
            conf = configparser.ConfigParser()
            conf.add_section("Cluster")
            conf.set("Cluster", "Debug", "True")
            conf.set("Cluster", "DataDirectory", temp_dir)
            conf.set("Cluster", "HeartbeatInterval", "5")
            conf.set("Cluster", "ElectionTimeout", "1000")
            conf.set("Cluster", "NodeCount", "3")
            conf.set("Cluster", "StorageClass", "SqliteStorage")

            config = Config(conf)

            # Create storage
            storage = StorageFactory.create_storage(1, config)

            # Verify
            assert type(storage).__name__ == "SqliteStorage"
            assert hasattr(storage, "save_snapshot")
            assert hasattr(storage, "load_snapshot")
            assert hasattr(storage, "get_storage_stats")

            # Verify database file was created
            assert os.path.exists(storage.db_path)
            assert storage.db_path.endswith("node_1.db")

            # Clean up
            storage.close()

    def test_keyvalue_state_machine_creation(self):
        """Test creating KeyValueStateMachine from configuration."""
        # Create config
        conf = configparser.ConfigParser()
        conf.add_section("Cluster")
        conf.set("Cluster", "Debug", "True")
        conf.set("Cluster", "DataDirectory", "./test_data")
        conf.set("Cluster", "HeartbeatInterval", "5")
        conf.set("Cluster", "ElectionTimeout", "1000")
        conf.set("Cluster", "NodeCount", "3")
        conf.set("Cluster", "StateMachineClass", "KeyValueStateMachine")

        config = Config(conf)

        # Create state machine
        state_machine = StateMachineFactory.create_state_machine(config)

        # Verify
        assert type(state_machine).__name__ == "KeyValueStateMachine"
        assert hasattr(state_machine, "apply_entry")
        assert hasattr(state_machine, "create_snapshot")

    def test_noop_state_machine_creation(self):
        """Test creating NoOpStateMachine from configuration."""
        # Create config
        conf = configparser.ConfigParser()
        conf.add_section("Cluster")
        conf.set("Cluster", "Debug", "True")
        conf.set("Cluster", "DataDirectory", "./test_data")
        conf.set("Cluster", "HeartbeatInterval", "5")
        conf.set("Cluster", "ElectionTimeout", "1000")
        conf.set("Cluster", "NodeCount", "3")
        conf.set("Cluster", "StateMachineClass", "NoOpStateMachine")

        config = Config(conf)

        # Create state machine
        state_machine = StateMachineFactory.create_state_machine(config)

        # Verify
        assert type(state_machine).__name__ == "NoOpStateMachine"

    def test_snapshot_configuration_loading(self):
        """Test loading snapshot-related configuration."""
        # Create config with snapshot settings
        conf = configparser.ConfigParser()
        conf.add_section("Cluster")
        conf.set("Cluster", "Debug", "True")
        conf.set("Cluster", "DataDirectory", "./test_data")
        conf.set("Cluster", "HeartbeatInterval", "5")
        conf.set("Cluster", "ElectionTimeout", "1000")
        conf.set("Cluster", "NodeCount", "3")
        conf.set("Cluster", "StorageClass", "InMemoryStorage")
        conf.set("Cluster", "SnapshotThreshold", "500")
        conf.set("Cluster", "MaxSnapshotsToKeep", "5")
        conf.set("Cluster", "SnapshotCompressionEnabled", "True")
        conf.set("Cluster", "StateMachineClass", "KeyValueStateMachine")

        config = Config(conf)

        # Verify snapshot settings
        assert config.snapshot_threshold == 500
        assert config.max_snapshots_to_keep == 5
        assert config.snapshot_compression_enabled is True
        assert config.state_machine_class == "KeyValueStateMachine"
        assert config.log_compaction_threshold == 500  # Backward compatibility

    def test_invalid_storage_class_raises_error(self):
        """Test that invalid storage class raises ValueError."""
        conf = configparser.ConfigParser()
        conf.add_section("Cluster")
        conf.set("Cluster", "Debug", "True")
        conf.set("Cluster", "DataDirectory", "./test_data")
        conf.set("Cluster", "HeartbeatInterval", "5")
        conf.set("Cluster", "ElectionTimeout", "1000")
        conf.set("Cluster", "NodeCount", "3")
        conf.set("Cluster", "StorageClass", "InvalidStorage")

        config = Config(conf)

        try:
            StorageFactory.create_storage(1, config)
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "Unsupported storage class" in str(e)

    def test_invalid_state_machine_class_raises_error(self):
        """Test that invalid state machine class raises ValueError."""
        conf = configparser.ConfigParser()
        conf.add_section("Cluster")
        conf.set("Cluster", "Debug", "True")
        conf.set("Cluster", "DataDirectory", "./test_data")
        conf.set("Cluster", "HeartbeatInterval", "5")
        conf.set("Cluster", "ElectionTimeout", "1000")
        conf.set("Cluster", "NodeCount", "3")
        conf.set("Cluster", "StateMachineClass", "InvalidStateMachine")

        config = Config(conf)

        try:
            StateMachineFactory.create_state_machine(config)
            raise AssertionError("Should have raised ValueError")
        except ValueError as e:
            assert "Unsupported state machine class" in str(e)

    def test_get_available_classes(self):
        """Test getting lists of available classes."""
        storage_classes = StorageFactory.get_available_storage_classes()
        assert "InMemoryStorage" in storage_classes
        assert "FileStorage" in storage_classes
        assert "AsyncFileStorage" in storage_classes
        assert "SqliteStorage" in storage_classes

        sm_classes = StateMachineFactory.get_available_state_machine_classes()
        assert "NoOpStateMachine" in sm_classes
        assert "KeyValueStateMachine" in sm_classes
