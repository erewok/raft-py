import os
import tempfile

import pytest
from raft.io.storage import AsyncFileStorage, FileStorage, InMemoryStorage


@pytest.fixture
def temp_config(config):
    """Create a config fixture with a temporary data directory for storage tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a copy of the config and override the data directory
        config.data_directory = temp_dir

        # Create initial storage structure for FileStorage tests
        # This is needed because FileStorage.set_stored_item_count() expects
        # existing directories and files
        for node_label in ["A", "B", "C"]:
            node_dir = os.path.join(temp_dir, node_label, "data", "000")
            os.makedirs(node_dir, exist_ok=True)
            # Create an initial empty file
            initial_file = os.path.join(node_dir, "000000")
            with open(initial_file, "w"):
                pass  # Create empty file

        yield config


def test_inmemory_storage_initialization(config):
    """Test InMemoryStorage initialization."""
    storage = InMemoryStorage(1, config)

    assert storage.log == []
    assert storage.metadata == {"node_id": 1}


def test_inmemory_storage_save_metadata(temp_config):
    """Test InMemoryStorage metadata saving."""
    storage = InMemoryStorage(1, temp_config)
    test_data = b"test metadata"

    storage.save_metadata(test_data)

    assert storage.metadata["stored"] == test_data
    assert "updated" in storage.metadata
    assert isinstance(storage.metadata["updated"], float)


def test_inmemory_storage_save_log_entry(temp_config):
    """Test InMemoryStorage log entry saving."""
    storage = InMemoryStorage(1, temp_config)
    test_entry = b"log entry 1"

    storage.save_log_entry(test_entry)

    assert len(storage.log) == 1
    assert storage.log[0] == test_entry


def test_inmemory_storage_multiple_log_entries(temp_config):
    """Test InMemoryStorage with multiple log entries."""
    storage = InMemoryStorage(2, temp_config)
    entries = [b"entry1", b"entry2", b"entry3"]

    for entry in entries:
        storage.save_log_entry(entry)

    assert len(storage.log) == 3
    assert storage.log == entries
    assert storage.metadata["node_id"] == 2


def test_file_storage_initialization(temp_config):
    """Test FileStorage initialization and directory creation."""
    storage = FileStorage(1, temp_config)

    assert storage.data_directory == temp_config.data_directory
    assert storage.node_label == "A"
    assert storage.stored_item_count >= 0

    # Check that directories were created
    assert os.path.exists(storage.storage_directory)
    assert os.path.exists(storage.data_filepath)
    assert os.path.exists(os.path.join(storage.data_filepath, "000"))


def test_file_storage_metadata_saving(temp_config):
    """Test FileStorage metadata saving to file."""
    storage = FileStorage(2, temp_config)
    test_metadata = b"test metadata for node 2"

    storage.save_metadata(test_metadata)

    # Verify file was created and contains the metadata
    assert os.path.exists(storage.metadata_filepath)
    with open(storage.metadata_filepath, "rb") as f:
        saved_data = f.read()
    assert saved_data == test_metadata


def test_file_storage_log_entry_saving(temp_config):
    """Test FileStorage log entry saving to file."""
    storage = FileStorage(1, temp_config)
    test_entry = b"test log entry\n"
    initial_count = storage.stored_item_count

    storage.save_log_entry(test_entry)

    # Check that item count increased
    assert storage.stored_item_count == initial_count + 1

    # Check that file was created and contains the entry
    filepath = storage.data_storage_filepath
    assert os.path.exists(filepath)
    with open(filepath, "rb") as f:
        saved_data = f.read()
    assert test_entry in saved_data


def test_file_storage_multiple_log_entries(temp_config):
    """Test FileStorage with multiple log entries."""
    storage = FileStorage(3, temp_config)
    entries = [b"entry1\n", b"entry2\n", b"entry3\n"]
    initial_count = storage.stored_item_count

    for entry in entries:
        storage.save_log_entry(entry)

    assert storage.stored_item_count == initial_count + len(entries)

    # Check that all entries were saved
    filepath = storage.data_storage_filepath
    with open(filepath, "rb") as f:
        content = f.read()

    for entry in entries:
        assert entry in content


def test_file_storage_data_storage_filepath_formatting(temp_config):
    """Test FileStorage filepath formatting with different item counts."""
    storage = FileStorage(1, temp_config)

    # Test with different item counts
    test_cases = [
        (0, "000/000000"),
        (1, "000/000000"),  # Same file as 0, different line
        (1000, "000/000001"),  # New file in same directory
        (1000000, "000/001000"),  # New file in same directory
    ]

    for count, expected_suffix in test_cases:
        storage.stored_item_count = count
        filepath = storage.data_storage_filepath
        assert expected_suffix in filepath


def test_file_storage_different_nodes(temp_config):
    """Test FileStorage with different node IDs creates different directories."""
    storage1 = FileStorage(1, temp_config)  # Node A
    storage2 = FileStorage(2, temp_config)  # Node B

    assert storage1.node_label == "A"
    assert storage2.node_label == "B"
    assert storage1.storage_directory != storage2.storage_directory

    # Both should create their own directories
    assert os.path.exists(storage1.storage_directory)
    assert os.path.exists(storage2.storage_directory)


@pytest.mark.trio
async def test_async_file_storage_initialization(temp_config):
    """Test AsyncFileStorage initialization."""
    storage = AsyncFileStorage(1, temp_config)

    assert storage.data_directory == temp_config.data_directory
    assert storage.node_label == "A"
    assert storage.stored_item_count == 0

    # Check that directories were created
    assert os.path.exists(storage.storage_directory)
    assert os.path.exists(storage.data_filepath)


@pytest.mark.trio
async def test_async_file_storage_metadata_saving(temp_config):
    """Test AsyncFileStorage metadata saving."""
    storage = AsyncFileStorage(2, temp_config)
    test_metadata = b"async test metadata"

    await storage.save_metadata(test_metadata)

    # Verify file was created and contains the metadata
    assert os.path.exists(storage.metadata_filepath)
    with open(storage.metadata_filepath, "rb") as f:
        saved_data = f.read()
    assert saved_data == test_metadata


@pytest.mark.trio
async def test_async_file_storage_log_entry_saving(temp_config):
    """Test AsyncFileStorage log entry saving."""
    storage = AsyncFileStorage(1, temp_config)
    test_entry = b"async test log entry\n"

    await storage.save_log_entry(test_entry)

    # Check that item count increased
    assert storage.stored_item_count == 1

    # Check that file was created and contains the entry
    filepath = storage.data_storage_filepath
    assert os.path.exists(filepath)
    with open(filepath, "rb") as f:
        saved_data = f.read()
    assert test_entry in saved_data


@pytest.mark.trio
async def test_async_file_storage_multiple_entries(temp_config):
    """Test AsyncFileStorage with multiple log entries."""
    storage = AsyncFileStorage(3, temp_config)
    entries = [b"async_entry1\n", b"async_entry2\n", b"async_entry3\n"]

    for entry in entries:
        await storage.save_log_entry(entry)

    assert storage.stored_item_count == len(entries)

    # Check that all entries were saved
    filepath = storage.data_storage_filepath
    with open(filepath, "rb") as f:
        content = f.read()

    for entry in entries:
        assert entry in content


@pytest.mark.trio
async def test_async_file_storage_set_stored_item_count(temp_config):
    """Test AsyncFileStorage set_stored_item_count method."""
    storage = AsyncFileStorage(1, temp_config)

    # Create some test data structure
    test_dir = os.path.join(storage.data_filepath, "000")
    os.makedirs(test_dir, exist_ok=True)
    test_file = os.path.join(test_dir, "000000")

    # Write some test lines
    with open(test_file, "w") as f:
        f.write("line1\n")
        f.write("line2\n")
        f.write("line3\n")

    # Test the method
    await storage.set_stored_item_count()

    # Should be 000 + 000000 + 003 = 3
    assert storage.stored_item_count == 3


def test_storage_classes_implement_base_interface(temp_config):
    """Test that all storage classes implement the BaseStorage interface."""
    # Test that all classes have the required methods
    storage_classes = [InMemoryStorage, FileStorage, AsyncFileStorage]

    for storage_class in storage_classes:
        storage = storage_class(1, temp_config)

        # Check that required methods exist
        assert hasattr(storage, 'save_metadata')
        assert hasattr(storage, 'save_log_entry')
        assert callable(storage.save_metadata)
        assert callable(storage.save_log_entry)


def test_file_storage_handles_large_item_counts(temp_config):
    """Test FileStorage data_storage_filepath with large item counts."""
    storage = FileStorage(1, temp_config)

    # Test with a large item count
    storage.stored_item_count = 999999999999  # 12 digits
    filepath = storage.data_storage_filepath

    # Should format correctly with 12 digits
    item_str = f"{storage.stored_item_count:012}"
    expected_dir = item_str[:3]
    expected_file = item_str[3:9]

    assert expected_dir in filepath
    assert expected_file in filepath


def test_storage_preserves_binary_data(temp_config):
    """Test that storage classes properly handle binary data."""
    # Test data with various binary content
    test_data = bytes([0, 1, 255, 128, 42]) + b"\x00\xff\x7f"

    # Test InMemoryStorage
    mem_storage = InMemoryStorage(1, temp_config)
    mem_storage.save_metadata(test_data)
    mem_storage.save_log_entry(test_data)

    assert mem_storage.metadata["stored"] == test_data
    assert mem_storage.log[0] == test_data

    # Test FileStorage
    file_storage = FileStorage(1, temp_config)
    file_storage.save_metadata(test_data)
    file_storage.save_log_entry(test_data)

    # Verify metadata
    with open(file_storage.metadata_filepath, "rb") as f:
        saved_metadata = f.read()
    assert saved_metadata == test_data

    # Verify log entry
    with open(file_storage.data_storage_filepath, "rb") as f:
        saved_log = f.read()
    assert test_data in saved_log
