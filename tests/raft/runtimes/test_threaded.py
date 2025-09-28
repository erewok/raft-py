import json
import queue
import threading
from unittest.mock import Mock, patch

from raft.io.storage import InMemoryStorage
from raft.models.server import Candidate, Follower, Leader
from raft.runtimes.threaded import ThreadedEventController, ThreadedRuntime

from raft.models import Event, EventType


def test_threaded_event_controller_initialization(config):
    """Test ThreadedEventController initialization."""
    controller = ThreadedEventController(1, config)

    assert controller.node_id == 1
    assert controller.debug == config.debug
    assert controller.address == config.node_mapping[1]["addr"]
    assert controller.heartbeat_timeout_ms == config.heartbeat_timeout_ms
    assert controller.election_timer is None
    assert controller.heartbeat is None
    assert isinstance(controller.command_event, threading.Event)
    assert isinstance(controller.inbound_msg_queue, queue.Queue)
    assert isinstance(controller.events, queue.Queue)
    assert isinstance(controller.outbound_msg_queue, queue.Queue)


def test_threaded_event_controller_add_response_to_queue(config):
    """Test adding responses to the outbound message queue."""
    controller = ThreadedEventController(1, config)

    # Mock message with dest and to_bytes method
    mock_msg = Mock()
    mock_msg.dest = ("127.0.0.1", 3111)
    mock_msg.to_bytes.return_value = b"test message"

    controller.add_response_to_queue(mock_msg)

    # Check that message was added to queue
    assert not controller.outbound_msg_queue.empty()
    addr, msg_bytes = controller.outbound_msg_queue.get()
    assert addr == ("127.0.0.1", 3111)
    assert msg_bytes == b"test message"


def test_threaded_event_controller_client_msg_into_event(config):
    """Test converting client messages into events."""
    controller = ThreadedEventController(1, config)

    # Test valid message
    append_request = json.dumps(
        {
            "term": 1,
            "leader_id": 1,
            "prev_log_index": 0,
            "prev_log_term": 0,
            "entries": [],
            "leader_commit_index": 0,
            "type": 3,  # AppendEntriesRequest
            "dest": ["127.0.0.1", 3111],
            "source": ["127.0.0.1", 3112],
        }
    ).encode()

    event = controller.client_msg_into_event(append_request)

    assert event is not None
    assert event.type == EventType.LeaderAppendLogEntryRpc
    assert not controller.events.empty()

    # Test debug message (special case)
    debug_msg = json.dumps(
        {
            "type": 99,  # DEBUG_MESSAGE
            "dest": ["127.0.0.1", 3111],
            "source": ["127.0.0.1", 3112],
        }
    ).encode()

    event = controller.client_msg_into_event(debug_msg)

    assert event is not None
    assert event.type == EventType.DEBUG_REQUEST
    assert event.msg.source == controller.address


def test_threaded_event_controller_run_and_stop_heartbeat(config):
    """Test starting and stopping heartbeat timer."""
    controller = ThreadedEventController(1, config)

    # Initially no heartbeat
    assert controller.heartbeat is None

    # Start heartbeat
    controller.run_heartbeat()
    assert controller.heartbeat is not None
    assert controller.heartbeat.thread is not None

    # Stop heartbeat
    controller.stop_heartbeat()
    assert controller.heartbeat is None


def test_threaded_event_controller_run_and_stop_election_timer(config):
    """Test starting and stopping election timer."""
    controller = ThreadedEventController(1, config)

    # Initially no election timer
    assert controller.election_timer is None

    # Start election timer
    controller.run_election_timeout_timer()
    assert controller.election_timer is not None
    assert controller.election_timer.thread is not None

    # Stop election timer
    controller.stop_election_timer()
    assert controller.election_timer is None


def test_threaded_event_controller_stop(config):
    """Test stopping the event controller."""
    controller = ThreadedEventController(1, config)

    # Start some timers
    controller.run_heartbeat()
    controller.run_election_timeout_timer()

    # Mock the transport client_send_msg to avoid actual network calls
    with patch("raft.runtimes.threaded.transport.client_send_msg"):
        controller.stop()

    # Check that timers are stopped
    assert controller.heartbeat is None
    assert controller.election_timer is None
    assert controller.command_event.is_set()


def test_threaded_runtime_initialization(config):
    """Test ThreadedRuntime initialization."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)

    assert runtime.debug == config.debug
    assert isinstance(runtime.instance, Follower)
    assert isinstance(runtime.event_controller, ThreadedEventController)
    assert isinstance(runtime.command_q, queue.Queue)
    assert runtime.thread is None


def test_threaded_runtime_log_name(config):
    """Test ThreadedRuntime log name property."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)

    log_name = runtime.log_name
    assert "ThreadedRuntime" in log_name
    assert "Follower" in log_name


def test_threaded_runtime_handle_debug_event(config):
    """Test handling debug events."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)
    debug_event = Event(EventType.DEBUG_REQUEST, None)

    # This should not raise any exceptions
    runtime.handle_debug_event(debug_event)


def test_threaded_runtime_handle_reset_election_timeout(config):
    """Test handling reset election timeout events."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)
    reset_event = Event(EventType.ResetElectionTimeout, None)

    # Mock the event controller's run_election_timeout_timer method
    runtime.event_controller.run_election_timeout_timer = Mock()

    runtime.handle_reset_election_timeout(reset_event)

    runtime.event_controller.run_election_timeout_timer.assert_called_once()


def test_threaded_runtime_handle_start_heartbeat(config):
    """Test handling start heartbeat events."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)
    heartbeat_event = Event(EventType.StartHeartbeat, None)

    # Mock the event controller's run_heartbeat method
    runtime.event_controller.run_heartbeat = Mock()

    runtime.handle_start_heartbeat(heartbeat_event)

    runtime.event_controller.run_heartbeat.assert_called_once()


def test_threaded_runtime_runtime_handle_event_debug(config):
    """Test runtime handling of debug events."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)
    debug_event = Event(EventType.DEBUG_REQUEST, None)

    runtime.handle_debug_event = Mock()
    runtime.runtime_handle_event(debug_event)

    runtime.handle_debug_event.assert_called_once_with(debug_event)


def test_threaded_runtime_runtime_handle_event_conversion_to_follower(config):
    """Test runtime handling of conversion to follower events."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)
    follower_event = Event(EventType.ConversionToFollower, None)

    runtime.handle_reset_election_timeout = Mock()
    runtime.event_controller.stop_heartbeat = Mock()

    runtime.runtime_handle_event(follower_event)

    runtime.handle_reset_election_timeout.assert_called_once_with(follower_event)
    runtime.event_controller.stop_heartbeat.assert_called_once()


def test_threaded_runtime_runtime_handle_event_conversion_to_leader(config):
    """Test runtime handling of conversion to leader events."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)
    leader_event = Event(EventType.ConversionToLeader, None)

    runtime.event_controller.stop_election_timer = Mock()

    runtime.runtime_handle_event(leader_event)

    runtime.event_controller.stop_election_timer.assert_called_once()


def test_threaded_runtime_drop_event(config):
    """Test dropping inappropriate events."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)

    # Follower should drop heartbeat events
    assert isinstance(runtime.instance, Follower)
    heartbeat_event = Event(EventType.HeartbeatTime, None)
    assert runtime.drop_event(heartbeat_event) is True

    # Other events should not be dropped
    debug_event = Event(EventType.DEBUG_REQUEST, None)
    assert runtime.drop_event(debug_event) is False

    # Change to leader and test heartbeat event is not dropped
    runtime.instance = Leader(1, config, InMemoryStorage(1, config))
    assert runtime.drop_event(heartbeat_event) is False


def test_threaded_runtime_handle_event(config):
    """Test primary event handling."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)

    # Mock the instance's handle_event method
    mock_response = Mock()
    mock_responses = [mock_response]
    mock_more_events = []

    runtime.instance.handle_event = Mock(return_value=(runtime.instance, (mock_responses, mock_more_events)))
    runtime.event_controller.add_response_to_queue = Mock()

    test_event = Event(EventType.DEBUG_REQUEST, None)

    runtime.handle_event(test_event)

    runtime.instance.handle_event.assert_called_once_with(test_event)
    runtime.event_controller.add_response_to_queue.assert_called_once_with(mock_response)


def test_threaded_runtime_handle_event_with_more_events(config):
    """Test event handling that generates more events."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)

    # Create a runtime event and non-runtime event
    runtime_event = Event(EventType.ConversionToFollower, None)
    normal_event = Event(EventType.HeartbeatTime, None)
    mock_more_events = [runtime_event, normal_event]

    runtime.instance.handle_event = Mock(return_value=(runtime.instance, ([], mock_more_events)))
    runtime.runtime_handle_event = Mock()
    runtime.event_controller.events.put = Mock()

    test_event = Event(EventType.DEBUG_REQUEST, None)

    runtime.handle_event(test_event)

    # Runtime event should be handled by runtime_handle_event
    runtime.runtime_handle_event.assert_called_with(runtime_event)
    # Normal event should be put in events queue
    runtime.event_controller.events.put.assert_called_with(normal_event)


def test_threaded_runtime_context_manager(config):
    """Test ThreadedRuntime as context manager."""
    with ThreadedRuntime(1, config, InMemoryStorage) as runtime:
        assert isinstance(runtime, ThreadedRuntime)
        runtime.stop = Mock()

    runtime.stop.assert_called_once()


def test_threaded_runtime_stop(config):
    """Test stopping the runtime."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)

    # Mock event controller stop
    runtime.event_controller.stop = Mock()

    # Create and start a mock thread
    mock_thread = Mock()
    runtime.thread = mock_thread

    runtime.stop()

    runtime.event_controller.stop.assert_called_once()
    mock_thread.join.assert_called_once()
    assert runtime.thread is None


def test_threaded_runtime_stop_without_thread(config):
    """Test stopping runtime when no thread is running."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)

    runtime.event_controller.stop = Mock()

    # No thread running
    assert runtime.thread is None

    runtime.stop()

    runtime.event_controller.stop.assert_called_once()
    assert runtime.thread is None


def test_parse_msg_to_event_integration(config):
    """Test integration with parse_msg_to_event function."""
    controller = ThreadedEventController(1, config)

    # Test various message types
    test_cases = [
        {
            "msg": json.dumps(
                {
                    "term": 1,
                    "leader_id": 1,
                    "prev_log_index": 0,
                    "prev_log_term": 0,
                    "entries": [],
                    "leader_commit_index": 0,
                    "type": 3,  # AppendEntriesRequest
                    "dest": ["127.0.0.1", 3111],
                    "source": ["127.0.0.1", 3112],
                }
            ).encode(),
            "expected_type": EventType.LeaderAppendLogEntryRpc,
        },
        {
            "msg": json.dumps(
                {
                    "term": 1,
                    "candidate_id": 1,
                    "last_log_index": 0,
                    "last_log_term": 0,
                    "type": 1,  # RequestVoteRequest
                    "dest": ["127.0.0.1", 3111],
                    "source": ["127.0.0.1", 3112],
                }
            ).encode(),
            "expected_type": EventType.CandidateRequestVoteRpc,
        },
        {
            "msg": json.dumps(
                {
                    "type": 99,  # DEBUG_MESSAGE
                    "dest": ["127.0.0.1", 3111],
                    "source": ["127.0.0.1", 3112],
                }
            ).encode(),
            "expected_type": EventType.DEBUG_REQUEST,
        },
    ]

    for test_case in test_cases:
        event = controller.client_msg_into_event(test_case["msg"])
        assert event is not None
        assert event.type == test_case["expected_type"]


def test_threaded_runtime_with_different_server_states(config):
    """Test runtime behavior with different server states."""
    # Test with Follower
    runtime = ThreadedRuntime(1, config, InMemoryStorage)
    assert isinstance(runtime.instance, Follower)
    assert "Follower" in runtime.log_name

    # Change to Candidate
    runtime.instance = Candidate(1, config, InMemoryStorage(1, config))
    assert isinstance(runtime.instance, Candidate)
    assert "Candidate" in runtime.log_name

    # Change to Leader
    runtime.instance = Leader(1, config, InMemoryStorage(1, config))
    assert isinstance(runtime.instance, Leader)
    assert "Leader" in runtime.log_name


def test_threaded_event_controller_termination_handling(config):
    """Test proper termination handling in event controller."""
    controller = ThreadedEventController(1, config)

    # Test termination sentinel
    assert controller.termination_sentinel is not None

    # Test that queues can handle termination sentinel
    controller.inbound_msg_queue.put(controller.termination_sentinel)
    controller.outbound_msg_queue.put(controller.termination_sentinel)
    controller.events.put(controller.termination_sentinel)

    # Should be able to retrieve them
    assert controller.inbound_msg_queue.get() is controller.termination_sentinel
    assert controller.outbound_msg_queue.get() is controller.termination_sentinel
    assert controller.events.get() is controller.termination_sentinel


def test_threaded_runtime_event_processing_integration(config):
    """Test integration of event processing through the runtime."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)

    # Create a simple event
    test_event = Event(EventType.DEBUG_REQUEST, None)

    # Mock the server's handle_event to return predictable results
    mock_server = Mock()
    mock_server.handle_event = Mock(return_value=(mock_server, ([], [])))
    mock_server.log_name = "MockServer"
    mock_server.__class__.log_name = Mock(return_value="MockServer")
    # Add transfer_attrs that can be iterated over (needed for handle_debug_event)
    mock_server.transfer_attrs = ("current_term", "node_id", "voted_for")
    # Add attributes that transfer_attrs will try to access
    mock_server.current_term = 1
    mock_server.node_id = 1
    mock_server.voted_for = None
    mock_server.log = []
    runtime.instance = mock_server

    # Process the event
    runtime.handle_event(test_event)

    # Verify the server's handle_event was called
    mock_server.handle_event.assert_called_once_with(test_event)


def test_threaded_runtime_queue_management(config):
    """Test proper queue management in threaded runtime."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)

    # Test command queue
    assert isinstance(runtime.command_q, queue.Queue)
    assert runtime.command_q.empty()

    # Test that we can signal stop via command queue
    runtime.command_q.put(True)
    assert not runtime.command_q.empty()


def test_threaded_event_controller_queue_properties(config):
    """Test queue properties and limits."""
    controller = ThreadedEventController(1, config)

    # Test queue maxsize settings
    assert controller.inbound_msg_queue.maxsize == 20
    assert controller.events.maxsize == 20
    # outbound_msg_queue has no explicit maxsize in constructor


def test_threaded_runtime_server_conversion_scenarios(config):
    """Test server conversion scenarios through runtime."""
    runtime = ThreadedRuntime(1, config, InMemoryStorage)

    # Start as Follower
    assert isinstance(runtime.instance, Follower)

    # Test conversion events
    conversion_events = [
        Event(EventType.ConversionToFollower, None),
        Event(EventType.ConversionToLeader, None),
    ]

    for event in conversion_events:
        # Mock necessary methods to avoid side effects
        runtime.handle_reset_election_timeout = Mock()
        runtime.event_controller.stop_heartbeat = Mock()
        runtime.event_controller.stop_election_timer = Mock()

        # Should handle without errors
        runtime.runtime_handle_event(event)
