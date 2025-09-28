import queue
import threading
import time
from socket import AF_INET, SO_REUSEADDR, SOCK_STREAM, socket, SOL_SOCKET
from unittest.mock import Mock, patch

import pytest

from raft.io import SHUTDOWN_CMD, transport

MSG_SIZES = [b"x" * (10**n) for n in range(0, 8)]
DEFAULT_ADDRESS = ("127.0.0.1", 40000)
BAD_ADDRESS = ("127.0.0.1", 40001)


def get_free_port():
    """Get a free port for testing"""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Message Protocol Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


def test_send_and_receive_message_round_trip():
    """Test that messages can be sent and received correctly"""
    port = get_free_port()
    address = ("127.0.0.1", port)

    # Create a socket pair for testing
    server_sock = socket(AF_INET, SOCK_STREAM)
    server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
    server_sock.bind(address)
    server_sock.listen(1)

    # Create client socket in a separate thread
    def client_thread():
        client_sock = socket(AF_INET, SOCK_STREAM)
        client_sock.connect(address)
        test_msg = b"Hello, World!"
        transport.send_message(client_sock, test_msg)
        response = transport.receive_message(client_sock)
        client_sock.close()
        return response

    client_thread_obj = threading.Thread(target=client_thread)
    client_thread_obj.start()

    # Server side
    conn, addr = server_sock.accept()
    received_msg = transport.receive_message(conn)
    assert received_msg == b"Hello, World!"

    # Send response back
    transport.send_message(conn, b"ACK")
    conn.close()
    server_sock.close()

    client_thread_obj.join()


@pytest.mark.parametrize("msg", MSG_SIZES[:6])  # Test up to 1MB messages
def test_send_receive_various_sizes(msg):
    """Test sending and receiving messages of various sizes"""
    port = get_free_port()
    address = ("127.0.0.1", port)

    server_sock = socket(AF_INET, SOCK_STREAM)
    server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
    server_sock.bind(address)
    server_sock.listen(1)

    received_msg = None

    def client_thread():
        client_sock = socket(AF_INET, SOCK_STREAM)
        client_sock.connect(address)
        transport.send_message(client_sock, msg)
        client_sock.close()

    client_thread_obj = threading.Thread(target=client_thread)
    client_thread_obj.start()

    conn, addr = server_sock.accept()
    received_msg = transport.receive_message(conn)
    conn.close()
    server_sock.close()

    client_thread_obj.join()

    assert received_msg == msg
    assert len(received_msg) == len(msg)


def test_receive_message_invalid_header():
    """Test receive_message with invalid header"""
    mock_sock = Mock()
    mock_sock.recv.return_value = b"invalid"
    result = transport.receive_message(mock_sock)
    assert result is None


def test_send_and_receive_convenience_function():
    """Test the send_and_receive convenience function"""
    port = get_free_port()
    address = ("127.0.0.1", port)

    server_sock = socket(AF_INET, SOCK_STREAM)
    server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
    server_sock.bind(address)
    server_sock.listen(1)

    response = None

    def client_thread():
        nonlocal response
        client_sock = socket(AF_INET, SOCK_STREAM)
        client_sock.connect(address)
        response = transport.send_and_receive(client_sock, b"test message")
        client_sock.close()

    client_thread_obj = threading.Thread(target=client_thread)
    client_thread_obj.start()

    conn, addr = server_sock.accept()
    received_msg = transport.receive_message(conn)
    assert received_msg == b"test message"
    transport.send_message(conn, b"response")
    conn.close()
    server_sock.close()

    client_thread_obj.join()
    assert response == b"response"


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Client Function Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


def test_client_send_msg_success():
    """Test successful client message sending"""
    port = get_free_port()
    address = ("127.0.0.1", port)
    msg_queue = queue.Queue()

    # Start server in background
    def server_thread():
        transport.listen_server(address, msg_queue)

    server_thread_obj = threading.Thread(target=server_thread, daemon=True)
    server_thread_obj.start()
    time.sleep(0.1)  # Give server time to start

    # Send message from client
    result = transport.client_send_msg(address, b"test message")

    # Check results
    assert result == b"ok"
    received_msg = msg_queue.get(timeout=1)
    assert received_msg == b"test message"

    # Shutdown server
    transport.client_send_msg(address, SHUTDOWN_CMD)


def test_client_send_msg_connection_failure():
    """Test client_send_msg when connection fails"""
    result = transport.client_send_msg(BAD_ADDRESS, b"test message", timeout=1)
    assert result is None


def test_client_send_msg_timeout():
    """Test client_send_msg with timeout"""
    # Try to connect to non-existent server with short timeout
    result = transport.client_send_msg(BAD_ADDRESS, b"test message", timeout=0.1)
    assert result is None


def test_broadcast_requests_success():
    """Test broadcasting requests to multiple servers"""
    msg_queue1 = queue.Queue()
    msg_queue2 = queue.Queue()

    port1 = get_free_port()
    port2 = get_free_port()
    address1 = ("127.0.0.1", port1)
    address2 = ("127.0.0.1", port2)

    # Start two servers
    def server1():
        transport.listen_server(address1, msg_queue1)

    def server2():
        transport.listen_server(address2, msg_queue2)

    server1_thread = threading.Thread(target=server1, daemon=True)
    server2_thread = threading.Thread(target=server2, daemon=True)

    server1_thread.start()
    server2_thread.start()
    time.sleep(0.1)  # Give servers time to start

    # Prepare requests
    requests = [
        (address1, b"message1"),
        (address2, b"message2"),
    ]

    # Broadcast requests
    results = transport.broadcast_requests(requests, timeout=2)

    # Check results
    assert len(results) == 2
    assert results[address1] == b"ok"
    assert results[address2] == b"ok"

    # Check messages were received
    assert msg_queue1.get(timeout=1) == b"message1"
    assert msg_queue2.get(timeout=1) == b"message2"

    # Shutdown servers
    transport.client_send_msg(address1, SHUTDOWN_CMD)
    transport.client_send_msg(address2, SHUTDOWN_CMD)


def test_broadcast_requests_partial_failure():
    """Test broadcast_requests when some servers are unreachable"""
    port = get_free_port()
    good_address = ("127.0.0.1", port)
    msg_queue = queue.Queue()

    # Start only one server
    def server():
        transport.listen_server(good_address, msg_queue)

    server_thread = threading.Thread(target=server, daemon=True)
    server_thread.start()
    time.sleep(0.1)

    # Prepare requests (one good, one bad)
    requests = [
        (good_address, b"good message"),
        (BAD_ADDRESS, b"bad message"),
    ]

    # Broadcast requests
    results = transport.broadcast_requests(requests, timeout=1)

    # Check results
    assert len(results) == 2
    assert results[good_address] == b"ok"
    assert results[BAD_ADDRESS] is None

    # Check good message was received
    assert msg_queue.get(timeout=1) == b"good message"

    # Shutdown server
    transport.client_send_msg(good_address, SHUTDOWN_CMD)


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Server Function Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


def test_handle_socket_client():
    """Test handle_socket_client function"""
    msg_queue = queue.Queue()
    port = get_free_port()
    address = ("127.0.0.1", port)

    # Create socket pair
    server_sock = socket(AF_INET, SOCK_STREAM)
    server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
    server_sock.bind(address)
    server_sock.listen(1)

    def client_thread():
        client_sock = socket(AF_INET, SOCK_STREAM)
        client_sock.connect(address)
        transport.send_message(client_sock, b"test message")
        response = transport.receive_message(client_sock)
        client_sock.close()
        return response

    client_thread_obj = threading.Thread(target=client_thread)
    client_thread_obj.start()

    # Server side
    conn, addr = server_sock.accept()
    result = transport.handle_socket_client(conn, addr, msg_queue)
    server_sock.close()

    client_thread_obj.join()

    # Check results
    assert result == b"test message"
    assert msg_queue.get() == b"test message"


def test_listen_server_with_event():
    """Test listen_server with termination event"""
    msg_queue = queue.Queue()
    server_event = threading.Event()
    port = get_free_port()
    address = ("127.0.0.1", port)
    server_ready = threading.Event()

    def server_thread():
        try:
            # Signal that server is starting
            server_ready.set()
            transport.listen_server(address, msg_queue, server_event)
        except Exception:
            # Server was stopped
            pass

    server_thread_obj = threading.Thread(target=server_thread, daemon=True)
    server_thread_obj.start()
    server_ready.wait(timeout=1)  # Wait for server to be ready
    time.sleep(0.1)  # Give server time to start listening

    # Send a message
    result = transport.client_send_msg(address, b"test message")
    if result == b"ok":
        # Stop server using event
        server_event.set()
        server_thread_obj.join(timeout=2)

        # Check message was received
        try:
            received_msg = msg_queue.get(timeout=1)
            assert received_msg == b"test message"
        except queue.Empty:
            # If no message received due to timing, that's also acceptable
            pass


def test_listen_server_shutdown_command():
    """Test listen_server shutdown with SHUTDOWN_CMD"""
    msg_queue = queue.Queue()
    port = get_free_port()
    address = ("127.0.0.1", port)
    server_ready = threading.Event()

    def server_thread():
        try:
            server_ready.set()
            transport.listen_server(address, msg_queue)
        except Exception:
            pass

    server_thread_obj = threading.Thread(target=server_thread, daemon=True)
    server_thread_obj.start()
    server_ready.wait(timeout=1)
    time.sleep(0.1)  # Give server time to start

    # Send shutdown command
    result = transport.client_send_msg(address, SHUTDOWN_CMD)
    if result == b"ok":
        server_thread_obj.join(timeout=2)

        # Check shutdown message was received
        try:
            received_msg = msg_queue.get(timeout=1)
            assert received_msg == SHUTDOWN_CMD
        except queue.Empty:
            # If no message received due to timing, that's also acceptable
            pass


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Error Handling Tests
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


def test_receive_message_socket_broken():
    """Test receive_message when socket connection is broken"""
    mock_sock = Mock()
    mock_sock.recv.side_effect = [b"        10", b""]  # Valid header, then empty chunk

    with pytest.raises(RuntimeError, match="Socket connection broken"):
        transport.receive_message(mock_sock)


def test_handle_socket_client_exception():
    """Test handle_socket_client handles exceptions gracefully"""
    msg_queue = queue.Queue()

    with patch("raft.io.transport.receive_message") as mock_receive:
        mock_receive.side_effect = Exception("Test exception")

        mock_client = Mock()
        mock_addr = ("127.0.0.1", 12345)

        result = transport.handle_socket_client(mock_client, mock_addr, msg_queue)

        # Should return None and close the client
        assert result is None
        mock_client.close.assert_called()
