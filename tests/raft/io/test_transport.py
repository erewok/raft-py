from socket import (
    socket,
    AF_INET,
    SOCK_STREAM,
    SOL_SOCKET,
    SO_REUSEADDR,
)
import threading

import pytest

from raft.io import transport


MSG_SIZES = [b"x" * (10 ** n) for n in range(0, 8)]
DEFAULT_ADDRESS = ("", 40000)


# def make_socket_thread():
#     sock = socket(AF_INET, SOCK_STREAM)
#     sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
#     sock.bind(DEFAULT_ADDRESS)
#     sock.listen()

#     def client_listener(sock):
#         while True:
#             client, _ = sock.accept()
#             client.close()
#     threading.Thread(target=client_listener, args=(sock, )).start()
#     yield sock
#     yield
#     sock.close()


# def make_client():
#     client = socket(AF_INET, SOCK_STREAM)
#     client.connect(DEFAULT_ADDRESS)
#     yield client
#     yield
#     client.close()


# @pytest.mark.parametrize("msg", [MSG_SIZES[0]])
# def test_send_message(msg):
#     sock_gen = make_socket_thread()
#     sock = next(sock_gen)
#     client_gen = make_client()
#     client = next(client_gen)
#     result = transport.receive_message(sock)
#     assert result == msg
#     next(client_gen)
