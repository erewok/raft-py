import concurrent.futures
import logging

# import queue
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
import traceback
from typing import Dict, List, Optional, Tuple


HEADER_LEN = 10
DEFAULT_MSG_LEN = 4096
DEFAULT_REQUEST_TIMEOUT = 10
LISTENER_SERVER_CLIENT_TTL = 120  # 2 minutes
Address = Tuple[str, int]
MsgResponse = Optional[bytes]
logger = logging.getLogger(__name__)
Request = Tuple[Address, bytes]
SHUTDOWN_CMD = b"SHUTDOWN"


def send_message(sock, msg: bytes):
    size = b"%10d" % len(msg)  # Make a 10-byte length field
    sock.sendall(size)
    sock.sendall(msg)


def receive_message(sock):
    chunks = []
    bytes_recd = 0
    first = sock.recv(HEADER_LEN).strip()
    msg_len = 0
    try:
        msg_len = int(first)
    except (TypeError, ValueError):
        return None

    while bytes_recd < msg_len:
        chunk = sock.recv(min(msg_len - bytes_recd, DEFAULT_MSG_LEN))
        if chunk == b"":
            raise RuntimeError("Socket connection broken")
        chunks.append(chunk)
        bytes_recd += len(chunk)
    return b"".join(chunks)


def send_and_receive(sock: socket, msg: bytes):
    send_message(sock, msg)
    return receive_message(sock)


# class MessageProtocol:
#     """Dave's Size-prefixed messaging class"""

#     def __init__(self):
#         self.buffer = bytearray()

#     def encode_messsage(self, msg):
#         return b"%10d" % len(msg) + msg

#     def decode_messages(self, data):
#         self.buffer.extend(data)
#         while len(self.buffer) >= 10:
#             size = int(self.buffer[:10])
#             if len(self.buffer) >= 10 + size:
#                 yield bytes(self.buffer[10 : 10 + size])
#             del self.buffer[: 10 + size]


def handle_socket_client(client, addr, msg_queue):
    msg = None
    try:
        msg = receive_message(client)
        if msg is not None:
            logger.debug(f"{addr[0]}:{addr[1]} Received: {msg.decode()}")
            msg_queue.put(msg)
            send_message(client, b"ok")
            client.close()
    except Exception:
        logger.error(traceback.format_exc())
    finally:
        client.close()
    return msg


def listen_server(address, msg_queue, listen_server_command_q=None):
    with socket(AF_INET, SOCK_STREAM) as sock:
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        sock.bind(address)
        sock.listen()
        logger.info(f"---Server Start: listening at {address[0]}:{address[1]}---")
        while True:
            if listen_server_command_q and not listen_server_command_q.empty():
                break
            client, addr = sock.accept()
            result = handle_socket_client(client, addr, msg_queue)
            if result == SHUTDOWN_CMD and addr == address:
                # received shutdown message from this host
                break
    logger.info(f"---Server Stop: Listening at {address[0]}:{address[1]}---")


def client_send_msg(
    address: Address, msg: bytes, timeout: int = DEFAULT_REQUEST_TIMEOUT
) -> Optional[bytes]:
    with socket(AF_INET, SOCK_STREAM) as sock:
        old_timeout = sock.gettimeout()
        sock.settimeout(timeout)
        try:
            sock.connect(address)
            send_message(sock, msg)
            response = receive_message(sock)
            sock.settimeout(old_timeout)
            if response == b"ok":
                return response
        except OSError:
            logger.error(f"---Client Send FAIL {address[0]}:{address[1]}---")
        return None


def broadcast_requests(
    address_msgs: List[Request], timeout: int = 1
) -> Dict[Address, MsgResponse]:
    results_by_addr: Dict[Address, MsgResponse] = {}  # addr -> bytes result
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        request_rpcs = {
            executor.submit(client_send_msg, addr, msg, timeout): addr
            for addr, msg in address_msgs
        }
        for future in concurrent.futures.as_completed(request_rpcs):
            addr = request_rpcs[future]
            try:
                results_by_addr[addr] = future.result()
            except Exception:
                logger.error(f"Failed reaching socket address: {addr[0]}:{addr[1]}")
                logger.error(traceback.format_exc())
                results_by_addr[addr] = None
    return results_by_addr