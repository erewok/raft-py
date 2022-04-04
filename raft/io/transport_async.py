from functools import partial
import logging
import traceback
from typing import Dict, List, Optional

from raft.internal import trio
from raft.io import (
    DEFAULT_MSG_LEN,
    DEFAULT_REQUEST_TIMEOUT,
    HEADER_LEN,
    CLIENT_LOG_NAME,
    SERVER_LOG_NAME,
    Address,
    MsgResponse,
    Request,
)

logger = logging.getLogger(__name__)


async def send_message(stream: trio.abc.SendStream, msg: bytes):
    size = b"%10d" % len(msg)  # Make a 10-byte length field
    await stream.send_all(size)
    await stream.send_all(msg)


async def receive_message(stream: trio.abc.ReceiveStream):
    chunks = []
    bytes_recd = 0
    first = await stream.receive_some(max_bytes=HEADER_LEN)
    first = first.strip()
    msg_len = 0
    try:
        msg_len = int(first)
    except (TypeError, ValueError):
        return None

    while bytes_recd < msg_len:
        chunk = await stream.receive_some(max_bytes=min(msg_len - bytes_recd, DEFAULT_MSG_LEN))
        if chunk == b"":
            raise RuntimeError("Socket connection broken")
        chunks.append(chunk)
        bytes_recd += len(chunk)
    return b"".join(chunks)


async def handle_socket_client(send_channel: trio.abc.SendChannel, server_stream):
    try:
        msg = await receive_message(server_stream)
        if msg is not None:
            await send_channel.send(msg)
            await server_stream.send_all(b"ok")
    except Exception:
        logger.error(traceback.format_exc())


async def listen_server(address, send_channel: trio.abc.SendChannel):
    async with send_channel:
        handler = partial(handle_socket_client, send_channel)
        logger.info(f"{SERVER_LOG_NAME} Start: listening at {address[0]}:{address[1]}")
        await trio.serve_tcp(handler, address[1])
        logger.info(f"{SERVER_LOG_NAME} Stop: Listening at {address[0]}:{address[1]}")


async def client_recv_msg(client_stream):
    logger.debug(f"{CLIENT_LOG_NAME} receiving")
    total = b""
    async for data in client_stream:
        total += data
    return total


async def client_send_msg(
    address: Address, msg: bytes, result_chan: trio.abc.SendChannel, timeout: int = DEFAULT_REQUEST_TIMEOUT
) -> Optional[bytes]:
    logger.debug(f"{CLIENT_LOG_NAME} connecting to {address[0]}:{address[1]}")
    with trio.move_on_after(timeout):
        client_stream = await trio.open_tcp_stream(address[0], address[1])
        async with client_stream:
            await send_message(client_stream, msg)
            await result_chan.send(msg)


async def client_send_success_reporter(read_chan: trio.abc.ReceiveChannel) -> Dict[Address, MsgResponse]:
    results_by_addr: Dict[Address, MsgResponse] = {}  # addr -> bytes result
    async with read_chan:
        pass


async def broadcast_requests(
    address_msgs: List[Request], timeout: int = 1
) -> Dict[Address, MsgResponse]:
    sender_with_timeout = partial(client_send_msg, timeout=timeout)

    async with trio.open_nursery() as nursery:
        results_tx, results_rx = trio.open_memory_channel(40)
        async with results_tx, results_rx:
            for (address, msg) in address_msgs:
                nursery.start_soon(
                    sender_with_timeout, nursery, address, msg, results_tx.clone()
                )
            results_by_addr = await client_send_success_reporter(results_rx)
    return results_by_addr


if __name__ == "__main__":
    # Run a test socket server from this script

    async def print_results(read_chan: trio.abc.ReceiveChannel):
        async with read_chan:
            async for msg in read_chan:
                print(msg)

    async def main():
        async with trio.open_nursery() as nursery:
            results_tx, results_rx = trio.open_memory_channel(40)
            async with results_tx, results_rx:
                nursery.start_soon(print_results, results_rx)
                await listen_server(("127.0.0.1", 5000), results_tx)

    trio.run(main)
