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


# # # # # # # # # # # # # # # # #
# Message Protocol functions
# # # # # # # # # # # # # # # # #
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
        chunk = await stream.receive_some(
            max_bytes=min(msg_len - bytes_recd, DEFAULT_MSG_LEN)
        )
        if chunk == b"":
            raise RuntimeError("Socket connection broken")
        chunks.append(chunk)
        bytes_recd += len(chunk)
    return b"".join(chunks)


# # # # # # # # # # # # # # # # #
# Socket Server functions
# # # # # # # # # # # # # # # # #
async def handle_socket_client(send_channel: trio.abc.SendChannel, server_stream):
    try:
        msg = await receive_message(server_stream)
        if msg is not None:
            await send_message(server_stream, b"ok")
            await send_channel.send(msg)
    except Exception:
        logger.error(traceback.format_exc())


async def listen_server(address, send_channel: trio.abc.SendChannel):
    async with send_channel:
        handler = partial(handle_socket_client, send_channel)
        logger.info(f"{SERVER_LOG_NAME} Start: listening at {address[0]}:{address[1]}")
        await trio.serve_tcp(handler, address[1])


# # # # # # # # # # # # # # # # #
# Socket Client functions
# # # # # # # # # # # # # # # # #
async def client_send_msg(
    nursery,
    address: Address,
    msg: bytes,
    result_chan: trio.abc.SendChannel,
    timeout: int = DEFAULT_REQUEST_TIMEOUT,
) -> Optional[bytes]:
    logger.debug(f"{CLIENT_LOG_NAME} connecting to {address[0]}:{address[1]}")
    with trio.move_on_after(timeout):
        try:
            client_stream = await trio.open_tcp_stream(address[0], address[1])
            async with client_stream:
                nursery.start_soon(send_message, client_stream, msg)
                result = await receive_message(client_stream)
                await result_chan.send((address, result))
        except OSError:
            logger.error(f"{CLIENT_LOG_NAME} Send FAIL {address[0]}:{address[1]}")


async def client_send_success_reporter(
    read_chan: trio.abc.ReceiveChannel,
) -> Dict[Address, List[MsgResponse]]:
    results_by_addr: Dict[Address, MsgResponse] = {}  # addr -> bytes result
    async with read_chan:
        async for (address, resp) in read_chan:
            if address not in results_by_addr:
                results_by_addr[address] = []
            results_by_addr[address].append(resp)
    return results_by_addr


async def broadcast_requests(
    address_msgs: List[Request], result_chan: Optional[trio.abc.SendChannel] = None, timeout: int = 1
) -> Dict[Address, List[MsgResponse]]:
    sender_with_timeout = partial(client_send_msg, timeout=timeout)

    async with trio.open_nursery() as nursery:
        if result_chan is None:
            # This is a fire-and-forget case: the caller won't get anything back
            results_tx, results_rx = trio.open_memory_channel(200)

        async with results_tx, results_rx:
            for (address, msg) in address_msgs:
                nursery.start_soon(
                    sender_with_timeout, nursery, address, msg, results_tx.clone()
                )
            results_by_addr = await client_send_success_reporter(results_rx)
    return results_by_addr


if __name__ == "__main__":  # pragma: no cover
    # Run a test socket server from this script
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--server", action="store_true")
    parser.add_argument("-c", "--client", action="store_true")

    ADDRESS = ("127.0.0.1", 5000)

    async def print_results(read_chan: trio.abc.ReceiveChannel):
        async with read_chan:
            async for msg in read_chan:
                print(msg)

    async def client_test():
        async with trio.open_nursery() as nursery:
            results_tx, results_rx = trio.open_memory_channel(40)
            async with results_tx, results_rx:
                nursery.start_soon(print_results, results_rx.clone())
                for n in range(12):
                    data = json.dumps({"test": n, "status": "ok"})
                    await client_send_msg(
                        nursery, ADDRESS, data.encode("utf-8"), results_tx
                    )

    async def server_test():
        async with trio.open_nursery() as nursery:
            results_tx, results_rx = trio.open_memory_channel(40)
            async with results_tx, results_rx:
                nursery.start_soon(print_results, results_rx.clone())
                await listen_server(ADDRESS, results_tx)

    args = parser.parse_args()

    if args.server:
        trio.run(server_test)
    else:
        trio.run(client_test)
