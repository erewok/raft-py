import trio

from raft.io import transport_async as transport


GLOBAL_ITEMS = []
ADDRESS = ("127.0.0.1", 5000)
BAD_ADDRESS = ("127.0.0.1", 7113)


async def socker_serv_collector(receive_channel: trio.abc.ReceiveChannel):
    async with receive_channel:
        async for item in receive_channel:
            GLOBAL_ITEMS.append(item)


async def test_async_listen_server():
    GLOBAL_ITEMS.clear()
    with trio.move_on_after(2):
        async with trio.open_nursery() as nursery:
            send_channel, receive_channel = trio.open_memory_channel(0)
            client_send_chan, _ = trio.open_memory_channel(0)
            with trio.CancelScope() as cancel_scope:
                async with send_channel, receive_channel:
                    nursery.start_soon(transport.listen_server, ADDRESS, send_channel.clone())
                    nursery.start_soon(socker_serv_collector, receive_channel.clone())
                    await trio.sleep(0.05)
                    await transport.client_send_msg(
                        nursery,
                        ADDRESS,
                        b"This is a test message",
                        client_send_chan.clone()
                    )
                    # This is a failing message
                    await transport.client_send_msg(
                        nursery,
                        BAD_ADDRESS,
                        b"This is a failing message",
                        client_send_chan.clone()
                    )
                    await trio.sleep(0.05)
                    cancel_scope.cancel()
    assert len(GLOBAL_ITEMS) == 1
    assert GLOBAL_ITEMS[0] == b"This is a test message"


async def test_message_broadcasting():
    GLOBAL_ITEMS.clear()

    messages = [
        (ADDRESS, b"test1"),
        (ADDRESS, b"test2"),
        (ADDRESS, b"test3"),
        (ADDRESS, b"test4"),
        (ADDRESS, b"test5"),
        (BAD_ADDRESS, b"fail1"),
        (BAD_ADDRESS, b"fail2"),
        (BAD_ADDRESS, b"fail3"),
        (BAD_ADDRESS, b"fail4"),
        (BAD_ADDRESS, b"fail5"),
    ]
    results = {}
    with trio.move_on_after(2):
        async with trio.open_nursery() as nursery:
            send_channel, receive_channel = trio.open_memory_channel(0)
            with trio.CancelScope() as cancel_scope:
                async with send_channel, receive_channel:
                    nursery.start_soon(transport.listen_server, ADDRESS, send_channel.clone())
                    nursery.start_soon(socker_serv_collector, receive_channel.clone())
                    await trio.sleep(0.05)
                    results = await transport.broadcast_requests(messages)
                    await trio.sleep(0.05)
                    cancel_scope.cancel()
    assert set(GLOBAL_ITEMS) == {b'test2', b'test3', b'test4', b'test1', b'test5'}
    assert len(results) == 5
