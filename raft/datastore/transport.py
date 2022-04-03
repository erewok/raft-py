from socket import AF_INET, SOCK_STREAM, socket

HEADER_LEN = 10
DEFAULT_MSG_LEN = 4096


def send_message(sock, msg):
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


# # # # # # # # # # # # # # # # # # #
# Testing
# # # # # # # # # # # # # # # # # # #


def echo_server(address):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.bind(address)
    sock.listen()
    client, _ = sock.accept()
    try:
        while True:
            msg = receive_message(client)
            if msg is None:
                continue
            send_message(client, msg)
    except Exception as err:
        print(err)
    finally:
        client.close()
    sock.close()


def echo_test(address):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.connect(address)
    for n in range(0, 8):
        msg = b"x" * (10**n)  # 1, 10, 100, 1000, 10000, bytes etc...
        print(f"Sending message of length {10**n} (n is {n})")
        send_message(sock, msg)
        response = receive_message(sock)
        assert msg == response
    sock.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--server", action="store_true")
    parser.add_argument("--client", action="store_true")
    parser.add_argument("--domain", default="", type=str)
    parser.add_argument("--port", default=2000, type=int)

    args = parser.parse_args()

    assert 1000 <= args.port <= 65336, f"Port is not within range {args.port}"
    if args.client:
        print("Running client test!")
        echo_test((args.domain, args.port))
    else:
        print("Running server test!")
        echo_server((args.domain, args.port))
