import json
from socket import socket, AF_INET, SOCK_STREAM
from .parser import parse, command_to_json
from .transport import receive_message, send_message


def client_send_command(address, cmd, json_mode=False):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.connect(address)
    if json_mode:
        action = parse(cmd)
        cmd = json.dumps(command_to_json(action))
    send_message(sock, cmd.encode("utf-8"))
    response = receive_message(sock)
    print(response.decode())
    sock.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser("warmup.py")
    parser.add_argument("command", type=str, nargs="+")
    parser.add_argument("--domain", default="", type=str)
    parser.add_argument("--port", default=2000, type=int)
    parser.add_argument(
        "--json-mode",
        action="store_true",
        help="Whether to send and receive messages in JSON",
    )
    args = parser.parse_args()
    cmd = " ".join(args.command)
    assert 1000 <= args.port <= 65336, f"Port is not within range {args.port}"
    client_send_command((args.domain, args.port), cmd, json_mode=args.json_mode)
