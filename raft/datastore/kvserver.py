import json
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
import traceback
import typing

from .parser import Command, parse
from .transport import receive_message, send_message


DATA_STORE: typing.Dict[str, str] = {}


def evaluate_json(msg: bytes) -> str:
    action = json.loads(msg.decode())
    cmd = Command.read(action.get("command").rsplit(".", 1)[-1])
    if not cmd or not isinstance(cmd, Command):
        return "Command is not parseable"

    if cmd is Command.Get:
        key = action.get("key")
        return DATA_STORE.get(key, "NOT PRESENT")
    if cmd is Command.Set:
        key = action.get("key")
        value = action.get("value")
        DATA_STORE[key] = value
        return "ok"
    if cmd is Command.Delete:
        key = action.get("key")
        if key in DATA_STORE:
            del DATA_STORE[key]
            return "ok"
        return "NOT PRESENT"


def evaluate(msg: bytes) -> str:
    msgstr = msg.decode()
    maybe_parse = parse(msgstr)
    if maybe_parse is None:
        return "Command is not parseable"
    (cmd, parts) = maybe_parse

    if cmd is Command.Get:
        key, *_ = parts
        return DATA_STORE.get(key, "NOT PRESENT")
    if cmd is Command.Set:
        key, *vals = parts
        DATA_STORE[key] = " ".join(vals)
        return "ok"
    if cmd is Command.Delete:
        key, *_ = parts
        if key in DATA_STORE:
            del DATA_STORE[key]
            return "ok"
        return "NOT PRESENT"


def handle_client(client, json_mode=False):
    try:
        msg = receive_message(client)
        if msg is not None:
            print(msg)
            action = evaluate_json(msg) if json_mode else evaluate(msg)
            send_message(client, action.encode("utf-8"))
            client.close()
    except Exception:
        print(traceback.format_exc())
    finally:
        client.close()


def dbserver(address, json_mode=False):
    sock = socket(AF_INET, SOCK_STREAM)
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
    sock.bind(address)
    sock.listen()
    print(f"KV Server listening on port {address[1]}")
    while True:
        client, _ = sock.accept()
        handle_client(client, json_mode=json_mode)
    sock.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser("warmup.py")
    parser.add_argument("--domain", default="", type=str)
    parser.add_argument("--port", default=2000, type=int)
    parser.add_argument(
        "--json-mode",
        action="store_true",
        help="Whether to send and receive messages in JSON",
    )
    args = parser.parse_args()
    assert 1000 <= args.port <= 65336, f"Port is not within range {args.port}"
    dbserver((args.domain, args.port), json_mode=args.json_mode)
