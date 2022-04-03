import enum
import logging
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)
formatter = logging.Formatter(
    "[%(levelname)s] %(asctime)s >> %(message)s", "%Y-%m-%d %H:%M:%S"
)
log_handler = logging.StreamHandler()
log_handler.setLevel(logging.INFO)
log_handler.setFormatter(formatter)
logger.addHandler(log_handler)


@enum.unique
class Command(enum.IntEnum):
    Get = 1
    Set = 2
    Delete = 3

    @classmethod
    def read(cls, val):
        if val.lower() == "get":
            return cls.Get
        if val.lower() == "set":
            return cls.Set
        if val.lower() == "delete":
            return cls.Delete
        raise ValueError(f"Unknown value: {val}")

    def __str__(self):
        return f"Command.{self.name}"


def tokenizer(msg: str) -> List[str]:
    return list(filter(bool, msg.split(" ")))


def parse(msg: str) -> Optional[Tuple[Command, List[str]]]:
    tokens = tokenizer(msg)
    if len(tokens) < 2:
        return None
    first, *rest = tokens
    try:
        cmd = Command.read(first)
    except ValueError:
        return None
    return cmd, rest


def command_to_json(action):
    """
    By this point, the command should have been previously parsed
    """

    def warn_drop_rest(rest):
        if len(rest) > 0:
            logger.warning(f"Dropping remaining instructions {', '.join(rest)}")

    if (
        not action
        or not isinstance(action, tuple)
        or not isinstance(action[0], Command)
    ):
        return "Command is not parseable"
    (cmd, parts) = action

    if cmd is Command.Get:
        key, *rest = parts
        warn_drop_rest(rest)
        return {"command": str(cmd), "key": key, "value": None}
    if cmd is Command.Set:
        key, *vals = parts
        value = " ".join(vals)
        return {"command": str(cmd), "key": key, "value": value}
    if cmd is Command.Delete:
        key, *rest = parts
        warn_drop_rest(rest)
        return {"command": str(cmd), "key": key, "value": None}
