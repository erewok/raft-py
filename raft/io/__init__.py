from typing import TypeAlias

HEADER_LEN = 10
DEFAULT_MSG_LEN = 4096
DEFAULT_REQUEST_TIMEOUT = 10
LISTENER_SERVER_CLIENT_TTL = 120  # 2 minutes
SHUTDOWN_CMD = b"SHUTDOWN"
# Type aliases
Address: TypeAlias = tuple[str, int]
MsgResponse: TypeAlias = bytes | None
Request: TypeAlias = tuple[Address, bytes]


SERVER_LOG_NAME = "[bold cyan]SocketServer[/]"
CLIENT_LOG_NAME = "[bold blue]SocketClient[/]"
