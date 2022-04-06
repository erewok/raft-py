from typing import Optional, Tuple

from . import loggers

HEADER_LEN = 10
DEFAULT_MSG_LEN = 4096
DEFAULT_REQUEST_TIMEOUT = 10
LISTENER_SERVER_CLIENT_TTL = 120  # 2 minutes
Address = Tuple[str, int]
MsgResponse = Optional[bytes]
Request = Tuple[Address, bytes]
SHUTDOWN_CMD = b"SHUTDOWN"

SERVER_LOG_NAME = "SocketServer"
if loggers.RICH_HANDLING_ON:
    SERVER_LOG_NAME = "[bold cyan]SocketServer[/]"
CLIENT_LOG_NAME = "SocketClient"
if loggers.RICH_HANDLING_ON:
    CLIENT_LOG_NAME = "[bold blue]SocketClient[/]"
