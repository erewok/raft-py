import logging
import queue

from raft.io import loggers  # noqa
from raft.io import transport
from raft.models import Event, EventType, EVENT_CONVERSION_TO_FOLLOWER  # noqa
from raft.models.helpers import Clock, Config
from raft.models.rpc import RpcBase, MsgType, parse_msg  # noqa
from raft.models.server import Follower, Server
from raft.models.server import LOG_FOLLOWER, LOG_LEADER


logger = logging.getLogger("raft")


class AsyncEventController:
    def __init__(self, node_id, config, termination_sentinel=None):
        self.node_id = node_id
        self.debug = config.debug
        this_node = config.node_mapping[self.node_id]
        self.address: str = this_node["addr"]
        self.heartbeat_timeout_ms = config.heartbeat_timeout_ms
        self.get_election_time = config.get_election_timeout
        self.election_timer = None
        self.heartbeat = None
        # need a singleton to trigger closing states
        self.termination_sentinel = (
            termination_sentinel if termination_sentinel is not None else object()
        )


class AsyncRuntime:
    def __init__(self, node_id: int, config: Config, storage_factory):
        storage = storage_factory(node_id, config)
        self.debug = config.debug
        self.instance: Server = Follower(node_id, config, storage)
        self.termination_sentinel = object()
        self.event_controller = AsyncEventController(
            node_id, config, termination_sentinel=self.termination_sentinel
        )
        self.command_q: queue.Queue[bool] = queue.Queue(maxsize=1)
        self.thread = None

    def run(self):
        pass

    def stop(self):
        pass