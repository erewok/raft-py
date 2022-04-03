import configparser
import logging
import queue
import random
import threading
from functools import cached_property
from typing import Callable

from raft.io import loggers
from . import Event, EventType

logger = logging.getLogger(__name__)


class Config:
    def __init__(self, conf: configparser.ConfigParser):
        self.conf = conf
        self.debug = True if conf["Cluster"]["Debug"] == "True" else False
        self.data_directory = conf["Cluster"]["DataDirectory"]
        self.heartbeat_interval = int(conf["Cluster"]["HeartbeatInterval"])
        self.election_timeout = int(conf["Cluster"]["ElectionTimeout"])
        self.election_timeout_ms = int(conf["Cluster"]["ElectionTimeout"]) / 1000
        self.heartbeat_timeout_ms = self.election_timeout_ms / self.heartbeat_interval
        self.node_count = int(conf["Cluster"]["NodeCount"])
        self.storage_class = conf["Cluster"]["StorageClass"]

    @cached_property
    def node_mapping(self):
        all_nodes = {}
        for n in range(1, self.node_count + 1):
            node_label = self.conf["Nodes"][f"Node{n}"]
            node_conf = self.conf[f"Node.{node_label}"]
            addr = (node_conf["Host"], int(node_conf["Port"]))
            all_nodes[n] = {"label": node_conf["Label"], "addr": addr}
        return all_nodes

    @property
    def get_election_timeout(self):
        def inner():
            return (
                random.randint(self.election_timeout, self.election_timeout * 2) / 1000
            )

        return inner


class Clock:
    def __init__(
        self,
        event_queue: queue.Queue[EventType],
        interval: float = 1.0,
        interval_func: Callable[[], float] = None,
        event_type: EventType = EventType.Tick,
    ):
        self.interval = interval
        self.interval_func = interval_func
        self.event_queue = event_queue
        self.event_type = event_type
        self.command_event: threading.Event = threading.Event()
        self.thread = None
        self._log_name = f"[Clock - {str(self.event_type)}]"
        if loggers.RICH_HANDLING_ON:
            self._log_name = f"[[yellow]Clock[/] - [blue]{str(self.event_type)}[/]]"

    def start(self):
        logger.info(f"{self._log_name} is starting up")
        if self.thread is None:
            self.thread = threading.Thread(
                target=self.generate_ticks, args=(self.event_queue,)
            )
        self.thread.start()

    def generate_ticks(self, event_q):
        interval = self.interval_func() if self.interval_func else self.interval
        while not self.command_event.wait(interval):
            logger.debug(f"{self._log_name}")
            event_q.put(Event(self.event_type, None))
        logger.info(f"{self._log_name} is shutting down")

    def stop(self):
        self.command_event.set()
        self.thread = None
        self.command_event.clear()
