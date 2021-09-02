import configparser
from functools import cached_property
import logging
import queue
import random
import threading
import time
from typing import Callable

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

    @cached_property
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
        self.command_q: queue.Queue[bool] = queue.Queue(maxsize=1)
        self.thread = None
        self.shutdown = False

    def start(self):
        if not self.command_q.empty():
            # Clear out the queue so we may be restarted
            self.command_q.get()

        logger.debug(f"[Clock - {str(self.event_type)}] is starting up")
        if self.thread is None:
            self.thread = threading.Thread(target=self.generate_ticks)
        self.shutdown = False
        self.thread.start()

    def generate_ticks(self):
        while not self.shutdown:
            if not self.command_q.empty():
                break
            interval = self.interval_func() if self.interval_func else self.interval
            time.sleep(interval)
            if not self.command_q.empty():
                break
            logger.info(f"[Clock] event: {str(self.event_type)}")
            self.event_queue.put(Event(self.event_type, None))

        logger.debug(f"[Clock - {str(self.event_type)}] is shutting down")

    def stop(self):
        self.shutdown = True
        if self.thread is None:
            return
        try:
            self.command_q.put_nowait(True)
        except queue.Full:
            pass
        self.thread.join()
        del self.thread
        self.thread = None
