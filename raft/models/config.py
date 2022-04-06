import configparser
from functools import partial
import logging
import random
from functools import cached_property


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
