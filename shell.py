import configparser
import json

from raft.io import loggers  # noqa
from raft.io import storage  # noqa
from raft.io.transport import client_send_msg  # noqa
from raft.models import Event, EventType
from raft.models import log
from raft.models import rpc
from raft.models import server
from raft.models.config import Config
from raft.runtimes.threaded import ThreadedRuntime, ThreadedEventController

conf = configparser.ConfigParser()
conf.read("./raft.ini")
config = Config(conf)

ec = ThreadedEventController(1, config)
runner = ThreadedRuntime(1, config, storage.InMemoryStorage)
HEARTBEAT_EVENT = Event(EventType.HeartbeatTime, None)
append_req = json.dumps(
    {
        "term": 11,
        "leader_id": 1,
        "prev_log_index": 11,
        "prev_log_term": 11,
        "entries": [{"command": "a", "term": 11}],
        "leader_commit_index": 1,
        "type": int(rpc.MsgType.AppendEntriesRequest),
    }
).encode("utf-8")


# # # # # # # # # # # # # # # # #
# Figure 7
# # # # # # # # # # # # # # # # #
def make_log(terms):
    rlg = log.Log()
    rlg.log = [log.LogEntry(term, b"x") for term in terms]
    return rlg


fig7_sample_msg = {
    "term": 7,
    "leader_id": 1,
    "prev_log_index": 9,
    "prev_log_term": 6,
    "entries": [{"command": "b", "term": 7}],
    "leader_commit_index": 9,
    "type": int(rpc.MsgType.AppendEntriesRequest),
}
leader_log = make_log([1, 1, 1, 4, 4, 5, 5, 6, 6, 6])
a_log = make_log([1, 1, 1, 4, 4, 5, 5, 6, 6])
b_log = make_log([1, 1, 1, 4])
c_log = make_log([1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6])
d_log = make_log([1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7])
e_log = make_log([1, 1, 1, 4, 4, 4, 4])
f_log = make_log([1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3])


def create_leader(with_fig7_followers=False):
    runner.instance = server.Leader(1, config, storage.InMemoryStorage)
    runner.instance.log = leader_log
    if with_fig7_followers:
        runner.instance.next_index = {1: 8, 2: 5, 3: 8, 4: 2, 5: 7}
        runner.instance.match_index = {1: 8, 2: 5, 3: 8, 4: 2, 5: 7}
    return runner


def create_follower(created_log, node_id=2):
    runner.instance = server.Follower(node_id, config, storage.InMemoryStorage)
    runner.instance.log = created_log
    return runner


def run_heartbeat():
    runner = create_leader()
    runner.instance  # THIS SHOULD BE THE LEADER
    return runner.handle_event(HEARTBEAT_EVENT)


def trigger_debug_log(node_id):
    addr = config.node_mapping[node_id]["addr"]
    client_send_msg(addr, b'{"type": 99}')


def trigger_debug_all():
    for n in range(2, 6):
        trigger_debug_log(n)


def run_simulation():
    create_leader()
    with runner:
        runner.run(foreground=False)
        run_heartbeat()
        trigger_debug_all()


def client_send_req(leader_id, command):
    addr = config.node_mapping[leader_id]["addr"]
    payload = {
        "type": "ClientRequest",
        "body": command,
        "callback_addr": ["127.0.0.1", 9999],
    }
    return client_send_msg(addr, json.dumps(payload).encode("utf-8"))
