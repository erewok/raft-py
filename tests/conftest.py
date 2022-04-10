import configparser
import os

import pytest

from raft.io.storage import InMemoryStorage
from raft.models.config import Config
from raft.models import log
from raft.models import rpc
from raft.models import server


test_dir = os.path.dirname(__file__)
root_dir = os.path.dirname(test_dir)


@pytest.fixture()
def config():
    _conf = configparser.ConfigParser()
    raft_ini = os.path.join(root_dir, "raft.ini")
    _conf.read(raft_ini)
    conf = Config(_conf)
    conf.election_timeout = 1
    conf.election_timeout_ms = 0.001
    conf.heartbeat_timeout_ms = conf.election_timeout_ms / conf.heartbeat_interval
    return conf


@pytest.fixture()
def storage(config):
    return InMemoryStorage(1, config)


@pytest.fixture()
def log_maker():
    def make_log(terms):
        rlg = log.Log()
        rlg.log = [log.LogEntry(term, b"x") for term in terms]
        return rlg

    return make_log


# These fixtures build sample logs from the Raft paper's Figure 7
@pytest.fixture()
def fig7_leader_log(log_maker):
    return log_maker([1, 1, 1, 4, 4, 5, 5, 6, 6, 6])


@pytest.fixture()
def figure7_logs(log_maker, fig7_leader_log):
    return dict(
        leader_log=fig7_leader_log,
        a_log=log_maker([1, 1, 1, 4, 4, 5, 5, 6, 6]),
        b_log=log_maker([1, 1, 1, 4]),
        c_log=log_maker([1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6]),
        d_log=log_maker([1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7]),
        e_log=log_maker([1, 1, 1, 4, 4, 4, 4]),
        f_log=log_maker([1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3]),
    )


@pytest.fixture()
def fig7_a_log(figure7_logs):
    return figure7_logs.get("a_log")


@pytest.fixture
def fig7_sample_message():
    return rpc.AppendEntriesRpc(
        **{
            "term": 7,
            "leader_id": 1,
            "prev_log_index": 9,
            "prev_log_term": 6,
            "entries": [log.LogEntry(8, b"x")],
            "leader_commit_index": 9,
            "dest": ("127.0.0.1", 3112),
            "source": ("127.0.0.1", 3111),
        }
    )


@pytest.fixture
def request_vote_message():
    return rpc.RequestVoteRpc(
        **{
            "term": 8,
            "candidate_id": 1,
            "last_log_index": 10,
            "last_log_term": 8,
            "dest": ("127.0.0.1", 3111),
            "source": ("127.0.0.1", 3112),
        }
    )


@pytest.fixture()
def candidate(config, storage):
    return server.Candidate(1, config, storage)


@pytest.fixture()
def follower(config, storage):
    return server.Follower(1, config, storage)


@pytest.fixture()
def leader(config, storage, fig7_leader_log):
    instance = server.Leader(1, config, storage)
    instance.log = fig7_leader_log
    instance.current_term = instance.log[-1].term
    instance.next_index = {1: 8, 2: 5, 3: 8, 4: 2, 5: 7}  # leader
    instance.match_index = {1: 8, 2: 5, 3: 8, 4: 2, 5: 7}  # leader
    return instance


@pytest.fixture()
def sample_append_confirm_rpc():
    return rpc.AppendEntriesResponse(1, 1, 1, True)


@pytest.fixture()
def debug_msg():
    return rpc.Debug()
