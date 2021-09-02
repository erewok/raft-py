import pytest

from raft.models import log


@pytest.fixture()
def entry():
    return log.LogEntry(1, b"start up")


@pytest.fixture()
def test_ordering(entry):
    entry.term = 2
    e1 = log.LogEntry(1, b"zzz")
    e3 = log.LogEntry(1, b"xxx")
    e4 = log.LogEntry(3, b"xxx")
    assert list(sorted((entry, e4, entry, e1, e3))) == [e1, e3, entry, e4]


@pytest.fixture()
def raft_log():
    return log.Log()


@pytest.fixture()
def raft_log_with_entries():
    e0 = log.LogEntry(1, b"a")
    e1 = log.LogEntry(2, b"b")
    e2 = log.LogEntry(3, b"c")
    e3 = log.LogEntry(3, b"d")
    e4 = log.LogEntry(4, b"e")
    e5 = log.LogEntry(5, b"f")
    e6 = log.LogEntry(5, b"g")
    rlg = log.Log()
    rlg.append_entries(-1, -1, [e0, e1, e2, e3, e4, e5, e6])
    return rlg


def test_append_entries(entry, raft_log):
    # should work
    assert raft_log.append_entries(1, 1, [])

    # put the first one in
    assert raft_log.append_entries(-1, 1, [entry])
    assert raft_log.log == [entry]
    # putting the same one in should be idempotent
    assert raft_log.append_entries(-1, 1, [entry])
    assert raft_log.log == [entry]
    # We can now add another one
    assert raft_log.append_entries(0, 1, [entry])
    assert raft_log.log == [entry, entry]
    # try empty again with a populated log
    assert raft_log.append_entries(1, 1, [])


def test_append_entries_failing(entry, raft_log):
    # no prior entry: should fail
    assert not raft_log.append_entries(1, 1, [entry])
    assert raft_log.log == []


def test_append_entries_full(entry, raft_log_with_entries):
    # mismatched term
    assert not raft_log_with_entries.append_entries(0, 0, [entry])
    # matching term and index
    assert raft_log_with_entries.append_entries(0, 1, [entry])


def test_append_entries_delete_end(entry, raft_log_with_entries):
    # mismatched term
    assert not raft_log_with_entries.append_entries(7, 4, [log.LogEntry(5, b"g")])
    #  "If an existing entry conflicts with a new one (same index, but different terms)"
    # delete the existing entry and all that follow it
    entries = [log.LogEntry(3, b"fixed-idx-3"), log.LogEntry(5, b"fixed-idx-4")]
    assert len(raft_log_with_entries.log) == 7
    assert raft_log_with_entries.append_entries(2, 3, entries)
    assert len(raft_log_with_entries.log) == 5


def test_figure_7(log_maker, figure7_logs):
    """Dave's test for paper figure 7 copied in here"""
    leader_log = figure7_logs["leader_log"]
    a_log = figure7_logs["a_log"]
    b_log = figure7_logs["b_log"]
    c_log = figure7_logs["c_log"]
    d_log = figure7_logs["d_log"]
    e_log = figure7_logs["e_log"]
    f_log = figure7_logs["f_log"]

    # succeeds
    assert leader_log.append_entries(9, 6, [log.LogEntry(8, b"x")])
    # fails
    assert not a_log.append_entries(9, 6, [log.LogEntry(8, b"x")])
    # fails
    assert not b_log.append_entries(9, 6, [log.LogEntry(8, b"x")])

    # works
    assert c_log.append_entries(9, 6, [log.LogEntry(8, b"x")])
    assert c_log == leader_log, c_log
    # Works
    assert d_log.append_entries(9, 6, [log.LogEntry(8, b"x")])
    assert d_log == leader_log
    # Fails
    assert not e_log.append_entries(9, 6, [log.LogEntry(8, b"x")])
    # Fails
    assert not f_log.append_entries(9, 6, [log.LogEntry(8, b"x")])
