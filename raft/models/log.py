from typing import List


class LogEntry:

    __slots__ = ["command", "term"]

    def __init__(self, term: int, data: bytes):
        self.command = data
        self.term = term

    def __eq__(self, other):
        return self.command == other.command and self.term == other.term

    def __repr__(self) -> str:
        return f"[T.{self.term}]: cmd={self.command.decode()}"

    def to_dict(self):
        return {"command": self.command.decode(), "term": self.term}

    @classmethod
    def from_json(cls, item):
        return cls(item.get("term"), item.get("command", "").encode("utf-8"))


class Log:
    """
    Note: this is a *zero-indexed* version of Raft's Log.

    We decrement all Raft-indexes when using them as list indices as a result!

    Log must be written to disk before the server replies back to the leader.
    """

    def __init__(self):
        self.log: List[LogEntry] = []

    def __eq__(self, other):
        return self.log == other.log

    def __len__(self):
        return len(self.log)

    def __repr__(self):
        return ", ".join(map(repr, self.log))

    def __getitem__(self, index) -> LogEntry:
        return self.log[index]

    def flush_to_storage(self, storage):
        pass

    def append_entries(
        self, prev_index: int = -1, prev_term: int = -1, entries: List[LogEntry] = None
    ) -> bool:
        """
        NOTE: Assume `prev_index` is using 0-based indexing UNLIKE as specified in the paper!
        This means that the _first_ element will have a `prev_index` of `-1`.

        > If two entries in different logs have the same index and term, then they store
        the same command.
        > If two entries in different logs have the same index and term, then the logs are
        identical in all preceding entries.

        This is almost entirely Dabeaz's implementation.
        """
        if not entries:
            return True
        if len(self.log) <= prev_index:
            # Invariant: no holes allowed
            return False

        if prev_index >= 0:
            if self.log[prev_index].term != prev_term:
                return False

        next_index = prev_index + 1
        insertion_point = slice(next_index, next_index + len(entries))
        # "If an existing entry conflicts with a new one (same index, but different terms)"
        # delete the existing entry and all that follow it
        for n, (existing_entry, new_entry) in enumerate(
            zip(self.log[insertion_point], entries)
        ):
            if existing_entry.term != new_entry.term:
                del self.log[next_index + n :]
                break

        self.log[insertion_point] = entries
        return True
