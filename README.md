# Raft In Python

This is a work-in-progress attempt to implement Raft in Python.

## Running

To run this using the provided `ini` file, you can invoke it like this using a node-id from 1 to 5:

```sh
$ python -m raft -c raft.ini -n 2
2021-09-03 14:35:46:35.876 [WARNING] raft: *--- RaftNode is starting in DEBUG mode ---*
2021-09-03 14:35:46:35.877 [WARNING] raft: *--- RaftNode will run in the foreground  ---*
2021-09-03 14:35:46:35.877 [INFO] raft: *--- EventController Start: process inbound messages *---
2021-09-03 14:35:46:35.877 [INFO] raft: *--- EventController Start: process outbound messages ---*
2021-09-03 14:35:46:35.877 [WARNING] raft: *--- RaftNode Start: primary event handler ---*
2021-09-03 14:35:46:35.877 [INFO] raft: *--- RaftNode Converting to [Follower] ---*
```

You can simultaneously run raft nodes in other terminals. If you have enough for quorum, one of these will be elected leader:

```sh
2021-09-03 14:36:07:36.313 [WARNING] raft.models.server: Converting Server class from <class 'raft.models.server.Follower'> to <class 'raft.models.server.Candidate'>
2021-09-03 14:36:07:36.313 [INFO] raft: *--- RaftNode Event OutboundMsg=4 ---*
2021-09-03 14:36:07:36.313 [INFO] raft: *--- RaftNode Event FurtherEventCount=0 ---*
2021-09-03 14:36:07:36.313 [INFO] raft: *--- RaftNode Handled event with qsize now 0 ---*
2021-09-03 14:36:07:36.408 [INFO] raft: *--- EventController turned item b'{"term": 2, "vote_granted": true, "source_node_id": 2, "type": 2, "dest": ["127.0.0.1", 3111], "source": ["127.0.0.1", 3112]}' into ReceiveServerCandidateVote even with qsize now 1 ---*
2021-09-03 14:36:07:36.409 [INFO] raft: *--- RaftNode Handling event: EventType=ReceiveServerCandidateVote MsgType=RequestVoteResponse ---*
2021-09-03 14:36:07:36.409 [INFO] raft: *--- RaftNode Handled event with qsize now 0 ---*
2021-09-03 14:36:07:36.448 [INFO] raft: *--- RaftNode Handling event: EventType=ReceiveServerCandidateVote MsgType=RequestVoteResponse ---*
2021-09-03 14:36:07:36.448 [INFO] raft: *--- EventController turned item b'{"term": 2, "vote_granted": true, "source_node_id": 3, "type": 2, "dest": ["127.0.0.1", 3111], "source": ["127.0.0.1", 3113]}' into ReceiveServerCandidateVote even with qsize now 0 ---*
2021-09-03 14:36:07:36.448 [INFO] raft.models.server: [Candidate] has received votes from a quorum of servers
2021-09-03 14:36:07:36.448 [INFO] raft: *--- RaftNode Event OutboundMsg=0 ---*
2021-09-03 14:36:07:36.448 [INFO] raft: *--- RaftNode Event FurtherEventCount=2 ---*
2021-09-03 14:36:07:36.448 [INFO] raft: *--- RaftNode Handled event with qsize now 2 ---*
2021-09-03 14:36:07:36.448 [INFO] raft: *--- RaftNode Handling event: EventType=SelfWinElection MsgType=none ---*
2021-09-03 14:36:07:36.448 [INFO] raft.models.server: [Candidate] has won the election
2021-09-03 14:36:07:36.449 [WARNING] raft.models.server: Converting Server class from <class 'raft.models.server.Candidate'> to <class 'raft.models.server.Leader'>
2021-09-03 14:36:07:36.449 [INFO] raft: *--- RaftNode Event OutboundMsg=0 ---*
2021-09-03 14:36:07:36.449 [INFO] raft: *--- RaftNode Event FurtherEventCount=1 ---*
2021-09-03 14:36:07:36.449 [INFO] raft: *--- RaftNode Converting to [Leader] ---*
```

To send requests into the cluster see the examples in [`shell.py`](./shell.py).
