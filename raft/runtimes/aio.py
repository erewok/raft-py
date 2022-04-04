import logging
import queue

from raft.io import loggers  # noqa
from raft.io import transport
from raft.models import EVENT_CONVERSION_TO_FOLLOWER, Event, EventType  # noqa
from raft.models.config import Config
from raft.models.clock import ThreadedClock
from raft.models.rpc import MsgType, RpcBase, parse_msg  # noqa
from raft.models.server import Follower, Server

from .base import BaseEventController, BaseRuntime


logger = logging.getLogger(__name__)


try:
    import trio
except ImportError:
    logger.info("Missing async features: install with `async` to enable")
    trio = None


class AsyncEventController(BaseEventController):
    """The job of this class is to package up 'things that happen'
    into events (see `Event`). The msg queue goes to lower-level socket-listeners.

    The `events` queue will be consumed by the runtime.
    _Some_ events have associated response messages but not all.

    For instance, an incoming RPC message is parsed and turned into an event.

    After it's been turned into an event, this class adds it to the `events`
    queue.

    It's sort of like MsgReceived-Q -> Events-Q -> Maybe Response-Q

    Later on, someone may reply and we need to figure out what to do with those responses.
    """

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

    def add_response_to_queue(self, msg):
        try:
            self.outbound_msg_queue.put_nowait((msg.dest, msg.to_bytes()))  # type: ignore
        except queue.Full:
            self.outbound_msg_queue.put((msg.dest, msg.to_bytes()))

    def client_msg_into_event(self, msg: bytes):
        """Inbound Message -> Events Queue"""
        try:
            result = parse_msg(msg)
        except ValueError:
            return None

        event = None
        if result.type == MsgType.AppendEntriesRequest:
            event = Event(EventType.LeaderAppendLogEntryRpc, result)
        elif result.type == MsgType.AppendEntriesResponse:
            event = Event(EventType.AppendEntryConfirm, result)
        elif result.type == MsgType.RequestVoteRequest:
            event = Event(EventType.CandidateRequestVoteRpc, result)
        elif result.type == MsgType.RequestVoteResponse:
            event = Event(EventType.ReceiveServerCandidateVote, result)
        elif result.type == MsgType.ClientRequest:
            event = Event(EventType.ClientAppendRequest, result)
        elif result.type == MsgType.DEBUG_MESSAGE and self.debug:
            # # # DEBUG EVENT # # #
            result.source = self.address
            event = Event(EventType.DEBUG_REQUEST, result)

        if event:
            self.events.put(event)
        return event

    def process_inbound_msgs(self):
        """Received Messages -> Events Queue"""
        logger.info(f"*--- EventController Start: process inbound messages *---")
        while True:
            if self.command_event.is_set():
                break
            item = self.inbound_msg_queue.get()
            self.inbound_msg_queue.task_done()
            if item is self.termination_sentinel or self.command_event.is_set():
                break
            event = self.client_msg_into_event(item)
            event_type = event.type if event is not None else "none"
            logger.info(
                (
                    f"*--- EventController turned item {str(item)} "
                    f"into {event_type} even with qsize now \x1b[33m{self.events.qsize()}\x1b[0m ---*"
                )
            )
        logger.info(f"*--- EventController Stop: process inbound messages ---*")

    def process_outbound_msgs(self):
        logger.info(f"*--- EventController Start: process outbound messages ---*")
        while True:
            if self.command_event.is_set():
                break

            item = self.outbound_msg_queue.get()
            self.outbound_msg_queue.task_done()
            if item is self.termination_sentinel or self.command_event.is_set():
                break
            logger.debug(f"Outbound_Msg={item}")
            (addr, msg_bytes) = item
            transport.client_send_msg(addr, msg_bytes)
            logger.debug(f"*--- EventController sent item {msg_bytes.decode()} ---*")
        logger.info(f"*--- EventController Stop: process outbound messages ---*")

    def run(self):
        # Launch request listener
        threading.Thread(
            target=transport.listen_server,
            args=(self.address, self.inbound_msg_queue),
            kwargs={"listen_server_event": self.command_event},
        ).start()
        # Launch inbound message processor
        threading.Thread(target=self.process_inbound_msgs).start()
        # Launch outbound message processor
        threading.Thread(target=self.process_outbound_msgs).start()

    def stop(self):
        self.stop_heartbeat()
        self.stop_election_timer()
        transport.client_send_msg(self.address, transport.SHUTDOWN_CMD, 5)
        self.command_event.set()
        self.inbound_msg_queue.put(self.termination_sentinel)
        self.outbound_msg_queue.put(self.termination_sentinel)
        self.events.put(self.termination_sentinel)

    def run_heartbeat(self):
        # self.stop_heartbeat()
        # Start Heartbeat timer (only leader should use this...)
        if self.heartbeat is None:
            self.heartbeat = Clock(
                self.events,
                interval=self.heartbeat_timeout_ms,
                event_type=EventType.HeartbeatTime,
            )
        self.heartbeat.start()

    def stop_heartbeat(self):
        if self.heartbeat is not None:
            self.heartbeat.stop()

    def run_election_timeout_timer(self):
        self.stop_election_timer()

        # Start Election timer with new random interval
        # Followers and Candidates will use this
        if self.election_timer is None:
            self.election_timer = Clock(
                self.events,
                interval_func=self.get_election_time,
                event_type=EventType.ElectionTimeoutStartElection,
            )
        self.election_timer.start()

    def stop_election_timer(self):
        if self.election_timer is not None:
            self.election_timer.stop()


class AsyncRuntime(BaseRuntime):
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

    def handle_debug_event(self, _: Event):
        no_dump_keys = {"config", "transfer_attrs", "log"}
        if self.debug:
            logger.info("*--- RaftNode DEBUGGING Event ---*")
            logger.info(f"*--- RaftNode is currently {self.instance.__class__} ---*")
            for key in filter(
                lambda el: el not in no_dump_keys, self.instance.transfer_attrs
            ):
                value = getattr(self.instance, key)
                logger.info(f"\t`{key}`: \t {str(value)}")
            logger.info(f"\t`Log`: \t {repr(self.instance.log)}")

    def handle_reset_election_timeout(self, _: Event):
        if self.debug:
            logger.info("*--- RaftNode resetting election timer ---*")
        self.event_controller.run_election_timeout_timer()

    def handle_start_heartbeat(self, _: Event):
        if self.debug:
            logger.info("*--- RaftNode starting heartbeat ---*")
            logger.info(f"*--- RaftNode is currently {self.instance.__class__} ---*")
        self.event_controller.run_heartbeat()

    def runtime_handle_event(self, event):
        """For events that need to interact with the runtime"""
        if event.type == EventType.DEBUG_REQUEST:
            self.handle_debug_event(event)
        elif event.type == EventType.ResetElectionTimeout:
            self.handle_reset_election_timeout(event)
        elif event.type == EventType.ConversionToFollower:
            logger.info(f"*--- RaftNode Converting to {LOG_FOLLOWER} ---*")
            self.handle_reset_election_timeout(event)
        elif event.type == EventType.ConversionToLeader:
            logger.info(f"*--- RaftNode Converting to {LOG_LEADER} ---*")

    def drop_event(self, event):
        if event.type == EventType.HeartbeatTime and isinstance(
            self.instance, Follower
        ):
            return True
        return False

    def handle_event(self, event):
        """Primary event handler"""
        msg_type = "none"
        if self.drop_event(event):
            return None
        if event.msg:
            msg_type = event.msg.type
        if event.type in self.runtime_events:
            self.runtime_handle_event(event)

        logger.info(
            f"*--- RaftNode Handling event: EventType={event.type} MsgType={msg_type} ---*"
        )
        self.instance, maybe_responses_events = self.instance.handle_event(event)
        if maybe_responses_events is not None:
            responses, more_events = maybe_responses_events
            responses = responses or []
            more_events = more_events or []
            logger.info(f"*--- RaftNode Event OutboundMsg={len(responses)} ---*")
            logger.info(
                f"*--- RaftNode Event FurtherEventCount={len(more_events)} ---*"
            )
            for response in responses:
                self.event_controller.add_response_to_queue(response)

            for further_event in more_events:
                if further_event.type in self.runtime_events:
                    self.runtime_handle_event(further_event)
                else:
                    self.event_controller.events.put(further_event)

    def run_event_handler(self):
        logger.warn("*--- RaftNode Start: primary event handler ---*")
        while True:
            if not self.command_q.empty():
                break
            try:
                event = self.event_controller.events.get_nowait()
            except queue.Empty:
                continue
            if event is self.termination_sentinel:
                break
            if not self.command_q.empty():
                break

            self.handle_event(event)
            logger.info(
                (
                    "*--- RaftNode Handled event with qsize now "
                    f"\x1b[33m{self.event_controller.events.qsize()}\x1b[0m ---*"
                )
            )
        logger.warn("*--- RaftNode Stop: Shutting down primary event handler ---*")

    async def run(self):
        async with trio.open_nursery() as nursery:
            # Open a channel:
            send_channel, receive_channel = trio.open_memory_channel(0)
            # Start a producer and a consumer, passing one end of the channel to
            # each of them:
            nursery.start_soon(producer, send_channel)
            nursery.start_soon(consumer, receive_channel)

    def stop(self):
        pass
