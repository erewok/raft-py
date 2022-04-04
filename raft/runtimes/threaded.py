import logging
import queue
import threading

from raft.io import loggers, transport
from raft.models import EVENT_CONVERSION_TO_FOLLOWER, EVENT_START_HEARTBEAT, Event, EventType  # noqa
from raft.models.helpers import Clock, Config
from raft.models.rpc import MsgType, RpcBase, parse_msg  # noqa
from raft.models.server import Follower, Leader, Server
from .base import BaseEventController, BaseRuntime


logger = logging.getLogger(__name__)


class ThreadedEventController(BaseEventController):
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

    def __init__(self, node_id: int, config: Config, termination_sentinel=None):
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

        # thread communication queues
        self.command_event: threading.Event = threading.Event()
        # messages inbound should be placed here
        self.inbound_msg_queue: queue.Queue[bytes] = queue.Queue(maxsize=20)
        # inbound messages get translated to events here: InboundMsg -> Event
        self.events: queue.Queue[Event] = queue.Queue(maxsize=20)
        # outbound messages placed here will be sent out
        self.outbound_msg_queue: queue.Queue[transport.Request] = queue.Queue()

        self._log_name = f"[EventController]"
        if loggers.RICH_HANDLING_ON:
            self._log_name = f"[[green]EventController[/]]"

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
        logger.info(f"{self._log_name} Start: process inbound messages")
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
                    f"{self._log_name} turned item {str(item)} "
                    f"into {event_type} even with qsize now {self.events.qsize()}"
                )
            )
        logger.info(f"{self._log_name} Stop: process inbound messages")

    def process_outbound_msgs(self):
        logger.info(f"{self._log_name} Start: process outbound messages")
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
            logger.debug(f"{self._log_name} sent item {msg_bytes.decode()}")
        logger.info(f"{self._log_name} Stop: process outbound messages")

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


class ThreadedRuntime(BaseRuntime):
    def __init__(self, node_id: int, config: Config, storage_factory):
        storage = storage_factory(node_id, config)
        self.debug = config.debug
        self.instance: Server = Follower(node_id, config, storage)
        self.termination_sentinel = object()
        self.event_controller = ThreadedEventController(
            node_id, config, termination_sentinel=self.termination_sentinel
        )
        self.command_q: queue.Queue[bool] = queue.Queue(maxsize=1)
        self.thread = None
        self.runtime_events = set(
            (
                EventType.DEBUG_REQUEST,
                EventType.ResetElectionTimeout,
                EventType.ConversionToFollower,
                EventType.ConversionToLeader,
                EventType.StartHeartbeat
            )
        )

    @property
    def log_name(self):
        if loggers.RICH_HANDLING_ON:
            return f"[[green]Runtime[/] - {self.instance.log_name()}]"
        return f"[Runtime - {self.instance.log_name()}]"

    def handle_debug_event(self, _: Event):
        no_dump_keys = {"config", "transfer_attrs", "log"}
        if self.debug:
            logger.info(f"{self.log_name} DEBUGGING Event")
            logger.info(f"{self.log_name} is currently {self.instance.__class__.log_name()}")
            for key in filter(
                lambda el: el not in no_dump_keys, self.instance.transfer_attrs
            ):
                value = getattr(self.instance, key)
                logger.info(f"\t`{key}`: \t {str(value)}")
            logger.info(f"\t`Log`: \t {repr(self.instance.log)}")

    def handle_reset_election_timeout(self, _: Event):
        if self.debug:
            logger.info(f"{self.log_name} resetting election timer")
        self.event_controller.run_election_timeout_timer()

    def handle_start_heartbeat(self, _: Event):
        if self.debug:
            logger.info(f"{self.log_name} starting heartbeat")
            logger.info(f"{self.log_name} is currently {self.instance.__class__.log_name()}")
        self.event_controller.run_heartbeat()

    def runtime_handle_event(self, event):
        """For events that need to interact with the runtime"""
        if event.type == EventType.DEBUG_REQUEST:
            self.handle_debug_event(event)
        elif event.type == EventType.ResetElectionTimeout:
            self.handle_reset_election_timeout(event)
        elif event.type == EventType.ConversionToFollower:
            logger.info(f"{self.log_name} Converting to {Follower.log_name()}")
            self.handle_reset_election_timeout(event)
        elif event.type == EventType.ConversionToLeader:
            logger.info(f"{self.log_name} Converting to {Leader.log_name()}")
        elif event.type == EventType.StartHeartbeat:
            self.handle_start_heartbeat(event)

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
            f"{self.log_name} Handling event: EventType={event.type} MsgType={msg_type}"
        )
        self.instance, (responses, more_events) = self.instance.handle_event(event)
        if responses:
            logger.info(f"RaftNode Event OutboundMsg={len(responses)}")
        if more_events:
            logger.info(
                f"{self.log_name} Event FurtherEventCount={len(more_events)}"
            )
        for response in responses:
            self.event_controller.add_response_to_queue(response)

        for further_event in more_events:
            if further_event.type in self.runtime_events:
                self.runtime_handle_event(further_event)
            else:
                self.event_controller.events.put(further_event)

    def run_event_handler(self):
        logger.warn(f"{self.log_name} Start: primary event handler")
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
            logger.debug(
                (
                    f"{self.log_name} Handled event with qsize now "
                    f"{self.event_controller.events.qsize()}"
                )
            )
        logger.warn(f"{self.log_name} Stop: Shutting down primary event handler")

    def run(self, foreground=True):
        if self.debug:
            fg_or_bg = "foreground" if foreground else "background"
            logger.warn(f"{self.log_name} is starting in DEBUG mode")
            logger.warn(f"{self.log_name} will run in the {fg_or_bg} ")

        # Trigger event that makes it so that the correct timers start/stop
        self.event_controller.run()
        self.event_controller.events.put(EVENT_CONVERSION_TO_FOLLOWER)

        if foreground:
            # Start Runtime's event-handler in main thread
            self.run_event_handler()
        else:
            # start the Runtime's event handler in a separate thread
            self.thread = threading.Thread(target=self.run_event_handler)
            self.thread.start()

    def stop(self):
        self.event_controller.stop()
        try:
            self.command_q.put(True)
        except queue.Full:
            pass
        if self.thread is not None:
            self.thread.join()
            self.thread = None

    def __enter__(self):
        return self

    def __exit__(self, *excs):
        self.stop()
