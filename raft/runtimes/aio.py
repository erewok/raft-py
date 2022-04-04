import logging
from typing import List, Optional

from raft.io import loggers  # noqa
from raft.io import transport
from raft.internal import trio  # only present if extra "async" installed
from raft.models import (
    EVENT_CONVERSION_TO_FOLLOWER,
    Event,
    EventType,
    parse_msg_to_event,
    rpc,
)
from raft.models import clock  # noqa
from raft.models.config import Config
from raft.models.clock import AsyncClock
from raft.models.server import Follower, Leader, Server
from .base import BaseEventController, BaseRuntime


logger = logging.getLogger(__name__)


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
        self.channel: Optional[trio.abc.SendChannel] = None
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
        self._log_name = f"[EventController]"
        if loggers.RICH_HANDLING_ON:
            self._log_name = f"[[green]EventController[/]]"

    async def run(self, channel: trio.abc.SendChannel):
        self.channel = channel
        async with self.channel:
            pass

    def client_msg_into_event(self, msg: bytes):
        """Inbound Message -> Events Queue"""
        event = parse_msg_to_event(msg)
        if event is not None:
            if event.type == EventType.DEBUG_REQUEST:
                event.msg.source = self.address
            self.events.put(event)
        return event

    async def process_inbound_msgs(self):
        """Received Messages -> Events Queue"""
        logger.info(f"{self._log_name} Start: process inbound messages")

    async def process_outbound_msgs(self, responses: List[rpc.RPCMessage]):
        logger.info(
            f"{self._log_name} EventController Start: process outbound messages"
        )
        for msg in responses:
            # DEBUGGING FOR NOW!
            # TODO: Build outbound transport
            print(msg)

    async def run_heartbeat(self):
        # Start Heartbeat timer (only leader should use this...)
        if self.heartbeat is None and self.channel is not None:
            async with trio.open_nursery() as nursery:
                async with self.channel:
                    self.heartbeat = clock.AsyncClock(
                        self.channel,
                        interval=self.heartbeat_timeout_ms,
                        event_type=EventType.HeartbeatTime,
                    )
                    nursery.start_soon(self.heartbeat.start)

    async def stop_heartbeat(self):
        if self.heartbeat is not None:
            await self.heartbeat.stop()

    async def run_election_timeout_timer(self):
        """
        Start Election timer with new random interval.
        Followers and Candidates will use this
        """
        await self.stop_election_timer()

        if self.election_timer is None and self.channel is not None:
            async with trio.open_nursery() as nursery:
                async with self.channel:
                    self.election_timer = clock.AsyncClock(
                        self.channel,
                        interval_func=self.get_election_time,
                        event_type=EventType.HeartbeatTime,
                    )
                    nursery.start_soon(self.election_timer.start)

    async def stop_election_timer(self):
        if self.election_timer is not None:
            await self.election_timer.stop()


class AsyncRuntime(BaseRuntime):
    def __init__(self, node_id: int, config: Config, storage_factory):
        storage = storage_factory(node_id, config)
        self.debug = config.debug
        self.instance: Server = Follower(node_id, config, storage)
        self.termination_sentinel = object()
        self.event_controller = AsyncEventController(
            node_id, config, termination_sentinel=self.termination_sentinel
        )
        self.events_tx: Optional[trio.abc.SendChannel] = None
        self.events_rx: Optional[trio.abc.ReceiveChannel] = None

    @property
    def log_name(self):
        if loggers.RICH_HANDLING_ON:
            return f"[[green]Runtime[/] - {self.instance.log_name()}]"
        return f"[Runtime - {self.instance.log_name()}]"

    async def handle_debug_event(self, _: Event):
        no_dump_keys = {"config", "transfer_attrs", "log"}
        if self.debug:
            logger.info(f"{self.log_name} DEBUGGING Event")
            logger.info(
                f"{self.log_name} is currently {self.instance.__class__.log_name()}"
            )
            for key in filter(
                lambda el: el not in no_dump_keys, self.instance.transfer_attrs
            ):
                value = getattr(self.instance, key)
                logger.info(f"\t`{key}`: \t {str(value)}")
            logger.info(f"\t`Log`: \t {repr(self.instance.log)}")

    async def handle_reset_election_timeout(self, _: Event):
        if self.debug:
            logger.info(f"{self.log_name} resetting election timer")
        self.event_controller.run_election_timeout_timer()

    async def handle_start_heartbeat(self, _: Event):
        logger.info(f"{self.log_name} starting heartbeat")
        logger.info(
            f"{self.log_name} is currently {self.instance.__class__.log_name()}"
        )
        await self.event_controller.run_heartbeat()

    async def runtime_handle_event(self, event):
        """For events that need to interact with the runtime"""
        if event.type == EventType.DEBUG_REQUEST:
            await self.handle_debug_event(event)
        elif event.type == EventType.ResetElectionTimeout:
            await self.handle_reset_election_timeout(event)
        elif event.type == EventType.ConversionToFollower:
            logger.info(f"{self.log_name} Converting to {Follower.log_name()}")
            await self.handle_reset_election_timeout(event)
        elif event.type == EventType.ConversionToLeader:
            await logger.info(f"{self.log_name} Converting to {Leader.log_name()}")
        elif event.type == EventType.StartHeartbeat:
            await self.handle_start_heartbeat(event)

    def drop_event(self, event):
        if event.type == EventType.HeartbeatTime and isinstance(
            self.instance, Follower
        ):
            return True
        return False

    async def handle_event(self, event):
        """Primary event handler"""
        msg_type = "none"
        if self.drop_event(event):
            return None
        if event.msg:
            msg_type = event.msg.type
        if event.type in self.runtime_events:
            await self.runtime_handle_event(event)

        logger.info(
            f"{self.log_name} Handling event: EventType={event.type} MsgType={msg_type}"
        )
        self.instance, (responses, more_events) = self.instance.handle_event(event)
        if responses:
            logger.info(f"{self.log_name} Event OutboundMsg={len(responses)}")
        if more_events:
            logger.info(f"{self.log_name} Event FurtherEventCount={len(more_events)}")

        await self.event_controller.process_inbound_msgs(responses)

        for further_event in more_events:
            if further_event.type in self.runtime_events:
                self.runtime_handle_event(further_event)
            else:
                await self.send_channel.send(further_event)

    async def run_event_handler(self):
        logger.warn(f"{self.log_name} Start: primary event handler")
        async with self.receive_channel:
            async for event in self.receive_channel:
                await self.handle_event(event)
                logger.debug(
                    (
                        f"{self.log_name} Handled event with qsize now "
                        f"{self.event_controller.events.qsize()}"
                    )
                )
        logger.warn(f"{self.log_name} Stop: Shutting down primary event handler")

    async def run_async(self):
        async with trio.open_nursery() as nursery:
            self.events_tx, self.events_rx = trio.open_memory_channel(40)
            async with self.events_tx, self.events_rx:
                nursery.start_soon(self.event_controller.run, self.events_tx)
                nursery.start_soon(self.run_event_handler)

    def run(self):
        pass

    def stop(self):
        pass
