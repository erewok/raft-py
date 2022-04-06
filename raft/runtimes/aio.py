import logging
from typing import List, Optional

from raft.io import loggers  # noqa
from raft.io import transport_async
from raft.internal import trio  # only present if extra "async" installed
from raft.models import (
    Event,
    EventType,
    parse_msg_to_event,
)
from raft.models import clock
from raft.models.config import Config
from raft.models.server import Follower, Leader, Server
from .base import BaseEventController, BaseRuntime, RUNTIME_EVENTS


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

    def __init__(self, node_id, config, command_event: Optional[trio.Event] = None):
        self.node_id = node_id
        self.debug = config.debug
        this_node = config.node_mapping[self.node_id]
        self.address: str = this_node["addr"]
        self.heartbeat_timeout_ms = config.heartbeat_timeout_ms
        self.get_election_time = config.get_election_timeout
        self.election_timer = None
        self.heartbeat = None

        # need a singleton to trigger closing states
        self.command_event: trio.Event = command_event if command_event else trio.Event()
        # messages inbound should be placed here
        self.inbound_send_channel: Optional[trio.abc.SendChannel] = None
        self.inbound_read_channel: Optional[trio.abc.ReadChannel] = None
        # inbound messages get translated to events here: InboundMsg -> Event
        self.events: Optional[trio.abc.SendChannel] = None
        # outbound messages placed here will be sent out
        self.outbound_send_channel: Optional[trio.abc.SendChannel] = None
        self.outbound_read_channel: Optional[trio.abc.SendChannel] = None

        self._log_name = f"[EventController]"
        if loggers.RICH_HANDLING_ON:
            self._log_name = f"[[green]EventController[/]]"

    async def run(self, send_channel: trio.abc.SendChannel):
        self.events = send_channel
        async with trio.open_nursery() as nursery:
            self.inbound_send_channel, self.inbound_read_channel = trio.open_memory_channel(100)
            self.outbound_send_channel, self.outbound_read_channel = trio.open_memory_channel(100)
            async with self.inbound_send_channel, self.inbound_read_channel:
                nursery.start_soon(transport_async.listen_server, self.address, self.inbound_send_channel.clone())
                async with self.outbound_send_channel, self.outbound_read_channel:
                    nursery.start_soon(self.process_outbound_msgs)
                    await self.process_inbound_msgs()

    def stop(self):
        self.stop_election_timer()
        self.stop_heartbeat()
        self.command_event.set()

    async def client_msg_into_event(self, msg: bytes):
        """Inbound Message -> Events Queue"""
        event = parse_msg_to_event(msg)
        if event is not None:
            if event.type == EventType.DEBUG_REQUEST:
                event.msg.source = self.address
            async with self.events:
                await self.events.send(event)
        return event

    async def process_inbound_msgs(self):
        """Received Messages -> Events Queue"""
        logger.info(f"{self._log_name} Start: process inbound messages")
        async with self.inbound_read_channel:
            async for item in self.inbound_read_channel:
                event = await self.client_msg_into_event(item)
                event_type = event.type if event is not None else "none"
                logger.info(
                    (
                        f"{self._log_name} turned item {str(item)} "
                        f"into {event_type} even with qsize now {self.events.qsize()}"
                    )
                )
                if self.command_event.is_set():
                    break
        logger.info(f"{self._log_name} Stop: process inbound messages")

    async def process_outbound_msgs(self):
        logger.info(
            f"{self._log_name} EventController Start: process outbound messages"
        )
        async with self.outbound_read_channel:
            async for item in self.outbound_read_channel:
                async with trio.open_nursery() as nursery:
                    results_tx, _ = trio.open_memory_channel(40)
                    async with results_tx:
                        (addr, msg_bytes) = item
                        await transport_async.client_send_msg(
                            nursery, addr, msg_bytes, results_tx.clone()
                        )
                        if self.command_event.is_set():
                            break

    async def run_heartbeat(self):
        # Start Heartbeat timer (only leader should use this...)
        if self.heartbeat is None and self.events is not None:
            async with trio.open_nursery() as nursery:
                async with self.send_channel:
                    self.heartbeat = clock.AsyncClock(
                        self.events.clone(),
                        interval=self.heartbeat_timeout_ms,
                        event_type=EventType.HeartbeatTime,
                    )
                    nursery.start_soon(self.heartbeat.start)

    def stop_heartbeat(self):
        if self.heartbeat is not None:
            self.heartbeat.stop()

    async def run_election_timeout_timer(self, events_channel: trio.abc.SendChannel):
        """
        Start Election timer with new random interval.
        Followers and Candidates will use this
        """
        self.stop_election_timer()

        if self.election_timer is None:
            async with trio.open_nursery() as nursery:
                async with events_channel:
                    self.election_timer = clock.AsyncClock(
                        events_channel.clone(),
                        interval_func=self.get_election_time,
                        event_type=EventType.HeartbeatTime,
                    )
                    nursery.start_soon(self.election_timer.start)

    def stop_election_timer(self):
        if self.election_timer is not None:
            self.election_timer.stop()
        self.election_timer = None


class AsyncRuntime(BaseRuntime):
    def __init__(self, node_id: int, config: Config, storage_factory):
        storage = storage_factory(node_id, config)
        self.debug = config.debug
        self.instance: Server = Follower(node_id, config, storage)
        self.command_event: trio.Event = trio.Event()
        self.event_controller = AsyncEventController(
            node_id, config, command_event=self.command_event
        )
        self.send_channel: Optional[trio.abc.SendChannel] = None
        self.receive_channel: Optional[trio.abc.ReceiveChannel] = None

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
        await self.event_controller.run_election_timeout_timer(self.send_channel)

    async def handle_start_heartbeat(self, _: Event):
        logger.info(f"{self.log_name} starting heartbeat")
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
        if event.type in RUNTIME_EVENTS:
            await self.runtime_handle_event(event)

        logger.info(
            f"{self.log_name} Handling event: EventType={event.type} MsgType={msg_type}"
        )
        self.instance, (responses, more_events) = self.instance.handle_event(event)
        if responses:
            logger.info(f"{self.log_name} Event OutboundMsg={len(responses)}")
        if more_events:
            logger.info(f"{self.log_name} Event FurtherEventCount={len(more_events)}")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.event_controller.process_inbound_msgs)

            for further_event in more_events:
                if further_event.type in RUNTIME_EVENTS:
                    await self.runtime_handle_event(further_event)
                else:
                    nursery.start_soon(self.send_channel.send, further_event)

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
                if self.command_event.is_set():
                    break
        logger.warn(f"{self.log_name} Stop: Shutting down primary event handler")

    async def run_async(self):
        async with trio.open_nursery() as nursery:
            self.send_channel, self.receive_channel = trio.open_memory_channel(40)
            async with self.send_channel, self.receive_channel:
                nursery.start_soon(self.event_controller.run, self.send_channel.clone())
                await self.run_event_handler()

    def run(self):
        logger.warn(f"{self.log_name} Starting up now")
        trio.run(self.run_async)

    def stop(self):
        self.command_event.set()
