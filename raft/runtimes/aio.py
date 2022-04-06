import logging
from typing import Optional

from raft.io import loggers  # noqa
from raft.io import transport_async
from raft.internal import trio  # only present if extra "async" installed
from raft.models import (
    EVENT_CONVERSION_TO_FOLLOWER,
    Event,
    EventType,
    parse_msg_to_event,
)
from raft.models import clock, rpc
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

    def __init__(
        self,
        node_id,
        config,
        nursery: Optional[trio.Nursery] = None,
        command_event: Optional[trio.Event] = None,
    ):
        self.node_id = node_id
        self.debug = config.debug
        this_node = config.node_mapping[self.node_id]
        self.address: str = this_node["addr"]
        self.heartbeat_timeout_ms = config.heartbeat_timeout_ms
        self.get_election_time = config.get_election_timeout
        self.election_timer = None
        self.heartbeat = None
        self.nursery = nursery

        # need a singleton to trigger closing states
        self.command_event: trio.Event = (
            command_event if command_event else trio.Event()
        )
        # messages inbound should be placed here
        self.inbound_send_channel: Optional[trio.abc.SendChannel] = None
        self.inbound_read_channel: Optional[trio.abc.ReadChannel] = None
        self.cancel_scopes: dict[str, trio.Nursery] = {}

        self._log_name = f"[AsyncEventController]"
        if loggers.RICH_HANDLING_ON:
            self._log_name = f"[[bright_cyan]AsyncEventController[/]]"

    def set_nursery(self, nursery: trio.Nursery):
        self.nursery = nursery

    async def run(self, events_channel: trio.abc.SendChannel):
        (inbound_send_channel, inbound_read_channel,) = trio.open_memory_channel(100)
        async with inbound_send_channel, inbound_read_channel:
            self.nursery.start_soon(
                transport_async.listen_server,
                self.address,
                inbound_send_channel.clone(),
            )
            await self.process_inbound_msgs(
                events_channel.clone(), inbound_read_channel.clone()
            )

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
        return event

    async def process_inbound_msgs(
        self,
        events_channel: trio.abc.SendChannel,
        inbound_msg_chan: trio.abc.ReceiveChannel,
    ):
        """
        Received Messages -> Events Queue

        Inbound messages get translated to events here: InboundMsg -> Event

        After that, they are placed on the `events_chan`
        """
        logger.info(f"{self._log_name} Start: process inbound messages")
        async with inbound_msg_chan:
            async with events_channel:
                while not self.command_event.is_set():
                    async for item in inbound_msg_chan:
                        event = await self.client_msg_into_event(item)
                        if event:
                            await events_channel.send(event)
                            logger.info(
                                (
                                    f"{self._log_name} turned item {str(item)} into {event.type} event"
                                )
                            )
                        if self.command_event.is_set():
                            break
        logger.info(f"{self._log_name} Stop: process inbound messages")

    async def send_outbound_msg(self, response: rpc.RPCMessage):
        logger.info(
            f"{self._log_name}: send outbound message {response.type} to {response.dest}"
        )
        results_tx, _ = trio.open_memory_channel(2)
        async with results_tx:
            await transport_async.client_send_msg(
                self.nursery, response.dest, response.to_bytes(), results_tx.clone()
            )

    async def run_heartbeat(self, events_channel: trio.abc.SendChannel):
        # Start Heartbeat timer (only leader should use this...)
        self.stop_heartbeat()
        with trio.CancelScope() as cancel_scope:
            if self.heartbeat is None:
                self.heartbeat = clock.AsyncClock(
                    events_channel,
                    interval=self.heartbeat_timeout_ms,
                    event_type=EventType.HeartbeatTime,
                )
                self.cancel_scopes["heartbeat"] = cancel_scope
                await self.heartbeat.start()

    def stop_heartbeat(self):
        if cancel_scope := self.cancel_scopes.get("heartbeat"):
            cancel_scope.cancel()
            del self.cancel_scopes["heartbeat"]

        if self.heartbeat is not None:
            self.heartbeat.stop()
            self.heartbeat = None

    async def run_election_timeout_timer(self, events_channel: trio.abc.SendChannel):
        """
        Start Election timer with new random interval.
        Followers and Candidates will use this
        """
        self.stop_election_timer()

        with trio.CancelScope() as cancel_scope:
            if self.election_timer is None:
                self.election_timer = clock.AsyncClock(
                    events_channel,
                    interval_func=self.get_election_time,
                    event_type=EventType.ElectionTimeoutStartElection,
                )
                self.cancel_scopes["election_timer"] = cancel_scope
                await self.election_timer.start()

    def stop_election_timer(self):
        if cancel_scope := self.cancel_scopes.get("election_timer"):
            cancel_scope.cancel()
            del self.cancel_scopes["election_timer"]
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
        self.events_send_channel: Optional[trio.abc.SendChannel] = None
        self.events_receive_channel: Optional[trio.abc.ReceiveChannel] = None
        self.nursery = None

    @property
    def log_name(self):
        if loggers.RICH_HANDLING_ON:
            return f"[[bright_cyan]AsyncRuntime[/] - {self.instance.log_name()}]"
        return f"[AsyncRuntime - {self.instance.log_name()}]"

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
        logger.info(f"{self.log_name} resetting election timer")
        self.nursery.start_soon(
            self.event_controller.run_election_timeout_timer,
            self.events_send_channel.clone()
        )

    async def handle_start_heartbeat(self, _: Event):
        logger.info(f"{self.log_name} starting heartbeat")
        self.nursery.start_soon(
            self.event_controller.run_heartbeat, self.events_send_channel.clone()
        )

    async def runtime_handle_event(self, event):
        """For events that need to interact with the runtime"""
        if event.type == EventType.DEBUG_REQUEST:
            await self.handle_debug_event(event)
        elif event.type == EventType.ResetElectionTimeout:
            await self.handle_reset_election_timeout(event)
        elif event.type == EventType.ConversionToFollower:
            logger.info(f"{self.log_name} Converting to {Follower.log_name()}")
            await self.handle_reset_election_timeout(event)
            self.event_controller.stop_heartbeat()
        elif event.type == EventType.ConversionToLeader:
            logger.info(f"{self.log_name} Converting to {Leader.log_name()}")
            self.event_controller.stop_election_timer()
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

        for response in responses:
            self.nursery.start_soon(self.event_controller.send_outbound_msg, response)

        for further_event in more_events:
            if further_event.type in RUNTIME_EVENTS:
                await self.runtime_handle_event(further_event)
            else:
                self.nursery.start_soon(self.events_send_channel.send, further_event)

    async def run_event_handler(self):
        logger.warn(f"{self.log_name}: event handler processing started")
        async with self.events_receive_channel:
            while not self.command_event.is_set():
                async for event in self.events_receive_channel:
                    await self.handle_event(event)
                    if self.command_event.is_set():
                        break
        logger.warn(f"{self.log_name} Stop: Shutting down primary event handler")

    async def run_async(self):
        async with trio.open_nursery() as nursery:
            self.nursery = nursery
            self.event_controller.set_nursery(nursery)
            (
                self.events_send_channel,
                self.events_receive_channel,
            ) = trio.open_memory_channel(40)
            async with self.events_send_channel, self.events_receive_channel:
                nursery.start_soon(
                    self.event_controller.run, self.events_send_channel.clone()
                )
                await self.events_send_channel.send(EVENT_CONVERSION_TO_FOLLOWER)
                await self.run_event_handler()

    def run(self):
        logger.warn(f"{self.log_name} Starting up now")
        trio.run(self.run_async)

    def stop(self):
        self.command_event.set()
        logger.warn(f"{self.log_name} Shutting down now")
