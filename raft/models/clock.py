import logging
import queue
import threading
from typing import Callable

from raft.internal import trio  # only present if extra "async" installed
from raft.io import loggers

from . import Event, EventType

logger = logging.getLogger(__name__)


class ThreadedClock:
    """
    A Clock implementation can be used for any internal Raft
    clock, such as an election-timeout or heartbeat timer (from the Leader).

    It accepts an `interval_func` or a discrete `interval`, the idea
    being that with an election timeout, we'd like to have a randomized
    interval and a function can compute a new random timeout anew each time.

    It uses a `threading.Event` to know when to stop ticking,
    and an `queue.Queue` to send all of its ticks to.
    """

    def __init__(
        self,
        event_queue: queue.Queue[EventType],
        interval: float = 1.0,  # seconds
        interval_func: Callable[[], float] = None,
        event_type: EventType = EventType.Tick,
    ):
        self.interval = interval
        self.interval_func = interval_func
        self.event_queue = event_queue
        self.event_type = event_type
        self.command_event: threading.Event = threading.Event()
        self.thread = None
        self._log_name = f"[Clock.{id(self)} - {str(self.event_type)}]"
        if loggers.RICH_HANDLING_ON:
            self._log_name = (
                f"[[yellow]Clock[/].{id(self)} - [blue]{str(self.event_type)}[/]]"
            )

    def start(self):
        if self.thread is None:
            self.thread = threading.Thread(
                target=self.generate_ticks, args=(self.event_queue,)
            )
        self.thread.start()

    def generate_ticks(self, event_q):
        interval = self.interval_func() if self.interval_func else self.interval
        logger.info(f"{self._log_name} starting up with interval {interval}")
        while not self.command_event.wait(interval):
            logger.info(f"{self._log_name} tick")
            event_q.put(Event(self.event_type, None))
        logger.info(f"{self._log_name} is shutting down")

    def stop(self):
        self.command_event.set()
        self.thread.join()
        self.thread = None
        self.command_event.clear()


class AsyncClock:
    """
    An AsyncClock implementation can be used for any internal Raft
    clock, such as an election-timeout or heartbeat timer (from the Leader).

    It accepts an `interval_func` or a discrete `interval`, the idea
    being that with an election timeout, we'd like to have a randomized
    interval and a function can compute a new random timeout anew each time.

    This clock implementation expect a `trio.SendChannel` to send events to.
    """

    def __init__(
        self,
        send_channel: trio.abc.SendChannel,
        interval: float = 1.0,  # seconds
        interval_func: Callable[[], float] = None,
        event_type: EventType = EventType.Tick,
    ):
        self.interval = interval
        self.interval_func = interval_func
        self.send_channel = send_channel
        self.event_type = event_type
        self.command_event: trio.Event = trio.Event()
        self._log_name = f"[Clock.{id(self)} - {str(self.event_type)}]"
        if loggers.RICH_HANDLING_ON:
            self._log_name = (
                f"[[yellow]Clock[/].{id(self)} - [blue]{str(self.event_type)}[/]]"
            )

    async def start(self):
        await self.generate_ticks(self.send_channel)

    async def generate_ticks(self, send_channel: trio.abc.SendChannel):
        interval = self.interval_func() if self.interval_func else self.interval
        logger.info(f"{self._log_name} starting up with interval {interval}")
        async with send_channel:
            while not self.command_event.is_set():
                await trio.sleep(interval)
                logger.info(f"{self._log_name} tick")
                await send_channel.send(Event(self.event_type, None))

    def stop(self):
        logger.info(f"{self._log_name} is shutting down")
        self.command_event.set()
