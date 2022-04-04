import logging
import queue
import threading
from typing import Callable

from raft.io import loggers
from . import Event, EventType


logger = logging.getLogger(__name__)


class ThreadedClock:
    def __init__(
        self,
        event_queue: queue.Queue[EventType],
        interval: float = 1.0,
        interval_func: Callable[[], float] = None,
        event_type: EventType = EventType.Tick,
    ):
        self.interval = interval
        self.interval_func = interval_func
        self.event_queue = event_queue
        self.event_type = event_type
        self.command_event: threading.Event = threading.Event()
        self.thread = None
        self._log_name = f"[Clock - {str(self.event_type)}]"
        if loggers.RICH_HANDLING_ON:
            self._log_name = f"[[yellow]Clock[/] - [blue]{str(self.event_type)}[/]]"

    def start(self):
        logger.info(f"{self._log_name} is starting up")
        if self.thread is None:
            self.thread = threading.Thread(
                target=self.generate_ticks, args=(self.event_queue,)
            )
        self.thread.start()

    def generate_ticks(self, event_q):
        interval = self.interval_func() if self.interval_func else self.interval
        while not self.command_event.wait(interval):
            logger.debug(f"{self._log_name}")
            event_q.put(Event(self.event_type, None))
        logger.info(f"{self._log_name} is shutting down")

    def stop(self):
        self.command_event.set()
        self.thread = None
        self.command_event.clear()
