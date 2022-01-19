import asyncio
import typing as t
import time
import logging
import sys


__all__ = (
    "LoggerMixin",
    "accumulate_batch_within_timeout",
    "get_remaining_messages",
)


T = t.TypeVar("T")


logging.basicConfig(
    format="'%(levelname)s - %(filename)s:%(lineno)d -- %(message)s'",
    stream=sys.stdout,
    level=logging.INFO,
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)


class LoggerMixin:
    @property
    def _logger(self) -> logging.Logger:
        name = ".".join([__name__, self.__class__.__name__])
        return logging.getLogger(name)


async def accumulate_batch_within_timeout(
    queue: asyncio.Queue[T], time_window: float
) -> t.List[T]:
    batch = []
    while time_window > 0:
        start = time.perf_counter()
        try:
            message = await asyncio.wait_for(queue.get(), timeout=time_window)
        except asyncio.TimeoutError:
            break
        else:
            batch.append(message)
            time_window -= time.perf_counter() - start
    return batch


async def get_remaining_messages(queue: asyncio.Queue[T]) -> t.List[T]:
    remaining_messages = []
    while True:
        try:
            message = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        else:
            remaining_messages.append(message)
            # TODO: Do I need it? In case I have 1+ Ackers/Nackers?
            await asyncio.sleep(0.1)
    return remaining_messages
