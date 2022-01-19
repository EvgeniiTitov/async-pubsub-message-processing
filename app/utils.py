import asyncio
import typing as t
import time
import logging
import sys
import signal
import argparse

from app._types import SignalHandlerCallable, GlobalExceptionHandlerCallable


__all__ = (
    "LoggerMixin",
    "accumulate_batch_within_timeout",
    "get_remaining_messages",
    "configure_event_loop",
    "parse_arguments",
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


def _register_signals(
    loop: asyncio.AbstractEventLoop,
    signals: t.Sequence[signal.Signals],
    signal_handler: SignalHandlerCallable,
) -> None:
    for signal_ in signals:
        loop.add_signal_handler(
            signal_,
            lambda s=signal_: asyncio.create_task(signal_handler(loop, s)),
        )


def configure_event_loop(
    loop: asyncio.AbstractEventLoop,
    signal_handler: SignalHandlerCallable,
    global_exception_callback: GlobalExceptionHandlerCallable,
    debug: bool = True,
) -> None:
    loop.set_debug(debug)
    _register_signals(
        loop=loop,
        signals=(signal.SIGHUP, signal.SIGTERM, signal.SIGINT),
        signal_handler=signal_handler,
    )
    loop.set_exception_handler(global_exception_callback)


def parse_arguments() -> t.MutableMapping[str, int]:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pull_size", type=int, default=5)
    parser.add_argument("--num_pullers", type=int, default=2)
    parser.add_argument("--msg_q_size", type=int, default=100)
    parser.add_argument("--ack_q_size", type=int, default=100)
    parser.add_argument("--nack_q_size", type=int, default=50)
    return vars(parser.parse_args())
