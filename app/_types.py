import asyncio
import signal
import typing as t

from gcloud.aio.pubsub import SubscriberMessage


__all__ = (
    "SignalHandlerCallable",
    "MessageHandlerCallable",
    "GlobalExceptionHandlerCallable",
)

SignalHandlerCallable = t.Callable[
    [asyncio.AbstractEventLoop, t.Optional[signal.Signals]], t.Any
]

MessageHandlerCallable = t.Callable[[SubscriberMessage], t.Awaitable[None]]

GlobalExceptionHandlerCallable = t.Callable[
    [asyncio.AbstractEventLoop, t.MutableMapping[str, t.Any]], None
]
