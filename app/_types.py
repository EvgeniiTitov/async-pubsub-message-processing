import asyncio
import signal
import typing as t

from aiohttp import ClientSession


__all__ = (
    "SignalHandlerCallable",
    "MessageHandlerCallable",
    "GlobalExceptionHandlerCallable",
    "MessageValidatorCallable",
)


MessagePayload = t.MutableMapping[str, t.Any]


SignalHandlerCallable = t.Callable[
    [asyncio.AbstractEventLoop, t.Optional[signal.Signals]], t.Any
]

MessageHandlerCallable = t.Callable[
    [MessagePayload, ClientSession], t.Awaitable[t.Any]
]

GlobalExceptionHandlerCallable = t.Callable[
    [asyncio.AbstractEventLoop, t.MutableMapping[str, t.Any]], None
]

MessageValidatorCallable = t.Callable[
    [t.Union[str, bytes]], t.Optional[MessagePayload]
]
