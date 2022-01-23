import asyncio
import typing as t

from aiohttp import ClientSession

from app._types import MessagePayload
from app.exceptions import MessageProcessingError


"""
MESSAGE PROCESSING HAPPENS HERE. ANY IO WILL DO JUST FINE.

FOR TEST PURPOSES WE COLLECT A BUNCH OF INFORMATION BASED ON THE
PERSON'S NAME KEKL
"""


_URLS = [
    "https://api.agify.io?name={name}",
    "https://api.genderize.io?name={name}",
    "https://api.nationalize.io?name={name}",
]


async def make_request(session: ClientSession, url: str) -> str:
    response = await session.get(url)
    return await response.text()


async def make_requests(
    session: ClientSession,
    urls: t.Sequence[str],
    params: t.MutableMapping[str, t.Any],
) -> tuple:
    return await asyncio.gather(
        *(make_request(session, url.format(**params)) for url in urls),
        return_exceptions=True,
    )


async def handle_message(
    message: MessagePayload, session: ClientSession, timeout: float = 2.0
) -> t.Any:
    name, index = message.get("name"), message.get("index")
    if not all((name, index)):
        raise MessageProcessingError("Incorrect message schema")
    try:
        responses = await asyncio.wait_for(
            asyncio.shield(
                make_requests(session, _URLS, params={"name": name})
            ),
            timeout=timeout,
        )
    except asyncio.exceptions.TimeoutError:
        raise MessageProcessingError("Timeout occurred, request(s) got stuck?")

    for response in responses:
        if isinstance(response, Exception):
            raise MessageProcessingError(
                "One or more requests returned exception"
            )

    print(f"\nProcessing {name} {index}. Got response {responses}")

    # TODO: Post process responses to build some meaningful response
    response_message = {
        "name": name,
        "index": index,
        "age": None,
        "gender": None,  # no assumptions
        "nationality": None,
    }
    return response_message
