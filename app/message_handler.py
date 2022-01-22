import asyncio
import typing as t

from aiohttp import ClientSession

from app._types import MessagePayload


"""
MESSAGE PROCESSING HAPPENS HERE. ANY IO WILL DO JUST FINE.

FOR TEST PURPOSES WE COLLECT A BUNCH OF INFORMATION BASED ON THE
PERSON'S NAME KEKL
"""


_URLS = [
    "https://api.agify.io?name={}",
    "https://api.genderize.io?name={}",
    "https://api.nationalize.io?name={}",
]


async def make_request(session: ClientSession, url: str) -> t.Any:
    response = await session.get(url)
    return await response.text()


async def handle_message(
    message: MessagePayload, session: ClientSession
) -> t.Any:
    # TODO: Dont forget to timeout requests :)
    name = message.get("name")
    index = message.get("index")

    responses = await asyncio.gather(
        *(make_request(session, url.format(name)) for url in _URLS)
    )
    print(f"\nProcessing {name} {index}. Got response {responses}")

    # TODO: Post process responses to build some meaninful response
    response_message = {
        "name": name,
        "index": index,
        "age": None,
        "gender": None,  # no assumptions
        "nationality": None,
    }
    return response_message
