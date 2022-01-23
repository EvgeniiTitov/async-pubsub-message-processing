import typing as t
import json

from pydantic import BaseModel, ValidationError

from app._types import MessagePayload


__all__ = "validate_message"


"""
Here a Pydantic Model and validation function could be defined
"""


class Message(BaseModel):
    name: str
    index: int


def validate_message(
    message: t.Union[str, bytes]
) -> t.Optional[MessagePayload]:
    if isinstance(message, bytes):
        message = str(message.decode("utf-8"))
    try:
        message = json.loads(message)
    except json.decoder.JSONDecodeError:
        raise
    try:
        message = Message(**message).dict()
    except ValidationError:
        raise
    return message


if __name__ == "__main__":
    test_s = b'{"name": "Emma", "index": "3"}'
    res = validate_message(test_s)
    print(res)
