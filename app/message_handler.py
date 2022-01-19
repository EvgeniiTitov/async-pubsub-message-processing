import asyncio

from gcloud.aio.pubsub import SubscriberMessage


async def handle_message(message: SubscriberMessage) -> None:
    print(f"Handling message: {message.data}")
    """
    Here things could be done asynchronously - i want to call some api/
    do something to the message before publishing it to another pubsub topic
    """
    await asyncio.sleep(1)
