import asyncio
import argparse
import logging
import typing as t
import os
import json

from gcloud.aio.pubsub import PubsubMessage, PublisherClient
import aiohttp


logger = logging.getLogger(__file__)
logging.basicConfig(
    level=logging.INFO,
    format="'%(levelname)s - %(filename)s:%(lineno)d -- %(message)s'",
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch_size", type=int, default=5)
    return parser.parse_args()


def next_number() -> t.Iterator[int]:
    c = 0
    while True:
        yield c
        c += 1


def get_names_pb_message() -> t.Iterator[PubsubMessage]:
    """
    Simulates something actually important such as customers transaction etc.
    """
    names = [
        "Liam",
        "Olivia",
        "Noah",
        "Emma",
        "Oliver",
        "Ava",
        "Elijah",
        "Charlotte",
        "William",
        "Sophia",
        "James",
        "Amelia",
    ]
    next_index = next_number()
    while True:
        for name in names:
            message = dict(name=name, index=str(next(next_index)))
            yield PubsubMessage(str(json.dumps(message)))


def generate_messages_batch(size: int) -> t.Iterator[t.List[PubsubMessage]]:
    get_new_message = get_names_pb_message()
    while True:
        yield [next(get_new_message) for _ in range(size)]


async def publish_messages(batch_size: int) -> None:
    try:
        async with aiohttp.ClientSession() as session:
            logger.info("aiohttp.ClientSession created")
            client = PublisherClient(session=session)
            topic = client.topic_path(
                project=os.environ.get("PROJECT"),
                topic=os.environ.get("TOPIC"),
            )
            logger.info("PubSub client initialized")
            message_generator = generate_messages_batch(batch_size)
            while True:
                batch = next(message_generator)
                logger.info("Generated batch of messages")

                # await client.publish(topic, batch)
                asyncio.create_task(client.publish(topic, batch))

                logger.info("Published a batch of messages")
                await asyncio.sleep(0.25)
    except asyncio.CancelledError:
        logger.info("Message publisher coro cancelled")
        raise


def main() -> int:
    # Thats ugly I dont care any more
    args = parse_arguments()
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    try:
        loop.create_task(publish_messages(args.batch_size))
        logger.info("Main task created")
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Key pressed, stopped generating messages")
    finally:
        loop.close()
    logger.info("Loop closed")
    return 0


if __name__ == "__main__":
    main()
