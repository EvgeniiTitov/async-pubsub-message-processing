import asyncio
import logging
import typing as t
import signal
import os
import functools

from gcloud.aio.pubsub import (
    SubscriberClient,
    SubscriberMessage,
    PublisherClient,
)
import aiohttp

from app._types import SignalHandlerCallable
from app.workers import Puller, MessageProcessor, Acker, Nacker, Publisher
from app.utils import configure_event_loop
from app.message_handler import handle_message
from app.validator import validate_message


__all__ = "run"


logger = logging.getLogger(__file__)
logging.basicConfig(
    level=logging.INFO,
    format="'%(levelname)s - %(filename)s:%(lineno)d -- %(message)s'",
)


async def _consume(
    subscription: str,
    publish_project: str,
    publish_topic: str,
    pull_batch: int,
    publish_batch: int,
    num_pullers: int,
    message_queue_size: int,
    ack_queue_size: int,
    nack_queue_size: int,
    to_publisher_queue_size: int,
    max_concurrent_tasks: int,
) -> None:
    try:
        puller_tasks: t.List[asyncio.Task] = []
        msg_processor_tasks: t.List[asyncio.Task] = []
        publisher_tasks: t.List[asyncio.Task] = []
        ack_tasks: t.List[asyncio.Task] = []
        nack_tasks: t.List[asyncio.Task] = []

        async with aiohttp.ClientSession() as session:
            subscriber_client = SubscriberClient(session=session)
            publisher_client = PublisherClient(session=session)
            logger.info("Subscriber and Publisher clients initialized")

            message_queue: "asyncio.Queue[SubscriberMessage]" = asyncio.Queue(
                message_queue_size
            )
            ack_queue: "asyncio.Queue[str]" = asyncio.Queue(ack_queue_size)
            nack_queue: "asyncio.Queue[str]" = asyncio.Queue(nack_queue_size)
            publisher_queue: "asyncio.Queue[t.Any]" = asyncio.Queue(
                to_publisher_queue_size
            )
            for i in range(num_pullers):
                puller = Puller(
                    subscriber_client=subscriber_client,
                    message_queue=message_queue,
                    subscription=subscription,
                    batch_size=pull_batch,
                    name=str(i),
                )
                puller_tasks.append(
                    asyncio.create_task(puller.run_loop(put_async=False))
                )
            message_processor = MessageProcessor(
                message_queue=message_queue,
                ack_queue=ack_queue,
                nack_queue=nack_queue,
                to_publisher_queue=publisher_queue,
                # Mypy has problems with partial, very cool
                handle_message_callback=functools.partial(
                    handle_message, session=session
                ),
                validate_message_callback=validate_message,
                max_concurrent_tasks=max_concurrent_tasks,
            )
            msg_processor_tasks.append(
                asyncio.create_task(message_processor.run_loop())
            )
            publisher = Publisher(
                publisher_client=publisher_client,
                publisher_queue=publisher_queue,
                project=publish_project,
                topic=publish_topic,
                publish_batch_size=publish_batch,
                name="1",
            )
            publisher_tasks.append(asyncio.create_task(publisher.run_loop()))
            acker = Acker(
                ack_queue=ack_queue,
                subscriber_client=subscriber_client,
                subscription=subscription,
                name="1",
            )
            ack_tasks.append(asyncio.create_task(acker.run_loop()))
            nacker = Nacker(
                nack_queue=nack_queue,
                subscriber_client=subscriber_client,
                subscription=subscription,
                name="1",
            )
            nack_tasks.append(asyncio.create_task(nacker.run_loop()))

            all_tasks = [
                *puller_tasks,
                *msg_processor_tasks,
                *publisher_tasks,
                *ack_tasks,
                *nack_tasks,
            ]
            done, _ = await asyncio.wait(
                all_tasks, return_when=asyncio.ALL_COMPLETED
            )
    except Exception as e:
        logger.error(f"Consume shut down. Error: {e}")
        raise


async def _perform_shutdown(
    loop: asyncio.AbstractEventLoop, sig: t.Optional[signal.Signals] = None
) -> None:
    # TODO: Ugly dependency on logger - pass it in (partial?)
    if sig:
        logger.info(f"Received exit signal {sig.name}")
    tasks = [
        task
        for task in asyncio.all_tasks()
        if task is not asyncio.current_task()
    ]
    _ = [task.cancel() for task in tasks]
    logger.info("Cancelling outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("All outstanding tasks cancelled")
    loop.stop()


def _handle_exception(loop, context) -> None:
    # TODO: Ugly dependency on logger - pass it in (partial?)
    message = context.get("exception", context["message"])
    logger.error(f"Caught exception: {message}. Shutting down...")
    asyncio.create_task(_perform_shutdown(loop))


def run(
    num_pullers: int,
    pull_size: int,
    msg_q_size: int,
    ack_q_size: int,
    to_pb_q_size: int,
    nack_q_size: int,
    max_concur_tasks: int,
    push_size: int,
) -> None:
    subscription = os.environ.get("SUBSCRIPTION")
    project = os.environ.get("PUB_PROJECT")
    topic = os.environ.get("PUB_TOPIC")
    if not all((subscription, project, topic)):
        raise EnvironmentError(
            "Ensure all ENV variables set. "
            "Expected: SUBSCRIPTION, PUB_PROJECT, PUB_TOPIC"
        )
    loop = asyncio.get_event_loop()
    configure_event_loop(
        loop=loop,
        signal_handler=_perform_shutdown,
        global_exception_callback=_handle_exception,
        debug=True,
    )
    try:
        loop.create_task(
            _consume(
                subscription=subscription,
                publish_project=project,
                publish_topic=topic,
                pull_batch=pull_size,
                publish_batch=push_size,
                num_pullers=num_pullers,
                message_queue_size=msg_q_size,
                ack_queue_size=ack_q_size,
                nack_queue_size=nack_q_size,
                max_concurrent_tasks=max_concur_tasks,
                to_publisher_queue_size=to_pb_q_size,
            )
        )
        loop.run_forever()
    finally:
        loop.close()
        logger.info("Successfully shutdown the app")
