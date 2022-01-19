import asyncio
import logging
import typing as t
import signal
import os

from gcloud.aio.pubsub import SubscriberClient, SubscriberMessage
import aiohttp

from app._types import SignalHandlerCallable
from app.workers import Puller, MessageProcessor, Acker, Nacker
from app.utils import configure_event_loop
from app.message_handler import handle_message


logger = logging.getLogger(__file__)
logging.basicConfig(
    level=logging.INFO,
    format="'%(levelname)s - %(filename)s:%(lineno)d -- %(message)s'",
)


async def consume(
    subscription: str,
    pull_batch: int,
    num_pullers: int,
    message_queue_size: int,
    ack_queue_size: int,
    nack_queue_size: int,
) -> None:
    async with aiohttp.ClientSession() as session:
        subscriber = SubscriberClient(session=session)
        logger.info("Subscriber client initialized")

        message_queue: "asyncio.Queue[SubscriberMessage]" = asyncio.Queue(
            message_queue_size
        )
        ack_queue: "asyncio.Queue[SubscriberMessage]" = asyncio.Queue(
            ack_queue_size
        )
        nack_queue: "asyncio.Queue[SubscriberMessage]" = asyncio.Queue(
            nack_queue_size
        )
        puller_tasks: t.List[asyncio.Task] = []
        msg_processor_tasks: t.List[asyncio.Task] = []
        producer_tasks: t.List[asyncio.Task] = []
        ack_tasks: t.List[asyncio.Task] = []
        nack_tasks: t.List[asyncio.Task] = []

        for i in range(num_pullers):
            puller = Puller(
                subscriber_client=subscriber,
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
            handle_message_callback=handle_message,
            max_concurrent_tasks=100,
        )
        msg_processor_tasks.append(
            asyncio.create_task(message_processor.run_loop())
        )

        acker = Acker(
            ack_queue=ack_queue,
            subscriber_client=subscriber,
            subscription=subscription,
            name="1",
        )
        ack_tasks.append(asyncio.create_task(acker.run_loop()))

        nacker = Nacker(
            nack_queue=nack_queue,
            subscriber_client=subscriber,
            subscription=subscription,
            name="1",
        )
        nack_tasks.append(asyncio.create_task(nacker.run_loop()))

        logger.info("All workers scheduled")
        all_tasks = [
            *puller_tasks,
            *msg_processor_tasks,
            *producer_tasks,
            *ack_tasks,
            *nack_tasks,
        ]
        done, _ = await asyncio.wait(
            all_tasks, return_when=asyncio.FIRST_COMPLETED
        )


async def perform_shutdown(
    loop: asyncio.AbstractEventLoop, sig: t.Optional[signal.Signals] = None
) -> None:
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


def handle_exception(loop, context) -> None:
    message = context.get("exception", context["message"])
    logger.error(f"Caught exception: {message}. Shutting down...")
    asyncio.create_task(perform_shutdown(loop))


def run(
    pull_size: int,
    num_pullers: int,
    msg_q_size: int,
    ack_q_size: int,
    nack_q_size: int,
) -> None:
    subscription = os.environ.get("SUBSCRIPTION")
    if not subscription:
        raise EnvironmentError("Provide SUBSCRIPTION env variable")
    loop = asyncio.get_event_loop()
    configure_event_loop(
        loop=loop,
        signal_handler=perform_shutdown,
        global_exception_callback=handle_exception,
        debug=True,
    )
    try:
        loop.create_task(
            consume(
                subscription=subscription,
                pull_batch=pull_size,
                num_pullers=num_pullers,
                message_queue_size=msg_q_size,
                ack_queue_size=ack_q_size,
                nack_queue_size=nack_q_size,
            )
        )
        loop.run_forever()
    finally:
        loop.close()
        logger.info("Successfully shutdown the app")
