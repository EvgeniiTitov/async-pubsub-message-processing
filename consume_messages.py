import asyncio
import logging
import typing as t
import signal

from gcloud.aio.pubsub import SubscriberClient, SubscriberMessage
import aiohttp

from _types import SignalHandlerCallable
from utils import Puller, Consumer, Acker


logger = logging.getLogger(__file__)
logging.basicConfig(
    level=logging.INFO,
    format="'%(levelname)s - %(filename)s:%(lineno)d -- %(message)s'",
)
SUBSCRIPTION = "projects/gcp-wow-rwds-ai-mlchapter-dev/subscriptions/etitov-poc-sample-topic-sub"


async def handle_message(message: SubscriberMessage) -> None:
    logger.info(f"Handling message: {message.data}")
    """
    Here things could be done asynchronously - i want to call some api/
    do something to the message before publishing it to another pubsub topic
    """
    await asyncio.sleep(1)


async def consume(pull_batch: int, num_pullers: int) -> None:
    async with aiohttp.ClientSession() as session:
        subscriber = SubscriberClient(session=session)
        logger.info("Subscriber client initialized")

        message_queue: "asyncio.Queue[SubscriberMessage]" = asyncio.Queue(100)
        ack_queue: "asyncio.Queue[SubscriberMessage]" = asyncio.Queue(100)
        nack_queue: "asyncio.Queue[SubscriberMessage]" = asyncio.Queue(100)

        puller_tasks: t.List[asyncio.Task] = []
        consumer_tasks: t.List[asyncio.Task] = []
        producer_tasks: t.List[asyncio.Task] = []
        ack_tasks: t.List[asyncio.Task] = []
        nack_tasks: t.List[asyncio.Task] = []

        for i in range(num_pullers):
            puller = Puller(
                subscriber_client=subscriber,
                message_queue=message_queue,
                subscription=SUBSCRIPTION,
                name=str(i),
            )
            puller_tasks.append(
                asyncio.create_task(puller.run_loop(put_async=False))
            )
        logger.info("Pullers started")

        consumer = Consumer(
            message_queue=message_queue,
            ack_queue=ack_queue,
            nack_queue=nack_queue,
            handle_message_callback=handle_message,
            max_concurrent_tasks=100,
        )
        consumer_tasks.append(asyncio.create_task(consumer.run_loop()))
        logger.info("Consumer started")

        acker = Acker(
            ack_queue=ack_queue,
            subscriber_client=subscriber,
            subscription=SUBSCRIPTION,
            name="1",
        )
        ack_tasks.append(asyncio.create_task(acker.run_loop()))
        logger.info("Acker started")

        all_tasks = [*puller_tasks, *consumer_tasks]
        done, _ = await asyncio.wait(
            all_tasks, return_when=asyncio.FIRST_COMPLETED
        )


async def perform_shutdown(
    loop, sig: t.Optional[signal.Signals] = None
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


def register_signals(
    loop,
    signals: t.Sequence[signal.Signals],
    signal_handler: SignalHandlerCallable,
) -> None:
    for signal_ in signals:
        loop.add_signal_handler(
            signal_,
            lambda s=signal_: asyncio.create_task(signal_handler(loop, s)),
        )


def handle_exception(loop, context) -> None:
    message = context.get("exception", context["message"])
    logger.error(f"Caught exception: {message}. Shutting down...")
    asyncio.create_task(perform_shutdown(loop))


def configure_event_loop(loop) -> None:
    loop.set_debug(True)
    register_signals(
        loop=loop,
        signals=(signal.SIGHUP, signal.SIGTERM, signal.SIGINT),
        signal_handler=perform_shutdown,
    )
    loop.set_exception_handler(handle_exception)
    logger.info("Loop configured")


def main() -> int:
    batch_size = 5

    loop = asyncio.get_event_loop()
    configure_event_loop(loop)
    try:
        loop.create_task(consume(batch_size, num_pullers=2))
        loop.run_forever()
    finally:
        loop.close()
        logger.info("Successfully shutdown the app")
    return 0


if __name__ == "__main__":
    main()
