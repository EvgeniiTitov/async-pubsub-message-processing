import asyncio
import typing as t

from gcloud.aio.pubsub import SubscriberClient, SubscriberMessage

from _types import MessageHandlerCallable
from logger import LoggerMixin


class Puller(LoggerMixin):
    """
    Pulls messages from a PubSub topic and puts them in a queue connected to
    the Consumer

    # TODO: Add message validation (json schema - pydantic?)
    # TODO: Gracefully terminate the puller if the coro gets cancelled. You
    #       might have pulled messages that haven't been processed yet
    """

    def __init__(
        self,
        subscriber_client: SubscriberClient,
        message_queue: asyncio.Queue,
        subscription: str,
        *,
        batch_size: int = 5,
        timeout: int = 30,
        name: t.Optional[str] = None,
    ) -> None:
        self._subscriber_client = subscriber_client
        self._queue = message_queue
        self._subscription = subscription
        self._batch_size = batch_size
        self._timeout = timeout
        self._name = name
        self._running = False
        self._must_stop = False
        self._logger.info(f"Puller{self._name} initialized")

    @property
    def is_running(self) -> bool:
        return self._running

    async def pull_messages_and_populate_queue(self, put_async: bool) -> None:
        self._running = True
        self._logger.info(f"Puller{self._name} started")
        try:
            while True:
                if self._must_stop:
                    break

                messages = await self._subscriber_client.pull(
                    subscription=self._subscription,
                    max_messages=self._batch_size,
                    timeout=self._timeout,
                )
                if not len(messages):
                    self._logger.info("No messages in the queue, retrying")
                    continue
                self._logger.info(f"{self._name} Got batch of messages")
                for message in messages:
                    if put_async:
                        asyncio.create_task(self._queue.put(message))
                    else:
                        await self._queue.put(message)
        except asyncio.CancelledError:
            self._logger.info(f"Puller{self._name} cancelled")
            raise
        self._logger.info("Puller stopped")

    def stop(self) -> None:
        self._must_stop = True


class Consumer(LoggerMixin):
    """
    Receives messages to process from the message queue populated by the
    Puller(s).
    Processes messages by calling the callback provided on each message. If the
    message's been successfully processed, its ID lands in the ack queue to get
    acked.
    """

    def __init__(
        self,
        message_queue: asyncio.Queue,
        ack_queue: asyncio.Queue,
        nack_queue: asyncio.Queue,
        handle_message_callback: MessageHandlerCallable,
        *,
        max_concurrent_tasks: int,
    ) -> None:
        self._message_queue = message_queue
        self._ack_queue = ack_queue
        self._nack_queue = nack_queue
        self._callback = handle_message_callback
        self._max_concurrent_tasks = max_concurrent_tasks
        self._sema = asyncio.Semaphore(max_concurrent_tasks)
        self._running = False
        self._must_stop = False
        self._logger.info("Consumer initialized")

    @property
    def is_running(self) -> bool:
        return self._running

    async def process_messages(self) -> None:
        self._running = True
        self._logger.info("Consumer started")
        message_queue = self._message_queue
        try:
            while True:
                message = await message_queue.get()
                self._logger.info("Consumer received message")
                await asyncio.shield(self._consume_one_message(message))
                message_queue.task_done()
        except asyncio.CancelledError:
            self._logger.info(
                "Consumer worker cancelled. Gracefully terminating"
            )
            # Ensure all scheduled message processing jobs have completed
            for _ in range(self._max_concurrent_tasks):
                await self._sema.acquire()
            # Ensure Acker acknowledged all successfully processed messages
            await self._ack_queue.join()
            # Ensure Nacker naked all unsuccessfully processed messages
            await self._nack_queue.join()
            self._logger.info("Consumer terminated gracefully")
            raise

    async def _consume_one_message(self, message: SubscriberMessage) -> None:
        await self._sema.acquire()
        task = asyncio.create_task(self._execute_callback(message))
        task.add_done_callback(lambda f: self._sema.release())
        self._logger.info("Consumer scheduled message")

    async def _execute_callback(self, message: SubscriberMessage) -> None:
        try:
            await self._callback(message)
            await self._ack_queue.put(message.ack_id)
        except asyncio.CancelledError:
            await self._nack_queue.put(message.ack_id)
            self._logger.info("Callback was cancelled")
        except Exception as e:
            await self._nack_queue.put(message.ack_id)
            self._logger.info(f"Provided callback raised exception {e}")


class Publisher:
    pass


class Acker:
    pass


class Nacker:
    pass
