import asyncio
import typing as t

from gcloud.aio.pubsub import SubscriberClient, SubscriberMessage

from _types import MessageHandlerCallable
from logger import LoggerMixin


class Puller(LoggerMixin):
    def __init__(
        self,
        subscriber_client: SubscriberClient,
        message_queue: asyncio.Queue,
        subscription: str,
        *,
        batch_size: int = 5,
        timeout: int = 15,
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
        self._logger.info("Puller initialized")

    async def pull_messages_and_populate_queue(self, put_async: bool) -> None:
        self._running = True
        self._logger.info("Puller started")
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
            # TODO: How to terminate gracefully here? I can still have some
            #       messages that I pulled but didn't schedule their execution
            raise
        self._logger.info("Puller stopped")

    def stop(self) -> None:
        self._must_stop = True


class Consumer(LoggerMixin):
    def __init__(
        self,
        message_queue: asyncio.Queue,
        ack_queue: asyncio.Queue,
        nack_queue: asyncio.Queue,
        handle_message_callback: MessageHandlerCallable,
        max_concurrent_tasks: int,
    ) -> None:
        self._message_queue = message_queue
        self._ack_queue = ack_queue
        self._nack_queue = nack_queue
        self._callback = handle_message_callback
        self._sema = asyncio.Semaphore(max_concurrent_tasks)

        self._running = False
        self._must_stop = False
        self._logger.info("Consumer initialized")

    async def process_messages(self) -> None:
        self._running = True
        self._logger.info("Consumer started")
        message_queue = self._message_queue
        try:
            while True:
                message = await message_queue.get()
                await asyncio.shield(self._consume_one_message(message))
                message_queue.task_done()
        except asyncio.CancelledError:
            self._logger.info("Consumer worker cancelled.")

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
