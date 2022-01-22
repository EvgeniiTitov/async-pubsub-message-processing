from abc import ABC, abstractmethod
import asyncio
import typing as t
import json

from pydantic import ValidationError as PydanticValidationError
from gcloud.aio.pubsub import (
    SubscriberMessage,
    SubscriberClient,
    PublisherClient,
    PubsubMessage,
)

from app.utils import (
    LoggerMixin,
    mark_remaining_items_as_done,
    get_items_batch,
)
from app._types import (
    MessageHandlerCallable,
    MessagePayload,
    MessageValidatorCallable,
)


__all__ = ("Puller", "MessageProcessor", "Acker", "Nacker", "Publisher")


class BaseWorker(ABC):
    @abstractmethod
    async def run_loop(self, *args, **kwargs) -> None:
        ...

    @property
    @abstractmethod
    def is_running(self) -> bool:
        ...


class Puller(LoggerMixin, BaseWorker):
    """
    Pulls messages from a PubSub topic and puts them in a queue connected to
    the MessageProcessor
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

    @property
    def is_running(self) -> bool:
        return self._running

    async def run_loop(self, put_async: bool) -> None:
        self._running = True
        self._logger.info(f"Puller{self._name} started")
        try:
            while True:
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
            # TODO: Do we ignore remaining unscheduled messages?
            self._logger.info(f"Puller{self._name} cancelled")
            raise
        self._logger.info("Puller terminated")


class MessageProcessor(LoggerMixin, BaseWorker):
    """
    Receives messages to process from the message queue populated by the
    Puller(s).
    Processes messages by calling the callback provided on each message. If the
    message's been successfully processed, its ID lands in the ack queue to get
    acked.
    """

    def __init__(
        self,
        *,
        message_queue: asyncio.Queue,
        ack_queue: asyncio.Queue,
        nack_queue: asyncio.Queue,
        to_publisher_queue: asyncio.Queue,
        handle_message_callback: MessageHandlerCallable,
        validate_message_callback: MessageValidatorCallable,
        max_concurrent_tasks: int,
    ) -> None:
        self._message_queue = message_queue
        self._ack_queue = ack_queue
        self._nack_queue = nack_queue
        self._publisher_queue = to_publisher_queue
        self._process_msg_callback = handle_message_callback
        self._validate_msg_callback = validate_message_callback
        self._max_concurrent_tasks = max_concurrent_tasks
        self._sema = asyncio.Semaphore(max_concurrent_tasks)
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    async def run_loop(self) -> None:
        self._running = True
        self._logger.info("MessageProcessor started")
        message_queue = self._message_queue
        try:
            while True:
                message = await message_queue.get()
                self._logger.info("MessageProcessor received message")
                await asyncio.shield(self._consume_one_message(message))
                message_queue.task_done()
        except asyncio.CancelledError:
            self._logger.info(
                "MessageProcessor cancelled. Gracefully terminating..."
            )
            await self._terminate_gracefully()
            raise

    async def _consume_one_message(self, message: SubscriberMessage) -> None:
        message_content, message_id = message.data, message.ack_id
        valid_message = await self._execute_message_validation_callback(
            message_content
        )
        # TODO: IMPORTANT: That shit will get redelivered choking everything
        if not valid_message:
            await self._nack_queue.put(message_id)
            return
        await self._sema.acquire()
        task = asyncio.create_task(
            self._execute_message_processing_callback(
                valid_message, message_id
            )
        )
        task.add_done_callback(lambda f: self._sema.release())
        self._logger.info("MessageProcessor scheduled message")

    async def _execute_message_validation_callback(
        self, message: t.Union[bytes, str]
    ) -> t.Optional[MessagePayload]:
        # TODO: IMPORTANT: Is it CPU intensive to validate large and
        #       complicated schema? Should I run it in a threadpool?
        try:
            valid_message = self._validate_msg_callback(message)
        except json.decoder.JSONDecodeError as e:
            self._logger.warning(
                f"Message {message} couldn't be decoded to json. Error: {e}"
            )
            return None
        except PydanticValidationError:
            self._logger.warning(
                f"Message {message} is of incorrect schema, ignored"
            )
            return None
        except Exception as e:
            self._logger.error(
                f"Provided message validation callback raised exception: {e}"
            )
            # TODO: Can't continue, raise?
        else:
            return valid_message

    async def _execute_message_processing_callback(
        self, message: MessagePayload, message_id: str
    ) -> None:
        try:
            result = await self._process_msg_callback(message)  # type: ignore
        except asyncio.CancelledError:
            self._logger.info("Callback was cancelled")
            await self._nack_queue.put(message_id)
        except Exception as e:
            self._logger.error(
                f"Provided message processing callback raised exception {e}"
            )
            await self._nack_queue.put(message_id)
            # TODO: Cant continue, raise?
        else:
            await asyncio.gather(
                self._ack_queue.put(message_id),
                self._publisher_queue.put(result),
            )

    async def _terminate_gracefully(self) -> None:
        # Ensure all scheduled message processing jobs have completed
        # aka they all have released the sema
        for _ in range(self._max_concurrent_tasks):
            await self._sema.acquire()
        self._logger.info(
            "MessageProcessor waited for all messages to get processed. "
            "Waiting for Acker and Nacker to complete..."
        )
        # Ensure Acker acknowledged all successfully processed messages
        await self._ack_queue.join()
        self._logger.info("Acker queue joined")
        # Ensure Nacker naked all unsuccessfully processed messages
        await self._nack_queue.join()
        self._logger.info("Nacker queue joined")
        self._logger.info("MessageProcessor terminated gracefully")


class Publisher(LoggerMixin, BaseWorker):
    """
    Receives messages to publish to a pubsub topic
    """

    def __init__(
        self,
        publisher_client: PublisherClient,
        publisher_queue: asyncio.Queue,
        *,
        project: str,
        topic: str,
        publish_batch_size: int,
        batch_accumulation_time_window: float = 0.3,
        name: t.Optional[str] = None,
    ) -> None:
        self._publisher_client = publisher_client
        self._publisher_queue = publisher_queue
        self._publish_batch_size = publish_batch_size
        self._time_window = batch_accumulation_time_window
        self._topic = self._publisher_client.topic_path(
            project=project, topic=topic
        )
        self._name = name
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    async def run_loop(self) -> None:
        self._running = True
        self._logger.info(f"Publisher{self._name} started")
        try:
            while True:
                messages = await get_items_batch(
                    queue=self._publisher_queue,
                    time_window=self._time_window,
                    max_items=self._publish_batch_size,
                )
                pubsub_messages = [
                    PubsubMessage(str(message)) for message in messages
                ]
                try:
                    await self._publisher_client.publish(
                        topic=self._topic, messages=pubsub_messages
                    )
                except Exception as e:
                    self._logger.error("Publish request failed", exc_info=e)
                    continue
                except asyncio.CancelledError:
                    raise
                else:
                    for _ in messages:
                        self._publisher_queue.task_done()
                    self._logger.info(
                        f"Pubslisher{self._name} published a batch of messages"
                    )
        except asyncio.CancelledError:
            self._logger.info(
                f"Publisher{self._name} cancelled. Gracefully terminating..."
            )
            await mark_remaining_items_as_done(self._publisher_queue)
            self._logger.info(f"Publisher{self._name} terminated gracefully")
            raise


class Acker(LoggerMixin, BaseWorker):
    """
    Acknowledges successfully processed messages by accumulating a batch of IDs
    and then calling Google's API
    # TODO: The logic is ugly, think again! Try re-acking if failed
    """

    def __init__(
        self,
        ack_queue: asyncio.Queue,
        subscriber_client: SubscriberClient,
        subscription: str,
        *,
        batch_accumulation_time_window: float = 0.3,
        name: t.Optional[str] = None,
    ) -> None:
        self._ack_queue = ack_queue
        self._subscriber_client = subscriber_client
        self._subscription = subscription
        self._time_window = batch_accumulation_time_window
        self._name = name
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    async def run_loop(self) -> None:
        self._running = True
        self._logger.info(f"Acker{self._name} started")
        try:
            while True:
                ack_ids = await get_items_batch(
                    queue=self._ack_queue, time_window=self._time_window
                )
                try:
                    await self._subscriber_client.acknowledge(
                        subscription=self._subscription, ack_ids=ack_ids
                    )
                except Exception as e:
                    self._logger.error(
                        "Acknowledge request failed", exc_info=e
                    )
                    continue
                    # TODO: Try acking each ID separately?
                except asyncio.CancelledError:
                    raise
                else:
                    for _ in ack_ids:
                        self._ack_queue.task_done()
                    self._logger.info(
                        f"Acker{self._name} acknowledged a batch of IDs"
                    )
        except asyncio.CancelledError:
            self._logger.info(
                f"Acker{self._name} cancelled. Gracefully terminating..."
            )
            # TODO: This is not fucking graceful, you didn't ack remaining ids
            await mark_remaining_items_as_done(self._ack_queue)
            self._logger.info(f"Acker{self._name} terminated gracefully")
            raise


class Nacker(LoggerMixin, BaseWorker):
    """
    Changes acknowledge deadline for messages whose processing has failed to 0,
    so that they get redelivered
    """

    def __init__(
        self,
        nack_queue: asyncio.Queue,
        subscriber_client: SubscriberClient,
        subscription: str,
        *,
        batch_accumulation_time_window: float = 0.3,
        name: t.Optional[str] = None,
    ) -> None:
        self._nack_queue = nack_queue
        self._subscriber_client = subscriber_client
        self._subscription = subscription
        self._time_window = batch_accumulation_time_window
        self._name = name
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    async def run_loop(self) -> None:
        self._running = True
        self._logger.info(f"Nacker{self._name} started")
        try:
            while True:
                nacks_id = await get_items_batch(
                    queue=self._nack_queue, time_window=self._time_window
                )
                try:
                    await self._subscriber_client.modify_ack_deadline(
                        subscription=self._subscription,
                        ack_ids=nacks_id,
                        ack_deadline_seconds=0,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._logger.error("Nack request failed", exc_info=e)
                    # TODO: Try nacking one by one?
                    continue
                else:
                    for _ in nacks_id:
                        self._nack_queue.task_done()
                    self._logger.info(
                        f"Nacker{self._name} nacked a batch of IDs"
                    )
        except asyncio.CancelledError:
            self._logger.info(
                f"Nacker{self._name} cancelled. Gracefully terminating..."
            )
            # TODO: This is not fucking graceful, you didn't nack remaining ids
            await mark_remaining_items_as_done(self._nack_queue)
            self._logger.info(f"Nacker{self._name} terminated gracefully")
            raise
