import asyncio
import logging
import queue
import threading
from typing import Iterator

import pyarrow as pa
import pyarrow.ipc as pa_ipc
from solace.messaging.messaging_service import (
    MessagingService,
    ReconnectionAttemptListener,
    ReconnectionListener,
    ServiceEvent,
    ServiceInterruptionListener,
)
from solace.messaging.receiver.direct_message_receiver import DirectMessageReceiver
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage
from solace.messaging.resources.topic_subscription import TopicSubscription

from ingestion.base import ConnectionState
from settings import Settings

log = logging.getLogger(__name__)


class _BatchHandler(MessageHandler):
    def __init__(self, q: queue.Queue) -> None:
        self._queue = q

    def on_message(self, message: InboundMessage) -> None:
        payload = message.get_payload_as_bytes()
        try:
            reader = pa_ipc.open_stream(pa.BufferReader(payload))
            for batch in reader:
                self._queue.put(batch)
        except Exception:
            log.warning("Solace: malformed IPC message dropped", exc_info=True)


class _StateListener(
    ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener
):
    """Bridges Solace SDK lifecycle callbacks (fired on SDK threads) to the
    consumer's connection state. The event argument is unused."""

    def __init__(self, consumer: "SolaceBatchConsumer") -> None:
        self._consumer = consumer

    def on_reconnected(self, event: ServiceEvent) -> None:
        self._consumer._set_state(ConnectionState.CONNECTED)

    def on_reconnecting(self, event: ServiceEvent) -> None:
        self._consumer._set_state(ConnectionState.RECONNECTING)

    def on_service_interrupted(self, event: ServiceEvent) -> None:
        self._consumer._set_state(ConnectionState.DOWN)


class SolaceBatchConsumer:
    """Async context manager for connection lifecycle; BatchConsumer for ingest."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._service: MessagingService | None = None
        self._receiver: DirectMessageReceiver | None = None
        self._queue: queue.Queue[pa.RecordBatch | None] = queue.Queue()
        self._state = ConnectionState.DOWN
        self._state_lock = threading.Lock()

    def _set_state(self, state: ConnectionState) -> None:
        with self._state_lock:
            self._state = state

    def connection_state(self) -> ConnectionState:
        with self._state_lock:
            return self._state

    async def __aenter__(self) -> "SolaceBatchConsumer":
        self._service = await asyncio.to_thread(self._connect)
        return self

    def _connect(self) -> MessagingService:
        props = {
            "solace.messaging.transport.host":
                f"tcp://{self._settings.solace_host}:{self._settings.solace_port}",
            "solace.messaging.service.vpn-name": self._settings.solace_vpn,
            "solace.messaging.authentication.scheme.basic.username":
                self._settings.solace_username,
            "solace.messaging.authentication.scheme.basic.password":
                self._settings.solace_password,
        }
        svc = MessagingService.builder().from_properties(props).build()
        svc.connect()
        listener = _StateListener(self)
        svc.add_reconnection_listener(listener)
        svc.add_reconnection_attempt_listener(listener)
        svc.add_service_interruption_listener(listener)
        self._set_state(ConnectionState.CONNECTED)
        return svc

    async def __aexit__(self, *_: object) -> None:
        await asyncio.to_thread(self.close)

    def batches(self) -> Iterator[pa.RecordBatch]:
        self._receiver = (
            self._service
            .create_direct_message_receiver_builder()
            .with_subscriptions([TopicSubscription.of(self._settings.solace_topic)])
            .build()
        )
        self._receiver.start()
        self._receiver.receive_async(_BatchHandler(self._queue))
        while True:
            item = self._queue.get()    # blocks until message or None sentinel
            if item is None:
                break
            yield item

    def close(self) -> None:
        self._set_state(ConnectionState.DOWN)
        self._queue.put(None)           # unblocks batches() generator
        if self._receiver is not None:
            self._receiver.terminate()
            self._receiver = None
        if self._service is not None:
            self._service.disconnect()
            self._service = None
