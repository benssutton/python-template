import pyarrow as pa
import pyarrow.ipc as pa_ipc
from solace.messaging.messaging_service import MessagingService
from solace.messaging.publisher.direct_message_publisher import DirectMessagePublisher
from solace.messaging.resources.topic import Topic


class SolacePublisher:
    def __init__(self, host: str, port: int, vpn: str,
                 username: str, password: str, topic: str) -> None:
        self._topic = topic
        props = {
            "solace.messaging.transport.host": f"tcp://{host}:{port}",
            "solace.messaging.service.vpn-name": vpn,
            "solace.messaging.authentication.scheme.basic.username": username,
            "solace.messaging.authentication.scheme.basic.password": password,
        }
        self._service: MessagingService = (
            MessagingService.builder().from_properties(props).build()
        )
        self._service.connect()
        self._publisher: DirectMessagePublisher = (
            self._service.create_direct_message_publisher_builder().build()
        )
        self._publisher.start()

    def publish_batch(self, batch: pa.RecordBatch) -> None:
        buf = pa.BufferOutputStream()
        with pa_ipc.new_stream(buf, batch.schema) as writer:
            writer.write_batch(batch)
        payload = buf.getvalue().to_pybytes()
        message = self._service.message_builder().build(payload)
        self._publisher.publish(message=message, destination=Topic.of(self._topic))

    def close(self) -> None:
        self._publisher.terminate()
        self._service.disconnect()
