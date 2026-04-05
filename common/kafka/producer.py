import json
from aiokafka import AIOKafkaProducer

class KafkaProducerWrapper:
    """Async Kafka producer with JSON serialization."""

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )

    async def start(self):
        await self._producer.start()

    async def stop(self):
        await self._producer.stop()

    async def send(self, topic: str, value: dict, key: str | None = None):
        key_bytes = key.encode("utf-8") if key else None
        await self._producer.send_and_wait(topic, value=value, key=key_bytes)
