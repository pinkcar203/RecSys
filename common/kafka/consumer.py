import json
from collections.abc import AsyncIterator
from aiokafka import AIOKafkaConsumer

class KafkaConsumerWrapper:
    # Async Kafka consumer with JSON deserialization.

    def __init__(
        self,
        topic: str,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "default-group",
    ):
        self._consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )

    async def start(self):
        await self._consumer.start()

    async def stop(self):
        await self._consumer.stop()

    async def commit(self):
        await self._consumer.commit()

    async def messages(self) -> AsyncIterator[dict]:
        async for msg in self._consumer:
            yield msg.value
            await self._consumer.commit()
