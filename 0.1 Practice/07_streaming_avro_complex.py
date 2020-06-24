import asyncio
from dataclasses import dataclass, field, asdict
from io import BytesIO
import json
import random

from confluent_kafka import Producer
from faker import Faker
from fastavro import parse_schema, writer

faker = Faker()
BROKER_URL = "PLAINTEXT://localhost:9092"

@dataclass
class ClickAttribute:
    element: str = field(default_factory=lambda: random.choice(["div", "a", "button"]))
    content: str = field(default_factory=faker.bs)

    @classmethod
    def attributes(self):
        return {faker.uri_page(): ClickAttribute() for _ in range(random.randint(1, 5))}

@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))
    attributes: dict = field(default_factory=ClickAttribute.attributes)

    schema = parse_schema(
        {
            "type": "record",
            "name": "click_event",
            "namespace": "streaming_avro_complex",
            "fields": [
                {"name": "email", "type": "string"},
                {"name": "timestamp", "type": "string"},
                {"name": "uri", "type": "string"},
                {"name": "number", "type": "int"},
                {
                    "name": "attributes",
                    "type": {
                        "type": "map",
                        "values": {
                            "type": "record",
                            "name": "attribute",
                            "fields": [
                                {"name": "element", "type": "string"},
                                {"name": "content", "type": "string"},
                            ],
                        },
                    },
                },
            ],
        }
    )

    def serialize(self):
        """Serializes the ClickEvent for sending to Kafka"""
        out = BytesIO()
        writer(out, ClickEvent.schema, [asdict(self)])
        return out.getvalue()

async def produce(topic_name):
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        p.produce(topic_name, ClickEvent().serialize())
        await asyncio.sleep(1.0)

async def produce_consume(topic_name):
    t1 = asyncio.create_task(produce(topic_name))
    await t1

if __name__=="__main__":
    try:
        asyncio.run(produce_consume("streaming_avro_complex"))
    except KeyboardInterrupt as e:
        print("Shutting down...")
