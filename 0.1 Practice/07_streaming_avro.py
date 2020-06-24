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
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))

    schema = parse_schema({
        "type": "record",
        "name": "click_event",
        "namespace": "com.hcvn.bi",
        "fields":[
            {"name": "email", "type": "string"},
            {"name": "timestamp", "type": "string"},
            {"name": "uri", "type": "string"},
            {"name": "number", "type": "int"}
        ]
    })

    def serialize(self):
        out = BytesIO()
        writer(out, ClickEvent.schema, [asdict(self)])
        return out.getvalue()

async def produce_consume(topic_name):
    t1 = asyncio.create_task(produce(topic_name))
    await t1

async def produce(topic_name):
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        p.produce(topic_name, ClickEvent().serialize())
        await asyncio.sleep(1.0)

if __name__=="__main__":
    try:
        asyncio.run(produce_consume("streaming_avro"))
    except KeyboardInterrupt as e:
        print("Shutting down")
