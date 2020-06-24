import asyncio
from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker

faker = Faker()

# BROKER_URL = "PLAINTEXT://kafka01-vn00c1.vn.infra:9092"

BROKER_URL = "PLAINTEXT://localhost:9092"
@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        return json.dumps(
            {
                "username": self.username,
                "currency": self.currency,
                "amount": self.amount,
            }
        )

async def produce_consume(topic_name):
    t1 = asyncio.create_task(produce(topic_name))
    # t2 = asyncio.create_task(consume(topic_name))
    await t1
    # await t2

async def produce(topic_name):
    p = Producer({"bootstrap.servers": BROKER_URL})

    while True:
        for _ in range(10):
            p.produce(topic_name, Purchase().serialize())
        await asyncio.sleep(0.01)

async def consume(topic_name):
    
    c = Consumer({
        "bootstrap.servers": BROKER_URL,
        "group.id": "0"
    })

    c.subscribe([topic_name])
    while True:
        messages = c.consume(5, timeout=1.0)
        print(f"consumed {len(messages)} messages")
        for message in messages:
            print(f"consume message {message.key()}: {message.value()}")

        # Do not delete this!
        await asyncio.sleep(0.01)

if __name__=="__main__":
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    try:
        asyncio.run(produce_consume("streaming.risk.data_score_test"))
    except KeyboardInterrupt as e:
        print("Shutting down ...")