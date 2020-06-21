from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker
import asyncio

faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "datascore"

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
                "amount": self.amount
            }
        )

def topic_exists(client, topic_name):
    topic_meta = client.list_topics(timeout=5)
    return topic_name in set(t.topic for t in iter(topic_meta.topics.values()))
    
def produce_sync(topic_name):
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        p.produce(topic_name, Purchase().serialize())
        p.flush()

async def produce_async(topic_name):
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        p.produce(topic_name, Purchase().serialize())
        await asyncio.sleep(0.5)

async def produce(topic_name):
    producer = asyncio.create_task(produce_async(topic_name))
    await producer

def create_topic(topic_name):
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    futures = client.create_topics(
        [
            NewTopic(
                topic=topic_name,
                num_partitions=5,
                replication_factor=1
            )
        ]
    )
    for _, future in futures.items():
        try:
            future.result()
        except Exception as e:
            pass

if __name__=="__main__":
    create_topic(TOPIC_NAME)
    try:
        # Run synchronous
        produce_sync(TOPIC_NAME)

    except KeyboardInterrupt as e:
        print("Shutting down...\n")