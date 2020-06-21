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

def create_topic(client, topic_name):
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

def produce(topic_name):
    p = Producer(
        {
            "bootstrap.servers": BROKER_URL,
            "client.id": "vn.bi",
            "linger.ms": 1000,
            "compression.type": "lz4",
            "batch.num.messages": 100,
        }
    )
    while True:
        p.produce(topic_name, Purchase().serialize())

if __name__=="__main__":
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    if topic_exists(client, TOPIC_NAME) is False:
        create_topic(client, TOPIC_NAME)
    try:
        produce(TOPIC_NAME)
    except KeyboardInterrupt as e:
        print("Shutting down...\n")