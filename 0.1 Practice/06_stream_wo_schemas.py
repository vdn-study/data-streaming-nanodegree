import asyncio
from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker

faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"

@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email) 
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)

    num_calls = 0
    
    def serialize(self):
        email_key = "email" if ClickEvent.num_calls < 10 else "user_email"
        ClickEvent.num_calls += 1

        return json.dumps({
            "uri": self.uri,
            "timestamp": self.timestamp,
            email_key: self.email
        })

    @classmethod
    def deserialize(self, json_data):
        purchase_json = json.loads(json_data)
        return Purchase(
            username=purchase_json["username"],
            currency=purchase_json["currency"],
            amount=purchase_json["amount"]
        )

async def produce_consume(topic_name):
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2

async def produce(topic_name):
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        message = ClickEvent().serialize()
        # print(message)
        p.produce(topic_name, message)
        await asyncio.sleep(1.0)

async def consume(topic_name):
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])
    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            clickevent_json = json.loads(message.value())
            try:
                print(
                    ClickEvent(
                        email=clickevent_json["user_email"],
                        timestamp=clickevent_json["timestamp"],
                        uri=clickevent_json["uri"],
                    )
                )
            except KeyError as e:   
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(1.0)

if __name__=="__main__":
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    try:
        asyncio.run(produce_consume("stream_wo_schema"))
    except KeyboardInterrupt as e:
        print("Shutting down ...")