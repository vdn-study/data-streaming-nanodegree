import asyncio
from dataclasses import asdict, dataclass, field
import json
import time
import random

import requests
from confluent_kafka import avro, Consumer, Producer
from confluent_kafka.avro import AvroConsumer, AvroProducer, CachedSchemaRegistryClient
from faker import Faker


faker = Faker()
REST_PROXY_URL = "http://localhost:8082"
TOPIC_NAME = "rest-proxy-avro"
CONSUMER_GROUP = f"rest-proxy-avro-group-{random.randint(0,10000)}"


async def consume():
    consumer_name = "rest-proxy-avro-consumer"
    headers = {"Content-Type": "application/vnd.kafka.json.v2+json"}
    data = {"name": consumer_name, "format": "avro"}
    resp = requests.post(
        f"{REST_PROXY_URL}/consumers/{CONSUMER_GROUP}",
        data=json.dumps(data),
        headers=headers,
    )
    try:
        resp.raise_for_status()
    except:
        print(
            f"Failed to create REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
        )

        return
    print("REST Proxy consumer group created")

    resp_data = resp.json()
    data = {"topics": [TOPIC_NAME]}
    resp = requests.post(
        f"{resp_data['base_uri']}/subscription", data=json.dumps(data), headers=headers
    )
    try:
        resp.raise_for_status()
    except:
        print(
            f"Failed to subscribe REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
        )
        return

    print("REST Proxy consumer subscription created")

    while True:
        headers = {"Accept": "application/vnd.kafka.avro.v2+json"}
        resp = requests.get(f"{resp_data['base_uri']}/records", headers=headers)
        try:
            resp.raise_for_status()
        except:
            print(
                f"Failed to fetch records with REST proxy consumer: {json.dumps(resp.json(), indent=2)}"
            )
            return

        print("Consumed records via REST Proxy:")
        print(f"{json.dumps(resp.json())}")
        await asyncio.sleep(0.1)

@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))

    schema = avro.loads(
        """{
        "type": "record",
        "name": "click_event",
        "namespace": "rest-proxy-avro",
        "fields": [
            {"name": "email", "type": "string"},
            {"name": "timestamp", "type": "string"},
            {"name": "uri", "type": "string"},
            {"name": "number", "type": "int"}
        ]
    }"""
    )


async def produce(topic_name):
    p = AvroProducer(
        {
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "schema.registry.url": "http://localhost:8081",
        }
    )
    try:
        while True:
            p.produce(
                topic=topic_name,
                value=asdict(ClickEvent()),
                value_schema=ClickEvent.schema,
            )
            await asyncio.sleep(0.1)
    except:
        raise

async def produce_consume(topic_name):
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume())
    await t1
    await t2

if __name__ == "__main__":
    try:
        asyncio.run(produce_consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")