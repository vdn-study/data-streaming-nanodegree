from dataclasses import dataclass, field, asdict
import json
import time
import random

import requests
from confluent_kafka import avro, Consumer, Producer
from confluent_kafka.avro import AvroConsumer, AvroProducer, CachedSchemaRegistryClient
from faker import Faker

faker = Faker()
REST_PROXY_URL = "http://localhost:8082"

@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))

def produce():
    headers = {
        "Content-Type": "application/vnd.kafka.json.v2+json"
    }

    data = {
        "records": [
            {
                "value": asdict(ClickEvent())
            }
        ]
    }

    resp = requests.post(
        f"{REST_PROXY_URL}/topics/rest-proxy-json",
        data=json.dumps(data),
        headers=headers
    )

    try:
        resp.raise_for_status()
    except:
        print(f"Failed to send data to REST Proxy {json.dumps(resp.json(), indent=2)}")

    print(f"Sent data to REST Proxy {json.dumps(resp.json(), indent=2)}")

if __name__=="__main__":
    try:
        while True:
            produce()
            time.sleep(0.5)
    except KeyboardInterrupt as e:
        print("Shutting down...")