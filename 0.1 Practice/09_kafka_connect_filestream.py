import asyncio
import json
import requests

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "connect_filestream"

async def log_task():
    task = asyncio.create_task(log())
    configure_connector()
    await task

async def log():
    with open(f"/tmp/{CONNECTOR_NAME}.log", "w") as f:
        iteration = 0
        while True:
            f.write(f"log number {iteration}\n")
            f.flush()
            await asyncio.sleep(1.0)
            iteration += 1

def configure_connector():
    print("create or updating the kafka connector...")
    rest_method = requests.post
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        return

    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={
            "Content-Type": "application/json"
        },
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "FileStreamSource",
                "topic": CONNECTOR_NAME,
                "tasks.max": 1,
                "file": f"/tmp/{CONNECTOR_NAME}.log",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.convert.schemas.enable": "false",
            }
        })
    )

    resp.raise_for_status()
    print("connector created successfully")

if __name__=="__main__":
    try:
        asyncio.run(log_task())
    except KeyboardInterrupt as e:
        print("Shutting down...")