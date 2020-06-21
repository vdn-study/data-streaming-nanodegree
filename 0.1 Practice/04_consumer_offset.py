import asyncio

from confluent_kafka import Consumer, Producer, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient, NewTopic

BROKER_URL = "PLAINTEXT://localhost:9092"

async def produce_consume(topic_name):
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2

async def produce(topic_name):
    p = Producer({"bootstrap.servers": BROKER_URL})
    curr_iteration = 0
    while True:
        p.produce(topic_name, f"iteration {curr_iteration}".encode("utf-8"))
        curr_iteration += 1
        await asyncio.sleep(0.1)

async def consume(topic_name):
    # Sleep few seconds for producer create some messages
    await asyncio.sleep(2.5)

    c = Consumer({
        "bootstrap.servers": BROKER_URL,
        "group.id": "0",
        "auto.offset.reset": "earliest",
    })

    c.subcribe([topic_name], on_assign=on_assign)
    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        if message.error() is not None:
            print(f"error message {message.key()}: {message.value()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        
        await asyncio.sleep(0.1)


def on_assign(consumer, paritions):
    for parition in paritions:
        patition.offset = OFFSET_BEGINNING

    consumer.assign(paritions)

if __name__=="__main__":
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    try:
        asyncio.run(produce_consume("consumer-offset"))
    except KeyboardInterrupt as e:
        print("Shutting down...")
