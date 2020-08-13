import asyncio
import json
import requests

KAFKA_CONNECT_URL = "http://localhost:8083"
CONNECTOR_NAME = "connector_jdbc"

def configure_connector():
    # https://docs.confluent.io/current/connect/kafka-connect-jdbc/source-connector/source_config_options.html
    print("creating or updating connector")

    rest_method = requests.post

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        return 

    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": "clicks-jdbc",
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "topic.prefix": "connect-",
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "table.whitelist": "stations",
                "tasks.max": 1,
                "connection.url": "jdbc:postgresql://localhost:5432/cta",
                "connection.user": "cta_admin",
                "connection.password": "chicago",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.convert": "org.apache.kafka.connect.json.JsonConverter",
                "value.convert.schemas.enable": "false",
            }
        })
    )

    try:
        resp.raise_for_status()
    except:
        print(f"Failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    print("connector created successfully")
    print("use kafka-console-consumer and kafka-topics to see the data")


if __name__=="__main__":
    configure_connector()