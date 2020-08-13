import json
import requests

REST_PROXY_URL = "http://localhost:8082"

def get_topics():
    resp = requests.get(f"{REST_PROXY_URL}/topics")

    try:
        resp.raise_for_status()
    except:
        print(f"Failed to get topics {json.dumps(resp.json(), indent=2)}")
        return []

    print("Fetched topics from Kafka:")
    print(json.dumps(resp.json(), indent=2))

    return resp.json()

def get_topic(topic_name):
    resp = requests.get(f"{REST_PROXY_URL}/topics/{topic_name}")

    try:
        resp.raise_for_status()
    except:
        print(f"Failed to get topic {topic_name} {json.dumps(resp.json(), indent=2)}")
        return []
    
    print("Fetched topic from kafka:")
    print(json.dumps(resp.json(), indent=2))

def get_brokers():
    resp = requests.get(f"{REST_PROXY_URL}/brokers")

    try:
        resp.raise_for_status()
    except:
        print(f"Failed to get broker {json.dumps(resp.json(), indent=2)}")
        return []

    print("Fetched brokers from Kafak:")
    print(json.dumps(resp.json(), indent=2))

def get_partitions(topic_name):
    resp = requests.get(f"{REST_PROXY_URL}/topics/{topic_name}/partitions")

    try:
        resp.raise_for_status()
    except:
        print(f"Failed to get partitions {json.dumps(resp.json(), indent=2)}")
        return []

    print(f"Fetch paritions of {topic_name} from Kafka:")
    print(json.dumps(resp.json(), indent=2))


if __name__=="__main__":
    get_topics()
    get_topic("schema_registry")
    get_brokers()
    get_partitions("schema_registry")