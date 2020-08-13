import producer_server
import logging


def run_kafka_producer(input_file, topic):
    logging.info(f"Start running kafka producer for {input_file} with defined topic {topic}")
    producer_server.ProducerServer(input_file, topic).generate_data()

if __name__=="__main__":
    input_file = "data/uber.json"
    topic = "uber"
    run_kafka_producer(input_file, topic)
