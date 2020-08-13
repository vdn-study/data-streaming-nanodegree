import logging
import logging.config
from configparser import ConfigParser
import os
cur_path = os.path.dirname(os.path.realpath(__file__))

# make sure logging config is picked up by modules
logging.config.fileConfig(os.path.join(cur_path, "logging.ini"))

import producer_server

def run_kafka_producer():
    """
    Create Kafka producer, check if relevant topic exists (if not create it) and start producing messages
    """

    # load config
    config = ConfigParser()
    config.read(os.path.join(cur_path, "app.cfg"))

    # start kafka producer
    logger.info("Starting Kafka Producer")
    producer = producer_server.ProducerServer(config)

    # check if topic exists
    logger.info("Creating topic...")
    producer.create_topic()

    # generate data
    logger.info("Starting to generate data...")

    try:
        producer.generate_data()
    except KeyboardInterrupt:
        logging.info("Stopping Kafka Producer")
        producer.close()


if __name__ == "__main__":

    # start logging
    logger = logging.getLogger(__name__)

    run_kafka_producer()
