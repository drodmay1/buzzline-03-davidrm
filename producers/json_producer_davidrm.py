"""
json_producer_davidrm.py

Stream JSON data to a Kafka topic.

Example JSON message
{"message": "I enjoy working with Python!", "author": "DavidRm"}

Example serialized to Kafka message
"{\"message\": \"I This is a custom Kafka message!\", \"author\": \"DavidRm\"}"

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time
import pathlib  # work with file paths
import json  # work with JSON data
from datetime import datetime

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("BUZZ_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval

#####################################
# Function to Generate Custom Messages
#####################################

def generate_messages():
    """Dynamically generate custom messages."""
    id_counter = 1
    while True:
        message = {
            "id": id_counter,
            "author": "David",
            "message": f"This is custom message {id_counter}",
            "timestamp": datetime.utcnow().isoformat(),
            "priority": "high" if id_counter % 2 == 0 else "normal"
        }
        logger.info(f"Generated custom message: {message}")
        yield message
        id_counter += 1
        time.sleep(1)  # Adjust the interval as needed


#####################################
# Set up Paths
#####################################

# The parent directory of this file is its folder.
# Go up one more parent level to get the project root.
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

# Set directory where data is stored
DATA_FOLDER: pathlib.Path = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

# Set the name of the data file
DATA_FILE: pathlib.Path = DATA_FOLDER.joinpath("buzz.json")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Main Function
#####################################


def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams generated JSON messages to the Kafka topic.
    - Sends JSON messages to the Kafka topic.
    """

    logger.info("START producer.")

    # fetch .env content
    topic = get_kafka_topic()
    interval = get_message_interval()

    # Verify the data file exists
    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    # Create the Kafka producer
    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        # Generate and send messages
        for message_dict in generate_messages():
            # Serialize the message as JSON
            message_json = json.dumps(message_dict)

            # Send the message to the Kafka topic
            producer.send(topic, value=message_json)
            logger.info(f"Sent message to topic '{topic}': {message_dict}")

            # Wait for the defined interval
            time.sleep(interval)

    except Exception as e:
        logger.error(f"Error producing messages: {e}")
    finally:
        producer.close()
        logger.info("Producer stopped.")

    logger.info("END producer.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
