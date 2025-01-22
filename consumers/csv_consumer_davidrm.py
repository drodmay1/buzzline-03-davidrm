"""
csv_consumer_davidrm.py

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # To parse JSON messages from Kafka
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from collections import deque  # For rolling window of temperature readings
import pandas as pd  # For handling data processing

# Load environment variables from .env
load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def get_stall_threshold() -> float:
    """Fetch message interval from environment or use default."""
    # Increase the threshold for detecting a stall to 1.0°F or higher
    temp_variation = float(os.getenv("SMOKER_STALL_THRESHOLD_F", 1.0))  # Increased to 1.0°F
    logger.info(f"Max stall temperature range: {temp_variation} F")
    return temp_variation

def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("SMOKER_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size

#####################################
# Function to detect a stall based on rolling window
#####################################

def detect_stall(rolling_window_deque: deque) -> bool:
    """Detect a temperature stall based on the rolling window."""
    WINDOW_SIZE = get_rolling_window_size()
    if len(rolling_window_deque) < WINDOW_SIZE:
        logger.debug(f"Rolling window size: {len(rolling_window_deque)}. Waiting for {WINDOW_SIZE}.")
        return False

    temp_range = max(rolling_window_deque) - min(rolling_window_deque)
    is_stalled = temp_range <= get_stall_threshold()
    
    # Log the rolling window and temperature range for debugging
    logger.debug(f"Rolling window: {list(rolling_window_deque)}")
    logger.debug(f"Temperature range: {temp_range}°F. Stalled: {is_stalled}")
    
    return is_stalled


#####################################
# Function to process a single message
#####################################

def process_message(message: str, rolling_window: deque) -> None:
    """Process a message and check for stall conditions."""
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON message into a dictionary
        message_dict = json.loads(message)  # Parse the JSON message
        logger.info(f"Processed JSON message: {message_dict}")

        # Ensure necessary fields are present
        if "temperature" in message_dict and "timestamp" in message_dict:
            # Extract and convert the temperature to float (removing ' C' part)
            temperature_str = message_dict["temperature"]
            temperature = float(temperature_str.replace(" C", ""))  # Remove ' C' and convert to float
            timestamp = message_dict["timestamp"]
            logger.info(f"Temperature: {temperature}°C, Timestamp: {timestamp}")

            # Append the temperature to the rolling window
            rolling_window.append(temperature)

            # Check for a stall
            if detect_stall(rolling_window):
                logger.info(f"STALL DETECTED at {timestamp}: Temp stable at {temperature}°F.")
        else:
            logger.error("Missing required fields (temperature or timestamp) in the message.")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

#####################################
# Main Function for Consumer
#####################################

def main() -> None:
    """Main entry point for the consumer."""
    logger.info("START consumer.")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")

    rolling_window = deque(maxlen=window_size)

    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window)  # Process message and check stall
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()

