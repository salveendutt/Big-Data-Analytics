from time import sleep
from kafka import KafkaConsumer, KafkaProducer
from config_streaming_processing import (
    kafka_address,
    kafka_topics,
    kafka_connection_attempts,
    kafka_connection_attempts_delay,
)


def create_kafka_consumer():
    attempts = 0
    while attempts < kafka_connection_attempts:
        try:
            consumer = KafkaConsumer(
                *kafka_topics,
                bootstrap_servers=[kafka_address],
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            consumer.subscribe(kafka_topics)
            return consumer
        except Exception as e:
            print(f"Attempt {attempts + 1} failed: {e}")
            attempts += 1
            sleep(kafka_connection_attempts_delay)
    else:
        print(f"Failed to connect to Kafka after {kafka_connection_attempts} attempts")
        exit(1)


def create_kafka_producer():
    attempts = 0
    while attempts < kafka_connection_attempts:
        try:
            producer = KafkaProducer(bootstrap_servers=[kafka_address])
            return producer
        except Exception as e:
            print(f"Attempt {attempts + 1} failed: {e}")
            attempts += 1
            sleep(kafka_connection_attempts_delay)
    else:
        print(f"Failed to connect to Kafka after {kafka_connection_attempts} attempts")
        exit(1)


def preprocess_row_1(row):
    """
    Data preprocessing for a single row in dataset 1 ('Fraudulent Transactions Data' from Kaggle)

    Parameters:
      raw (dict): A single unprocessed row in the dataset

    Returns:
      dict: Preprocessed row with keys: 'type', 'amount', 'oldbalanceOrg', 'newbalanceOrig',
        'isMerchant', 'isFlaggedFraud', 'isFraud' transformed to float/int values only.
    """
    transaction_types = {
        "CASH-IN": 1,
        "CASH-OUT": 2,
        "DEBIT": 3,
        "PAYMENT": 4,
        "TRANSFER": 5,
    }
    return {
        "type": transaction_types.get(row["type"], 0),
        "amount": row["amount"],
        "oldbalanceOrg": row["oldbalanceOrg"],
        "newbalanceOrig": row["newbalanceOrig"],
        "isMerchant": row["nameDest"].startswith("M"),
        "isFlaggedFraud": row["isFlaggedFraud"],
        "isFraud": row["isFraud"],
    }


def preprocess_row_2(row):
    return {
        "distance_from_home": row["distance_from_home"],
        "distance_from_last_transaction": row["distance_from_last_transaction"],
        "ratio_to_median_purchase_price": row["ratio_to_median_purchase_price"],
        "repeat_retailer": int(row["repeat_retailer"]),
        "used_chip": int(row["used_chip"]),
        "used_pin_number": int(row["used_pin_number"]),
        "isFraud": int(row["fraud"]),
    }


def preprocess_row_3(row):
    # TODO: fetch customer information; calculate number of customer transactions today
    entry_modes = {"Contactless": 1, "Chip": 2, "Swipe": 3}
    return {
        "customer_id": row["customer_id"],
        "bin": row["bin"],
        "amount": row["amt"],
        "entry_mode": entry_modes.get(row["entry_mode"], 0),
        "isFraud": 0,
    }


def preprocess_row_4(row):
    result = row.copy()
    result["amount"] = row["Amount"]
    result["isFraud"] = row["Class"]

    # cleanup
    del result["Time"]
    del result["Amount"]
    del result["Class"]
    return result
