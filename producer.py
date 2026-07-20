# Imports
import json
import time
import pandas as pd
from kafka import KafkaProducer

# Config
TOPIC = "bank-transactions"
CSV_PATH = "data/bank_transactions_data_2.csv"
SEND_INTERVAL_SECONDS = 1

# Connecting to Kafka
def create_producer():
    return KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

# Loading the dataset
def main():
    df = pd.read_csv(CSV_PATH)
    producer = create_producer()
    print(f"Connected to Kafka. Streaming {len(df)} transactions to topic '{TOPIC}'...")

 # Streaming loop
    count = 0
    while True:
        for _, row in df.iterrows():
            record = row.to_dict()
            producer.send(TOPIC, value=record)
            count += 1
            print(f"[{count}] Sent transaction {record['TransactionID']} "
                  f"(${record['TransactionAmount']:.2f}, {record['Channel']})")
            time.sleep(SEND_INTERVAL_SECONDS)
#Entry points
if __name__ == "__main__":
    main()
