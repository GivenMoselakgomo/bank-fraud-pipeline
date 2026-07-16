import json
import time
import pandas as pd
from kafka import KafkaProducer

TOPIC = "bank-transactions"
CSV_PATH = "data/bank_transactions_data_2.csv"
SEND_INTERVAL = 1

def create_producer():
    return KafkaProducer(
        bootstrap_servers = "localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )