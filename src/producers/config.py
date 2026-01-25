import os

BOOTSTRAP_SERVERS = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_RAW = "transactions-raw"
TOPIC_FRAUD = "fraud-alerts"
TOPIC_VALID = "transactions-validated"

COUNTRIES = [
    "Sri Lanka", "USA", "United Kingdom", "Singapore", 
    "Australia", "Germany", "Japan", "UAE"
]

FRAUD_TRIGGER_PROBABILITY = 0.1
FRAUD_TIME_DELTA_SECONDS = 600