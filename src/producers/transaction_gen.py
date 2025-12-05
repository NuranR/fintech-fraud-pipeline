import time
import json
import random
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
from faker import Faker
import config  # Imports config.py

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

fake = Faker()

def get_producer():
    return KafkaProducer(
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def generate_transaction(user_id=None):
    if not user_id:
        user_id = fake.uuid4()
        
    return {
        "transaction_id": fake.uuid4(),
        "user_id": user_id,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "currency": "USD",
        "merchant_category": random.choice(["Food", "Electronics", "Travel", "Fashion"]),
        "country": random.choice(config.COUNTRIES)
    }

def start_stream():
    producer = get_producer()
    logger.info(f"🚀 Starting Producer. Target Topic: {config.TOPIC_RAW}")

    try:
        while True:
            # Randomly inject a fraud pattern based on probability
            if random.random() < config.FRAUD_TRIGGER_PROBABILITY:
                
                # Valid transaction (Origin: Sri Lanka)
                victim_id = fake.uuid4()
                tx1 = generate_transaction(user_id=victim_id)
                tx1['country'] = "Sri Lanka"
                
                producer.send(config.TOPIC_RAW, value=tx1)
                logger.info(f"Generated Legitimate: User {victim_id[:8]}... at {tx1['country']}")

                # Impossible travel transaction (Target: UK, +2 mins)
                tx2 = generate_transaction(user_id=victim_id)
                tx2['country'] = "United Kingdom"
                t1 = datetime.strptime(tx1['timestamp'], "%Y-%m-%d %H:%M:%S")
                t2 = t1 + timedelta(seconds=config.FRAUD_TIME_DELTA_SECONDS)
                tx2['timestamp'] = t2.strftime("%Y-%m-%d %H:%M:%S")

                producer.send(config.TOPIC_RAW, value=tx2)
                logger.warning(f"⚠️  INJECTED FRAUD: User {victim_id[:8]}... jumped to {tx2['country']} in 2 mins!")

            else:
                # Standard random transaction
                tx = generate_transaction()
                producer.send(config.TOPIC_RAW, value=tx)
                logger.debug(f"Sent normal tx: {tx['transaction_id']}")

            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("🛑 Stopping stream...")
        producer.close()

if __name__ == "__main__":
    start_stream()