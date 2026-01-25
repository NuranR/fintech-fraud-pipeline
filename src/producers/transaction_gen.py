import time
import json
import random
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from faker import Faker
import config  # Imports config.py

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

fake = Faker()

def create_topic_if_missing():
    admin_client = KafkaAdminClient(
        bootstrap_servers=config.BOOTSTRAP_SERVERS,
        client_id='admin_setup'
    )

    existing_topics = admin_client.list_topics()

    topics_to_check = [config.TOPIC_RAW, config.TOPIC_FRAUD]
    new_topics_list = []
    
    for topic in topics_to_check:
        if topic not in existing_topics:
            logger.info(f"Topic '{topic}' not found. Queueing for creation...")
            # Create topic with 3 partitions
            new_topics_list.append(NewTopic(
                name=topic, 
                num_partitions=3, 
                replication_factor=1
            ))
        else:
            logger.info(f"Topic '{topic}' already exists.")

    # Creating all queued topics
    if new_topics_list:
        try:
            admin_client.create_topics(new_topics=new_topics_list, validate_only=False)
            logger.info(f"✅ Successfully created {len(new_topics_list)} new topics.")
        except Exception as e:
            logger.warning(f"Topic creation failed (might exist): {e}")
        
    admin_client.close()

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
        "amount": round(random.uniform(10.0, 6000.0), 2),
        "currency": "USD",
        "merchant_category": random.choice(["Food", "Electronics", "Travel", "Fashion"]),
        "country": random.choice(config.COUNTRIES)
    }

def start_stream():
    create_topic_if_missing()
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