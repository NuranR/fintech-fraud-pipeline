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

# Track active users for realistic fraud simulation
active_users = {}
USER_POOL_SIZE = 50  # Pool of recurring users for realistic patterns

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

def generate_transaction(user_id=None, country=None):
    if not user_id:
        user_id = fake.uuid4()
        
    return {
        "transaction_id": fake.uuid4(),
        "user_id": user_id,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "amount": round(random.uniform(10.0, 4500.0), 2),  # Normal transactions under $5000
        "currency": "USD",
        "merchant_category": random.choice(["Food", "Electronics", "Travel", "Fashion"]),
        "country": country if country else random.choice(config.COUNTRIES)
    }

def generate_high_value_fraud():
    """Generate a transaction with amount > $5000 (automatic fraud)"""
    tx = generate_transaction()
    tx['amount'] = round(random.uniform(5001.0, 15000.0), 2)
    return tx

def generate_impossible_travel_fraud():
    """
    Generate impossible travel fraud:
    Same user transacts from two DIFFERENT countries within the time window.
    The fraud detector will catch this pattern.
    """
    user_id = fake.uuid4()
    
    # Pick two DIFFERENT random countries
    country1 = random.choice(config.COUNTRIES)
    country2 = random.choice([c for c in config.COUNTRIES if c != country1])
    
    # First transaction from country1
    tx1 = generate_transaction(user_id=user_id, country=country1)
    
    # Second transaction from country2 (within the detection window)
    tx2 = generate_transaction(user_id=user_id, country=country2)
    # Add small time offset (1-5 minutes) - well within 10 min window
    t1 = datetime.strptime(tx1['timestamp'], "%Y-%m-%d %H:%M:%S")
    t2 = t1 + timedelta(seconds=random.randint(60, 300))
    tx2['timestamp'] = t2.strftime("%Y-%m-%d %H:%M:%S")
    
    return tx1, tx2, country1, country2

def start_stream():
    create_topic_if_missing()
    producer = get_producer()
    logger.info(f"🚀 Starting Producer. Target Topic: {config.TOPIC_RAW}")
    logger.info(f"📊 Fraud injection probability: {config.FRAUD_TRIGGER_PROBABILITY * 100}%")

    try:
        while True:
            rand = random.random()
            
            # 10% chance: Inject impossible travel fraud (two transactions, different countries)
            if rand < config.FRAUD_TRIGGER_PROBABILITY:
                tx1, tx2, country1, country2 = generate_impossible_travel_fraud()
                
                # Send first transaction
                producer.send(config.TOPIC_RAW, value=tx1)
                logger.info(f"🌍 User {tx1['user_id'][:8]}... transacted from {country1}")
                
                # Small delay to simulate real-world timing
                time.sleep(random.uniform(0.5, 2))
                
                # Send second transaction (impossible travel)
                producer.send(config.TOPIC_RAW, value=tx2)
                logger.warning(f"⚠️  IMPOSSIBLE TRAVEL: User {tx2['user_id'][:8]}... jumped {country1} → {country2}!")
            
            # 5% chance: High value fraud (> $5000)
            elif rand < config.FRAUD_TRIGGER_PROBABILITY + 0.05:
                tx = generate_high_value_fraud()
                producer.send(config.TOPIC_RAW, value=tx)
                logger.warning(f"💰 HIGH VALUE: User {tx['user_id'][:8]}... spent ${tx['amount']:.2f}")
            
            # 85% chance: Normal legitimate transaction
            else:
                tx = generate_transaction()
                producer.send(config.TOPIC_RAW, value=tx)
                logger.debug(f"✅ Normal: {tx['user_id'][:8]}... ${tx['amount']:.2f} in {tx['country']}")

            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("🛑 Stopping stream...")
        producer.close()

if __name__ == "__main__":
    start_stream()