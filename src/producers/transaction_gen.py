import time
import json
import random
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from faker import Faker

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import config

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

fake = Faker()


def create_topic_if_missing():
    admin_client = KafkaAdminClient(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        client_id='admin_setup'
    )

    existing_topics = admin_client.list_topics()
    topics_to_check = [config.TOPIC_RAW, config.TOPIC_FRAUD]
    new_topics_list = []

    for topic in topics_to_check:
        if topic not in existing_topics:
            logger.info(f"Topic '{topic}' not found. Queueing for creation...")
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
    """Create and return a Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )


def generate_transaction(user_id=None, country=None, amount=None):
    """
    Generate a single transaction.
    
    Args:
        user_id: Optional user ID (generates new if None)
        country: Optional country (uses weighted random if None)
        amount: Optional amount (generates random normal amount if None)
    """
    if not user_id:
        user_id = fake.uuid4()

    # Default: weighted country selection (85% domestic)
    if not country:
        if random.random() < config.DOMESTIC_TRANSACTION_RATIO:
            country = config.HOME_COUNTRY
        else:
            # Pick from foreign countries only
            foreign_countries = config.get_foreign_countries()
            country = random.choice(foreign_countries)

    # Default: normal transaction amount (under threshold)
    if not amount:
        amount = round(random.uniform(10.0, config.HIGH_VALUE_THRESHOLD - 500), 2)

    return {
        "transaction_id": fake.uuid4(),
        "user_id": user_id,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "amount": amount,
        "currency": "USD",
        "merchant_category": random.choice(config.MERCHANT_CATEGORIES),
        "country": country
    }


def generate_high_value_foreign():
    foreign_countries = config.get_foreign_countries()
    country = random.choice(foreign_countries)
    amount = round(random.uniform(config.HIGH_VALUE_THRESHOLD + 1, 15000.0), 2)
    
    tx = generate_transaction(country=country, amount=amount)
    return tx, country


def generate_high_value_domestic():
    amount = round(random.uniform(config.HIGH_VALUE_THRESHOLD + 1, 15000.0), 2)
    
    tx = generate_transaction(country=config.HOME_COUNTRY, amount=amount)
    return tx


def generate_impossible_travel_fraud():
    user_id = fake.uuid4()

    # Pick two DIFFERENT random countries from all branch countries
    country1 = random.choice(config.BRANCH_COUNTRIES)
    country2 = random.choice([c for c in config.BRANCH_COUNTRIES if c != country1])

    # First transaction
    tx1 = generate_transaction(user_id=user_id, country=country1)

    # Second transaction from different country (within detection window)
    tx2 = generate_transaction(user_id=user_id, country=country2)
    
    # Add small time offset (1-5 minutes) - within the 10 min window
    t1 = datetime.strptime(tx1['timestamp'], "%Y-%m-%d %H:%M:%S")
    t2 = t1 + timedelta(seconds=random.randint(60, 300))
    tx2['timestamp'] = t2.strftime("%Y-%m-%d %H:%M:%S")

    return tx1, tx2, country1, country2


def start_stream():
    create_topic_if_missing()
    producer = get_producer()
    
    total_fraud_prob = (config.FRAUD_PROBABILITY_HIGH_VALUE_FOREIGN + 
                         config.FRAUD_PROBABILITY_HIGH_VALUE_DOMESTIC + 
                         config.FRAUD_PROBABILITY_IMPOSSIBLE_TRAVEL)
    
    logger.info("=" * 60)
    logger.info(f"🚀 Starting Transaction Generator")
    logger.info(f"📊 Kafka Topic: {config.TOPIC_RAW}")
    logger.info(f"💰 High-Value Threshold: ${config.HIGH_VALUE_THRESHOLD:,}")
    logger.info(f"🌍 Domestic Ratio (normal tx): {config.DOMESTIC_TRANSACTION_RATIO * 100}%")
    logger.info(f"⚠️  Test Fraud Data: {total_fraud_prob * 100}%")
    logger.info(f"   - High-Value Foreign: {config.FRAUD_PROBABILITY_HIGH_VALUE_FOREIGN * 100}%")
    logger.info(f"   - High-Value Domestic: {config.FRAUD_PROBABILITY_HIGH_VALUE_DOMESTIC * 100}%")
    logger.info(f"   - Impossible Travel: {config.FRAUD_PROBABILITY_IMPOSSIBLE_TRAVEL * 100}%")
    logger.info("=" * 60)

    tx_count = 0
    fraud_count = 0

    # Calculate thresholds for random selection
    threshold_high_foreign = config.FRAUD_PROBABILITY_HIGH_VALUE_FOREIGN
    threshold_high_domestic = threshold_high_foreign + config.FRAUD_PROBABILITY_HIGH_VALUE_DOMESTIC
    threshold_impossible = threshold_high_domestic + config.FRAUD_PROBABILITY_IMPOSSIBLE_TRAVEL

    try:
        while True:
            rand = random.random()

            if rand < threshold_high_foreign:
                # High-value from foreign country
                tx, country = generate_high_value_foreign()
                producer.send(config.TOPIC_RAW, value=tx)
                logger.warning(f"💰 HIGH VALUE FOREIGN: ${tx['amount']:,.2f} from {country}")
                fraud_count += 1
                tx_count += 1

            elif rand < threshold_high_domestic:
                # High-value from HOME country
                tx = generate_high_value_domestic()
                producer.send(config.TOPIC_RAW, value=tx)
                logger.warning(f"💰 HIGH VALUE DOMESTIC: ${tx['amount']:,.2f} in {config.HOME_COUNTRY}")
                fraud_count += 1
                tx_count += 1

            elif rand < threshold_impossible:
                # Impossible travel fraud
                tx1, tx2, country1, country2 = generate_impossible_travel_fraud()

                # Send first transaction
                producer.send(config.TOPIC_RAW, value=tx1)
                logger.info(f"🌍 User {tx1['user_id'][:8]}... transacted from {country1}")
                tx_count += 1

                # Small delay to simulate timing
                time.sleep(random.uniform(0.5, 2))

                # Send second transaction (impossible travel)
                producer.send(config.TOPIC_RAW, value=tx2)
                logger.warning(f"⚠️  IMPOSSIBLE TRAVEL: {tx2['user_id'][:8]}... {country1} → {country2}!")
                fraud_count += 1
                tx_count += 1

            else:
                # Normal transaction (under threshold)
                tx = generate_transaction()
                producer.send(config.TOPIC_RAW, value=tx)
                
                # Only log occasional normal transactions to reduce noise
                if tx_count % 10 == 0:
                    logger.info(f"✅ Normal: {tx['user_id'][:8]}... ${tx['amount']:.2f} in {tx['country']}")
                tx_count += 1

            # Periodic status update
            if tx_count % 50 == 0:
                logger.info(f"📈 Stats: {tx_count} total | {fraud_count} fraud attempts")

            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("🛑 Stopping stream...")
        logger.info(f"📊 Final Stats: {tx_count} total | {fraud_count} fraud attempts")
        producer.close()


if __name__ == "__main__":
    start_stream()
