
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Local paths (for Spark jobs running on host machine)
LOCAL_DATA_LAKE_PATH = os.path.join(PROJECT_ROOT, "data", "lake", "transactions")
LOCAL_FRAUD_LAKE_PATH = os.path.join(PROJECT_ROOT, "data", "lake", "fraud")
LOCAL_CHECKPOINT_PATH = os.path.join(PROJECT_ROOT, "data", "checkpoints", "fraud_job")
LOCAL_REPORT_PATH = os.path.join(PROJECT_ROOT, "data", "reports")

# Docker/Airflow paths (mounted via docker-compose volumes)
DOCKER_DATA_LAKE_PATH = "/opt/airflow/data/lake/transactions"
DOCKER_FRAUD_LAKE_PATH = "/opt/airflow/data/lake/fraud"
DOCKER_REPORT_PATH = "/opt/airflow/data/reports"

# KAFKA CONFIGURATION
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_BOOTSTRAP_SERVERS_INTERNAL = "kafka:29092"  # For Docker internal communication

# Kafka Topics
TOPIC_RAW = "transactions-raw"          # Raw incoming transactions
TOPIC_FRAUD = "fraud-alerts"            # Real-time fraud alerts
# TOPIC_VALID = "transactions-validated"  # Validated transactions

HOME_COUNTRY = "Sri Lanka"
BRANCH_COUNTRIES = [
    "Sri Lanka",      # Home country
    "USA",
    "United Kingdom",
    "Singapore",
    "Australia",
    "Germany",
    "Japan",
    "UAE"
]

HIGH_VALUE_THRESHOLD = 5000
IMPOSSIBLE_TRAVEL_WINDOW = "10 minutes"

# Watermark delay for late-arriving data in streaming
WATERMARK_DELAY = "2 minutes"

# Spark micro-batch processing interval
SPARK_TRIGGER_INTERVAL = "5 seconds"

# TRANSACTION GENERATOR CONFIGURATION
FRAUD_PROBABILITY_HIGH_VALUE_FOREIGN = 0.05   # 5% high-value from foreign
FRAUD_PROBABILITY_HIGH_VALUE_DOMESTIC = 0.05  # 5% high-value domestic 
FRAUD_PROBABILITY_IMPOSSIBLE_TRAVEL = 0.05    # 5% impossible travel
# Remaining ~85% are normal transactions

# Percentage of NORMAL transactions that are domestic (from HOME_COUNTRY)
DOMESTIC_TRANSACTION_RATIO = 0.85  # 85% domestic, 15% foreign (legitimate)

# Merchant categories for transaction generation
MERCHANT_CATEGORIES = ["Food", "Electronics", "Travel", "Fashion", "Utilities", "Healthcare"]

# AIRFLOW CONFIGURATION
DAG_SCHEDULE_INTERVAL = "0 */6 * * *"  # Every 6 hours (00:00, 06:00, 12:00, 18:00)

# HELPER FUNCTIONS

def get_foreign_countries():
    return [c for c in BRANCH_COUNTRIES if c != HOME_COUNTRY]


def is_foreign_country(country):
    return country != HOME_COUNTRY


def is_valid_branch_country(country):
    return country in BRANCH_COUNTRIES
