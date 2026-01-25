import logging
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Get project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Windows environment compatibility for Spark
if sys.platform.startswith('win'):
    os.environ['HADOOP_HOME'] = PROJECT_ROOT
    os.environ['hadoop.home.dir'] = PROJECT_ROOT

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "transactions-raw"
OUTPUT_TOPIC_FRAUD = "fraud-alerts"
CHECKPOINT_LOCATION = os.path.join(PROJECT_ROOT, "data/checkpoints/fraud_job")
PARQUET_OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data/lake/transactions")

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("FraudDetector")

# Schema definition matching producer
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("country", StringType(), True)
])
    
def create_spark_session():
    return SparkSession.builder \
        .appName("FinTechFraudDetector") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "3") \
        .config("spark.ui.port", "0") \
        .getOrCreate()

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
        
    logger.info(f"⚡ Processing Batch ID: {batch_id} with {batch_df.count()} records")
    
    # Cache data because we are using it twice, to avoid reading from kafka again
    batch_df.persist()

    # Fraud Detection Logic
    # Criteria: Amount > 5000 OR Country is UK (Simulation target)
    fraud_df = batch_df.filter((col("amount") > 5000) | (col("country") == "United Kingdom"))
    valid_df = batch_df.filter((col("amount") <= 5000) & (col("country") != "United Kingdom"))

    # Sink 1: High priority alerts to Kafka
    if not fraud_df.isEmpty():
        logger.warning(f"🚨 Detected {fraud_df.count()} FRAUD transactions!")
        fraud_out = fraud_df.selectExpr("to_json(struct(*)) AS value")
        
        fraud_out.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", OUTPUT_TOPIC_FRAUD) \
            .save()

    # Sink 2: Valid transactions to Data Lake (Parquet)
    if not valid_df.isEmpty():
        # Adding date column to partition folders by day (YYYY-MM-DD)
        valid_out = valid_df.withColumn("date", to_timestamp(col("timestamp")).cast("date"))
        
        valid_out.write \
            .mode("append") \
            .partitionBy("date") \
            .parquet(PARQUET_OUTPUT_PATH)
            
    batch_df.unpersist()

def run():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN") # Hide verbose logs
    
    logger.info("Starting Stream Processing...")

    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    json_stream = raw_stream.select(
        from_json(col("value").cast("string"), transaction_schema).alias("data")
    ).select("data.*")

    # Start Micro-batch execution (every 5 seconds)
    query = json_stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run()