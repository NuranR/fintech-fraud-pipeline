import logging
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, collect_set, 
    size, array_distinct, expr, lit, struct, first
)
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
FRAUD_PARQUET_PATH = os.path.join(PROJECT_ROOT, "data/lake/fraud")  # Fraud data for batch reporting

# Fraud Detection Config
IMPOSSIBLE_TRAVEL_WINDOW = "10 minutes"  # Window to detect impossible travel
WATERMARK_DELAY = "2 minutes"            # Allow late data up to 2 minutes
HIGH_VALUE_THRESHOLD = 5000              # Amount threshold for high-value fraud

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
    
    # Cache data because we are using it multiple times
    batch_df.persist()

    # ===========================================
    # FRAUD TYPE 1: High Value Transactions (> $5000)
    # ===========================================
    high_value_fraud = batch_df.filter(col("amount") > HIGH_VALUE_THRESHOLD) \
        .withColumn("fraud_type", lit("HIGH_VALUE")) \
        .withColumn("fraud_reason", expr(f"concat('Amount $', amount, ' exceeds ${HIGH_VALUE_THRESHOLD} threshold')"))
    
    if not high_value_fraud.isEmpty():
        count = high_value_fraud.count()
        logger.warning(f"🚨 HIGH VALUE FRAUD: {count} transactions over ${HIGH_VALUE_THRESHOLD}!")

    # ===========================================
    # FRAUD TYPE 2: Impossible Travel Detection
    # Uses windowing to find same user in different countries within 10 minutes
    # ===========================================
    
    # Group transactions by user within a 10-minute TUMBLING window (non-overlapping)
    # This prevents the same fraud from being detected multiple times
    windowed_df = batch_df \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .groupBy(
            col("user_id"),
            window(col("event_time"), IMPOSSIBLE_TRAVEL_WINDOW)  # Tumbling window (no slide = no overlap)
        ) \
        .agg(
            collect_set("country").alias("countries"),
            collect_set("transaction_id").alias("transaction_ids"),
            first("timestamp").alias("first_timestamp"),
            first("amount").alias("amount"),
            first("currency").alias("currency"),
            first("merchant_category").alias("merchant_category")
        )
    
    # Filter users with transactions from multiple countries in the window
    impossible_travel_fraud = windowed_df \
        .filter(size(col("countries")) > 1) \
        .withColumn("fraud_type", lit("IMPOSSIBLE_TRAVEL")) \
        .withColumn("fraud_reason", expr("concat('User transacted from ', size(countries), ' countries: ', concat_ws(', ', countries), ' within 10 minutes')")) \
        .withColumn("country", expr("concat_ws(', ', countries)")) \
        .withColumn("transaction_id", expr("concat_ws(', ', transaction_ids)")) \
        .withColumn("timestamp", col("first_timestamp")) \
        .select("transaction_id", "user_id", "timestamp", "amount", "currency", 
                "merchant_category", "country", "fraud_type", "fraud_reason")
    
    if not impossible_travel_fraud.isEmpty():
        count = impossible_travel_fraud.count()
        # Show details in logs (use 'country' which now contains the joined countries list)
        impossible_travel_fraud.select("user_id", "country", "fraud_reason").show(truncate=False)
        logger.warning(f"🚨 IMPOSSIBLE TRAVEL FRAUD: {count} users detected in multiple countries!")

    # ===========================================
    # Combine all fraud and send to Kafka
    # ===========================================
    high_value_for_kafka = high_value_fraud.select(
        "transaction_id", "user_id", "timestamp", "amount", "currency",
        "merchant_category", "country", "fraud_type", "fraud_reason"
    )
    
    all_fraud = high_value_for_kafka.union(impossible_travel_fraud)
    
    if not all_fraud.isEmpty():
        total_fraud = all_fraud.count()
        logger.warning(f"📤 Sending {total_fraud} FRAUD alerts to Kafka topic: {OUTPUT_TOPIC_FRAUD}")
        
        # Send to Kafka for real-time alerts
        fraud_out = all_fraud.selectExpr("to_json(struct(*)) AS value")
        fraud_out.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", OUTPUT_TOPIC_FRAUD) \
            .save()
        
        # Also write fraud to Parquet for batch reporting (Airflow can read this)
        fraud_parquet = all_fraud.withColumn("date", to_timestamp(col("timestamp")).cast("date"))
        fraud_parquet.write \
            .mode("append") \
            .partitionBy("date") \
            .parquet(FRAUD_PARQUET_PATH)
        logger.info(f"💾 Saved {total_fraud} fraud transactions to Fraud Lake for batch reporting")

    # ===========================================
    # Valid transactions → Data Lake (Parquet)
    # ===========================================
    # Exclude high-value fraud from valid transactions
    valid_df = batch_df.filter(col("amount") <= HIGH_VALUE_THRESHOLD)
    
    if not valid_df.isEmpty():
        valid_out = valid_df.withColumn("date", to_timestamp(col("timestamp")).cast("date"))
        valid_out.write \
            .mode("append") \
            .partitionBy("date") \
            .parquet(PARQUET_OUTPUT_PATH)
        logger.info(f"💾 Saved {valid_df.count()} valid transactions to Data Lake")
            
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