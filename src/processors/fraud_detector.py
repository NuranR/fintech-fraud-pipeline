import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, collect_set,
    size, expr, lit, first
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Import centralized config
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import config

# Windows environment compatibility for Spark
if sys.platform.startswith('win'):
    os.environ['HADOOP_HOME'] = config.PROJECT_ROOT
    os.environ['hadoop.home.dir'] = config.PROJECT_ROOT

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
    """
    Process each micro-batch of transactions.
    
    Detects fraud and routes to appropriate sinks:
    - Fraud → Kafka + Parquet
    - Valid → Parquet
    """
    if batch_df.isEmpty():
        return

    logger.info(f"⚡ Processing Batch ID: {batch_id} with {batch_df.count()} records")

    # Cache data because we use it multiple times
    batch_df.persist()

    # ===========================================
    # FRAUD TYPE 1: High Value Transactions
    # Amount > threshold (ANY location - domestic OR foreign)
    # ===========================================
    high_value_fraud = batch_df.filter(
        col("amount") > config.HIGH_VALUE_THRESHOLD
    ).withColumn("fraud_type", lit("HIGH_VALUE")) \
     .withColumn("fraud_reason", expr(
        f"concat('Amount $', amount, ' exceeds ${config.HIGH_VALUE_THRESHOLD} threshold in ', country)"
     ))

    if not high_value_fraud.isEmpty():
        count = high_value_fraud.count()
        logger.warning(f"🚨 HIGH VALUE: {count} suspicious transactions detected!")

    # ===========================================
    # FRAUD TYPE 2: Impossible Travel Detection
    # Same user in different countries within time window
    # ===========================================
    
    # Group transactions by user within a tumbling window
    windowed_df = batch_df \
        .withColumn("event_time", to_timestamp(col("timestamp"))) \
        .groupBy(
            col("user_id"),
            window(col("event_time"), config.IMPOSSIBLE_TRAVEL_WINDOW)
        ) \
        .agg(
            collect_set("country").alias("countries"),
            collect_set("transaction_id").alias("transaction_ids"),
            first("timestamp").alias("first_timestamp"),
            first("amount").alias("amount"),
            first("currency").alias("currency"),
            first("merchant_category").alias("merchant_category")
        )

    # Filter users with transactions from multiple countries
    impossible_travel_fraud = windowed_df \
        .filter(size(col("countries")) > 1) \
        .withColumn("fraud_type", lit("IMPOSSIBLE_TRAVEL")) \
        .withColumn("fraud_reason", expr(
            "concat('User transacted from ', size(countries), ' countries: ', concat_ws(', ', countries), ' within 10 minutes')"
        )) \
        .withColumn("country", expr("concat_ws(', ', countries)")) \
        .withColumn("transaction_id", expr("concat_ws(', ', transaction_ids)")) \
        .withColumn("timestamp", col("first_timestamp")) \
        .select("transaction_id", "user_id", "timestamp", "amount", "currency",
                "merchant_category", "country", "fraud_type", "fraud_reason")

    if not impossible_travel_fraud.isEmpty():
        count = impossible_travel_fraud.count()
        impossible_travel_fraud.select("user_id", "country", "fraud_reason").show(truncate=False)
        logger.warning(f"🚨 IMPOSSIBLE TRAVEL: {count} users detected in multiple countries!")

    # ===========================================
    # Combine all fraud and send to Kafka + Parquet
    # ===========================================
    high_value_for_kafka = high_value_fraud.select(
        "transaction_id", "user_id", "timestamp", "amount", "currency",
        "merchant_category", "country", "fraud_type", "fraud_reason"
    )

    all_fraud = high_value_for_kafka.union(impossible_travel_fraud)

    if not all_fraud.isEmpty():
        total_fraud = all_fraud.count()
        logger.warning(f"📤 Sending {total_fraud} FRAUD alerts to Kafka: {config.TOPIC_FRAUD}")

        # Send to Kafka for real-time alerts
        fraud_out = all_fraud.selectExpr("to_json(struct(*)) AS value")
        fraud_out.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", config.TOPIC_FRAUD) \
            .save()

        # Write fraud to Parquet for batch reporting
        fraud_parquet = all_fraud.withColumn("date", to_timestamp(col("timestamp")).cast("date"))
        fraud_parquet.write \
            .mode("append") \
            .partitionBy("date") \
            .parquet(config.LOCAL_FRAUD_LAKE_PATH)
        logger.info(f"💾 Saved {total_fraud} fraud transactions to Fraud Lake")

    # ===========================================
    # Valid transactions → Data Lake (Parquet)
    # Exclude ALL high-value transactions from valid
    # ===========================================
    valid_df = batch_df.filter(
        col("amount") <= config.HIGH_VALUE_THRESHOLD
    )

    if not valid_df.isEmpty():
        valid_out = valid_df.withColumn("date", to_timestamp(col("timestamp")).cast("date"))
        valid_out.write \
            .mode("append") \
            .partitionBy("date") \
            .parquet(config.LOCAL_DATA_LAKE_PATH)
        logger.info(f"💾 Saved {valid_df.count()} valid transactions to Data Lake")

    batch_df.unpersist()


def run():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("=" * 60)
    logger.info(f"🚀 Starting Fraud Detector")
    logger.info(f"📊 Kafka Source: {config.TOPIC_RAW}")
    logger.info(f"📊 Kafka Sink: {config.TOPIC_FRAUD}")
    logger.info(f"💰 High-Value Threshold: ${config.HIGH_VALUE_THRESHOLD:,}")
    logger.info(f"⏱️  Impossible Travel Window: {config.IMPOSSIBLE_TRAVEL_WINDOW}")
    logger.info("=" * 60)

    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", config.TOPIC_RAW) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    json_stream = raw_stream.select(
        from_json(col("value").cast("string"), transaction_schema).alias("data")
    ).select("data.*")

    # Start micro-batch execution
    query = json_stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", config.LOCAL_CHECKPOINT_PATH) \
        .trigger(processingTime=config.SPARK_TRIGGER_INTERVAL) \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    run()
