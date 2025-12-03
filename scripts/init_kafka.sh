#!/bin/bash

echo "⏳ Waiting for Kafka to be ready..."
# loop until the 'kafka-topics' command works
while ! docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 > /dev/null 2>&1; do
  sleep 2
done

echo "✅ Kafka is up!"

echo "🛠  Creating topic: transactions-raw"
docker exec kafka kafka-topics --create \
  --if-not-exists \
  --topic transactions-raw \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

echo "🎉 Setup Complete!"