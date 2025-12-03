@echo off
echo Waiting for Kafka to be ready...
timeout /t 5 /nobreak >nul

echo Creating topic: transactions-raw...
docker exec kafka kafka-topics --create --if-not-exists --topic transactions-raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo.
echo Topic created.