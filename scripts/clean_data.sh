#!/bin/bash

echo "Starting Data Cleanup..."

echo "Deleting checkpoints (Spark State)..."
rm -rf "data/checkpoints"

echo "Deleting data lake (Parquet files)..."
rm -rf "data/lake"

echo "Deleting reports (Airflow outputs)..."
rm -rf "data/reports"

echo ""
echo "Recreating empty folders..."
mkdir -p "data/checkpoints"
mkdir -p "data/lake/transactions"
mkdir -p "data/reports"

echo ""
echo "Clean up complete"
