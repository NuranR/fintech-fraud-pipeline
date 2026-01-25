# FinTech Fraud Detection Pipeline

Real-time credit card fraud detection system using Apache Kafka, Spark Structured Streaming, and Apache Airflow.

## Architecture

```
[Transaction Generator] → [Kafka] → [Spark Streaming] → [Parquet Data Lake]
                                            ↓
                                    [Fraud Alerts Topic]

[Airflow DAG] → Reads Parquet → Generates Reports (Every 6 Hours)
```

## Fraud Detection Rules

| Rule                  | Description                                             |
| --------------------- | ------------------------------------------------------- |
| **HIGH_VALUE**        | Any transaction > $5,000                                |
| **IMPOSSIBLE_TRAVEL** | Same user transacts from 2+ countries within 10 minutes |

## Quick Start

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Setup Python environment
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Mac/Linux
pip install -r requirements.txt

# 3. Initialize data folders (run from project root)
scripts\clean_data.bat       # Windows
# Or manually create: data/checkpoints, data/lake/transactions, data/reports

# 4. Start Spark fraud detector (Terminal 1)
cd src/processors
python fraud_detector.py

# 5. Start transaction generator (Terminal 2)
cd src/producers
python transaction_gen.py

# 6. View Kafka UI
open http://localhost:8080

# 7. View Airflow (admin/admin)
open http://localhost:8081
# Toggle the 'reconciliation_dag' to ON to enable scheduled reports
```

## Project Structure

```
├── docker-compose.yml      # Kafka, Zookeeper, Airflow, Postgres
├── requirements.txt        # Python dependencies
├── scripts/
│   └── clean_data.bat      # Reset data folders (checkpoints, lake, reports)
├── src/
│   ├── config.py           # Centralized configuration
│   ├── producers/
│   │   └── transaction_gen.py   # Simulates credit card transactions
│   ├── processors/
│   │   └── fraud_detector.py    # Spark Streaming fraud detection
│   └── jobs/
│       ├── reconciliation_dag.py  # Airflow DAG
│       └── generate_report.py     # Batch report generator
└── data/                   # Created by scripts/clean_data.bat
    ├── checkpoints/        # Spark streaming state
    ├── lake/               # Parquet files (transactions & fraud)
    └── reports/            # Generated batch reports
```

## Reports Generated

1. **Reconciliation Report** - Compares Ingress vs Validated amounts
2. **Analytics Report** - Fraud breakdown by Merchant Category

## Tech Stack

- **Apache Kafka** - Message streaming
- **Apache Spark Structured Streaming** - Real-time processing
- **Apache Airflow** - Batch orchestration
- **Parquet** - Data lake storage
- **Python** - PySpark, kafka-python, pandas
