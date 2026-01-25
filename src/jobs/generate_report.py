"""
Generates two reports for Airflow DAG:
1. RECONCILIATION REPORT - Ingress vs Validated amounts
2. ANALYTICS REPORT - Fraud by Merchant Category

Reads data from Parquet files in the data lake
"""
import pandas as pd
import os
import sys
import glob
from datetime import datetime

# Import centralized config
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    import config
    # Use Docker paths when running in Airflow container
    DATA_LAKE_PATH = config.DOCKER_DATA_LAKE_PATH
    FRAUD_LAKE_PATH = config.DOCKER_FRAUD_LAKE_PATH
    REPORT_OUTPUT_PATH = config.DOCKER_REPORT_PATH
    HOME_COUNTRY = config.HOME_COUNTRY
except ImportError:
    # Fallback for direct execution
    DATA_LAKE_PATH = "/opt/airflow/data/lake/transactions"
    FRAUD_LAKE_PATH = "/opt/airflow/data/lake/fraud"
    REPORT_OUTPUT_PATH = "/opt/airflow/data/reports"
    HOME_COUNTRY = "Sri Lanka"


def load_data():
    # Load valid and fraud transaction data from Parquet files in the data lake.
    valid_files = glob.glob(f"{DATA_LAKE_PATH}/**/*.parquet", recursive=True)
    fraud_files = glob.glob(f"{FRAUD_LAKE_PATH}/**/*.parquet", recursive=True)
    
    valid_df = None
    fraud_df = None
    
    if valid_files:
        valid_df = pd.read_parquet(valid_files)
    
    if fraud_files:
        fraud_df = pd.read_parquet(fraud_files)
    
    return valid_df, fraud_df, valid_files, fraud_files


def generate_reconciliation_report(valid_df, fraud_df, valid_files, fraud_files):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Calculate metrics
    validated_tx_count = len(valid_df) if valid_df is not None else 0
    validated_volume = valid_df['amount'].sum() if valid_df is not None else 0.0
    
    fraud_tx_count = len(fraud_df) if fraud_df is not None else 0
    fraud_volume = fraud_df['amount'].sum() if fraud_df is not None else 0.0
    
    total_ingress_count = validated_tx_count + fraud_tx_count
    total_ingress_volume = validated_volume + fraud_volume
    
    fraud_rate = (fraud_tx_count / total_ingress_count * 100) if total_ingress_count > 0 else 0
    reconciliation_diff = total_ingress_volume - validated_volume - fraud_volume
    
    report_content = f"""
=====================================================
       RECONCILIATION REPORT
=====================================================
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Schedule : Every 6 Hours
-----------------------------------------------------

This report compares TOTAL INGRESS (all transactions
received) against VALIDATED amount to ensure all
transactions are properly accounted for.

=== TRANSACTION VOLUME RECONCILIATION ===

┌─────────────────────────────────────────────────┐
│  TOTAL INGRESS (Raw Transactions Received)      │
│  ─────────────────────────────────────────────  │
│  Count  : {total_ingress_count:>12,}                          │
│  Volume : ${total_ingress_volume:>14,.2f}                       │
└─────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────┐
│  VALIDATED (Processed to Data Warehouse)        │
│  ─────────────────────────────────────────────  │
│  Count  : {validated_tx_count:>12,}                          │
│  Volume : ${validated_volume:>14,.2f}                       │
└─────────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────┐
│  FRAUD (Blocked & Quarantined)                  │
│  ─────────────────────────────────────────────  │
│  Count  : {fraud_tx_count:>12,}                          │
│  Volume : ${fraud_volume:>14,.2f}                       │
└─────────────────────────────────────────────────┘

=== RECONCILIATION CHECK ===

  Ingress - Validated - Fraud = ${reconciliation_diff:,.2f}
  
  Status: {"✓ BALANCED" if abs(reconciliation_diff) < 0.01 else "⚠ DISCREPANCY DETECTED"}
 
=== KEY METRICS ===

  Fraud Rate      : {fraud_rate:.2f}%
  Validation Rate : {100 - fraud_rate:.2f}%

=====================================================
           END OF RECONCILIATION REPORT
=====================================================
"""
    
    # Save report
    os.makedirs(REPORT_OUTPUT_PATH, exist_ok=True)
    filename = f"reconciliation_{timestamp}.txt"
    full_path = os.path.join(REPORT_OUTPUT_PATH, filename)
    
    with open(full_path, "w") as f:
        f.write(report_content)
    
    print(f"Reconciliation Report saved: {full_path}")
    return report_content


def generate_analytics_report(valid_df, fraud_df, valid_files, fraud_files):
    """
    Generate ANALYTICS REPORT: Fraud Attempts by Merchant Category.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    fraud_tx_count = len(fraud_df) if fraud_df is not None else 0
    fraud_volume = fraud_df['amount'].sum() if fraud_df is not None else 0.0
    
    # =========================================
    # Fraud by Merchant Category Analysis
    # =========================================
    fraud_by_merchant_section = "  No fraud data available for analysis.\n"
    
    if fraud_df is not None and len(fraud_df) > 0 and 'merchant_category' in fraud_df.columns:
        merchant_fraud = fraud_df.groupby('merchant_category').agg(
            count=('transaction_id', 'count'),
            total_amount=('amount', 'sum'),
            avg_amount=('amount', 'mean')
        ).sort_values('count', ascending=False)
        
        fraud_by_merchant_section = ""
        fraud_by_merchant_section += "  ┌─────────────────┬─────────┬───────────┬────────────────┬──────────────┐\n"
        fraud_by_merchant_section += "  │ Merchant        │ Count   │ % of All  │ Total Amount   │ Avg Amount   │\n"
        fraud_by_merchant_section += "  │ Category        │         │ Fraud     │                │              │\n"
        fraud_by_merchant_section += "  ├─────────────────┼─────────┼───────────┼────────────────┼──────────────┤\n"
        
        for category, row in merchant_fraud.iterrows():
            pct = (row['count'] / fraud_tx_count * 100) if fraud_tx_count > 0 else 0
            fraud_by_merchant_section += f"  │ {category:15} │ {int(row['count']):>7,} │ {pct:>7.1f}%  │ ${row['total_amount']:>12,.2f}  │ ${row['avg_amount']:>10,.2f}  │\n"
        
        fraud_by_merchant_section += "  └─────────────────┴─────────┴───────────┴────────────────┴──────────────┘\n"
    
    # =========================================
    # Fraud by Detection Type
    # =========================================
    fraud_by_type_section = "  No fraud type data available.\n"
    
    if fraud_df is not None and len(fraud_df) > 0 and 'fraud_type' in fraud_df.columns:
        type_fraud = fraud_df.groupby('fraud_type').agg(
            count=('transaction_id', 'count'),
            total_amount=('amount', 'sum')
        ).sort_values('count', ascending=False)
        
        fraud_by_type_section = ""
        for ftype, row in type_fraud.iterrows():
            pct = (row['count'] / fraud_tx_count * 100) if fraud_tx_count > 0 else 0
            bar_length = int(pct / 2)
            bar = "█" * bar_length + "░" * (50 - bar_length)
            fraud_by_type_section += f"  {ftype:20} : {int(row['count']):>6,} ({pct:>5.1f}%) │ ${row['total_amount']:>12,.2f}\n"
            fraud_by_type_section += f"                         {bar}\n"
    
    report_content = f"""
=====================================================
         ANALYTICS REPORT
     Fraud Attempts by Merchant Category
=====================================================
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Schedule : Every 6 Hours
-----------------------------------------------------

=== FRAUD SUMMARY ===

  Total Fraud Transactions : {fraud_tx_count:,}
  Total Fraud Volume       : ${fraud_volume:,.2f}
  Average Fraud Amount     : ${(fraud_volume/fraud_tx_count if fraud_tx_count > 0 else 0):,.2f}

=== FRAUD ATTEMPTS BY MERCHANT CATEGORY ===

{fraud_by_merchant_section}

=== FRAUD BY DETECTION TYPE ===

{fraud_by_type_section}

=====================================================
            END OF ANALYTICS REPORT
=====================================================
"""
    
    # Save report
    os.makedirs(REPORT_OUTPUT_PATH, exist_ok=True)
    filename = f"analytics_{timestamp}.txt"
    full_path = os.path.join(REPORT_OUTPUT_PATH, filename)
    
    with open(full_path, "w") as f:
        f.write(report_content)
    
    print(f"Analytics Report saved: {full_path}")
    return report_content


def generate_daily_report():
    """
    Main entry point for Airflow DAG.
    Generates two separate reports:
    1. RECONCILIATION REPORT - Ingress vs Validated amounts
    2. ANALYTICS REPORT - Fraud by Merchant Category
    """
    print(f"Starting report generation at {datetime.now()}...")
    print("="*60)

    # Load data from data lake (Parquet files)
    valid_df, fraud_df, valid_files, fraud_files = load_data()
    
    if not valid_files and not fraud_files:
        print("No data found in data lake. Generating empty reports...")
        os.makedirs(REPORT_OUTPUT_PATH, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        no_data_msg = f"""
=====================================================
REPORT STATUS: NO DATA AVAILABLE
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
=====================================================

No parquet files found in the data lake.
Please ensure the fraud_detector.py Spark job is running
and processing transactions.

Checked paths:
- Valid transactions: {DATA_LAKE_PATH}
- Fraud transactions: {FRAUD_LAKE_PATH}

=====================================================
"""
        for report_type in ["reconciliation", "analytics"]:
            filename = f"{report_type}_{timestamp}.txt"
            with open(os.path.join(REPORT_OUTPUT_PATH, filename), "w") as f:
                f.write(no_data_msg)
            print(f"Empty {report_type} report generated.")
        return
    
    print(f"Found {len(valid_files)} valid transaction files")
    print(f"Found {len(fraud_files)} fraud transaction files")
    print("="*60)
    
    try:
        # Generate Report 1: Reconciliation
        print("\n[1/2] Generating Reconciliation Report...")
        recon_report = generate_reconciliation_report(valid_df, fraud_df, valid_files, fraud_files)
        print(recon_report)
        
        # Generate Report 2: Analytics (Fraud by Merchant)
        print("\n[2/2] Generating Analytics Report...")
        analytics_report = generate_analytics_report(valid_df, fraud_df, valid_files, fraud_files)
        print(analytics_report)
        
        print("\n" + "="*60)
        print("All reports generated successfully!")
        print("="*60)
        
    except Exception as e:
        print(f"Error generating reports: {e}")
        raise e


if __name__ == "__main__":
    generate_daily_report()