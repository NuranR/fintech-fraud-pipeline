import pandas as pd
import os
import glob
from datetime import datetime

# Paths mapped via docker volumes
DATA_LAKE_PATH = "/opt/airflow/data/lake/transactions"
REPORT_OUTPUT_PATH = "/opt/airflow/data/reports"


def generate_daily_report():
    """
    Generate a reconciliation report based on the Parquet data lake.
    This is a FAST report that only reads from disk (no Kafka).
    """
    print(f"Starting reconciliation job at {datetime.now()}...")

    # =========================================
    # STEP 1: Read validated transactions from Data Lake
    # =========================================
    files = glob.glob(f"{DATA_LAKE_PATH}/**/*.parquet", recursive=True)
    
    if not files:
        print("No data found to report on.")
        # Still generate a report showing no data
        report_content = f"""
        =====================================================
        FINTECH PIPELINE - RECONCILIATION REPORT
        Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        Report Frequency: Every 6 Hours
        =====================================================
        
        STATUS: NO DATA AVAILABLE
        
        No parquet files found in the data lake.
        Please ensure the fraud_detector.py Spark job is running.
        
        =====================================================
        END OF REPORT
        """
        os.makedirs(REPORT_OUTPUT_PATH, exist_ok=True)
        filename = f"reconciliation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        full_path = os.path.join(REPORT_OUTPUT_PATH, filename)
        with open(full_path, "w") as f:
            f.write(report_content)
        print(f"Report generated: {full_path}")
        print(report_content)
        return

    print(f"Found {len(files)} parquet files. Processing...")

    try:
        df = pd.read_parquet(files)
        
        # Calculate validated metrics
        validated_tx_count = len(df)
        validated_volume = df['amount'].sum()
        unique_users = df['user_id'].nunique()
        
        # =========================================
        # STEP 2: Country breakdown
        # =========================================
        country_analysis = ""
        if 'country' in df.columns:
            country_stats = df.groupby('country').agg(
                count=('transaction_id', 'count'),
                total_amount=('amount', 'sum')
            ).sort_values('total_amount', ascending=False)
            
            country_analysis = "\n        TRANSACTIONS BY COUNTRY:\n        ------------------------\n"
            for country, row in country_stats.head(10).iterrows():
                country_analysis += f"        {country:20} : {int(row['count']):6} txns | ${row['total_amount']:>12,.2f}\n"
        
        # =========================================
        # STEP 3: Merchant category breakdown
        # =========================================
        merchant_analysis = ""
        if 'merchant_category' in df.columns:
            merchant_stats = df.groupby('merchant_category').agg(
                count=('transaction_id', 'count'),
                total_amount=('amount', 'sum')
            ).sort_values('total_amount', ascending=False)
            
            merchant_analysis = "\n        TRANSACTIONS BY MERCHANT CATEGORY:\n        -----------------------------------\n"
            for category, row in merchant_stats.iterrows():
                merchant_analysis += f"        {category:15} : {int(row['count']):6} txns | ${row['total_amount']:>12,.2f}\n"
        
        # =========================================
        # STEP 4: Time-based analysis
        # =========================================
        time_analysis = ""
        if 'timestamp' in df.columns:
            df['hour'] = pd.to_datetime(df['timestamp']).dt.hour
            hourly_stats = df.groupby('hour').size()
            peak_hour = hourly_stats.idxmax()
            time_analysis = f"\n        Peak Transaction Hour: {peak_hour}:00 ({hourly_stats[peak_hour]} transactions)"
        
        # =========================================
        # STEP 5: Generate Report
        # =========================================
        report_content = f"""
        =====================================================
        FINTECH PIPELINE - RECONCILIATION REPORT
        Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        Report Frequency: Every 6 Hours
        =====================================================
        
        DATA LAKE SUMMARY:
        ------------------
        Total Validated Transactions: {validated_tx_count:,}
        Total Transaction Volume    : ${validated_volume:,.2f}
        Active Users                : {unique_users:,}
        Average Transaction Amount  : ${validated_volume / validated_tx_count if validated_tx_count > 0 else 0:,.2f}
        Files Processed             : {len(files)}
        {country_analysis}
        {merchant_analysis}
        {time_analysis}
        
        NOTE: This report shows VALID transactions only.
        Fraudulent transactions are blocked and sent to
        the 'fraud-alerts' Kafka topic.
        
        =====================================================
        END OF REPORT
        """

        # Save report
        os.makedirs(REPORT_OUTPUT_PATH, exist_ok=True)
        filename = f"reconciliation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        full_path = os.path.join(REPORT_OUTPUT_PATH, filename)
        
        with open(full_path, "w") as f:
            f.write(report_content)
            
        print(f"Report generated successfully: {full_path}")
        print(report_content)
        
    except Exception as e:
        print(f"Error generating report: {e}")
        raise e

if __name__ == "__main__":
    generate_daily_report()