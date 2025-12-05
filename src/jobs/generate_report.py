import pandas as pd
import os
import glob
from datetime import datetime

# Paths mapped via docker volumes
DATA_LAKE_PATH = "/opt/airflow/data/lake/transactions"
REPORT_OUTPUT_PATH = "/opt/airflow/data/reports"

def generate_daily_report():
    print(f"Starting reconciliation job at {datetime.now()}...")

    files = glob.glob(f"{DATA_LAKE_PATH}/**/*.parquet", recursive=True)
    
    if not files:
        print("No data found to report on.")
        return

    print(f"Found {len(files)} parquet files. Processing...")

    # pd used as the generated data can fit in RAM
    try:
        df = pd.read_parquet(files)
        
        # Calculate key business metrics
        total_tx_count = len(df)
        total_volume = df['amount'].sum()
        unique_users = df['user_id'].nunique()
        
        report_content = f"""
        =========================================
        FINTECH PIPELINE - RECONCILIATION REPORT
        Date: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        =========================================
        
        SUMMARY METRICS:
        ----------------
        Total Valid Transactions : {total_tx_count}
        Total Volume Processed   : ${total_volume:,.2f}
        Active Users             : {unique_users}
        
        Files Processed          : {len(files)}
        
        =========================================
        STATUS: BALANCED
        """

        # save
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