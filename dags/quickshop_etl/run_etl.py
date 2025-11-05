import argparse
import logging
import pandas as pd
import os
from sqlalchemy import create_engine
from datetime import datetime

# Database connection details (match Airflow connection)
DB_USER = "airflow"
DB_PASSWORD = "airflow"
DB_HOST = "postgres"
DB_PORT = "5432"
DB_NAME = "airflow"


def run_daily_pipeline(date_str: str):
    """
    This function performs a simple ETL:
    1. Reads or creates raw orders data
    2. Cleans and transforms it
    3. Loads it into Postgres as 'clean_orders'
    """

    input_path = "/opt/airflow/data/orders_raw.csv"

    # --- Step 1: Create dummy data if file doesn't exist ---
    if not os.path.exists(input_path):
        logging.warning(f"{input_path} not found. Creating dummy raw data...")
        os.makedirs(os.path.dirname(input_path), exist_ok=True)
        dummy_data = {
            "order_id": [1, 2, 3, 4],
            "order_date": [date_str]*4,
            "category": ["Electronics", "Clothing", "Books", "Groceries"],
            "quantity": [2, 1, 3, 5],
            "unit_price": [200, 150, 100, 50],
            "status": ["completed", "completed", "returned", "completed"]
        }
        pd.DataFrame(dummy_data).to_csv(input_path, index=False)
        logging.info(f"Dummy data created at {input_path}")

    # --- Step 2: Load raw data ---
    logging.info(f"Reading raw data for {date_str} from {input_path}")
    df = pd.read_csv(input_path)

    # --- Step 3: Clean / transform ---
    logging.info("Cleaning and transforming data...")
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
    df["is_returned"] = df["status"].str.lower() == "returned"
    df["order_total"] = df["quantity"] * df["unit_price"]

    # --- Step 4: Write to Postgres ---
    conn_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(conn_str)

    logging.info("Loading cleaned data into Postgres (table: clean_orders)...")
    df.to_sql("clean_orders", engine, if_exists="replace", index=False)
    logging.info("✅ ETL pipeline completed successfully!")


def main():
    parser = argparse.ArgumentParser(description="Run QuickShop ETL process.")
    parser.add_argument("--start-date", required=True,
                        help="Start date (YYYYMMDD)")
    args = parser.parse_args()

    date_str = args.start_date
    logging.basicConfig(level=logging.INFO)
    logging.info(f"--- Running ETL for date: {date_str} ---")

    try:
        run_daily_pipeline(date_str)
        logging.info(f"--- ETL successful for {date_str} ---")
    except Exception as e:
        logging.error(f"--- ETL FAILED for {date_str}: {e} ---")
        raise e


if __name__ == "__main__":
    main()
