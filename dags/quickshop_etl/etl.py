import pandas as pd
from sqlalchemy import create_engine
import logging
import os

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

DATABASE_URL = os.environ.get(
    "QUICKSHOP_DB_CONN", "postgresql://user:password@localhost:5432/quickshop_db")
ENGINE = create_engine(DATABASE_URL)
DATA_DIR = "/opt/airflow/data"


def extract_data(date_str: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    products_df = pd.read_csv(f"{DATA_DIR}/products.csv")
    inventory_df = pd.read_csv(f"{DATA_DIR}/inventory.csv")

    order_file_name = f"orders_{date_str}.csv"
    try:
        orders_df = pd.read_csv(f"{DATA_DIR}/{order_file_name}")
        orders_df['order_date'] = pd.to_datetime(
            orders_df['order_date']).dt.date
    except FileNotFoundError:
        logging.error(f"Daily order file not found: {order_file_name}")
        return products_df, inventory_df, pd.DataFrame()

    return products_df, inventory_df, orders_df


def transform_data(products_df: pd.DataFrame, inventory_df: pd.DataFrame, orders_df: pd.DataFrame) -> pd.DataFrame:
    if orders_df.empty:
        return pd.DataFrame()

    orders_df = orders_df.rename(
        columns={'qty': 'quantity', 'user_id': 'customer_id'})
    products_df = products_df.rename(
        columns={'price': 'unit_price', 'product_name': 'name'})

    orders_enriched = orders_df.merge(
        products_df[['product_id', 'category', 'name']],
        on='product_id',
        how='left'
    )

    orders_enriched['order_total'] = orders_enriched['quantity'] * \
        orders_enriched['unit_price']

    orders_enriched['is_returned'] = (
        orders_enriched['order_status'] == 'returned')

    orders_enriched = orders_enriched[orders_enriched['order_status'] != 'cancelled']

    final_orders = orders_enriched[[
        'order_id', 'customer_id', 'product_id', 'name', 'category',
        'quantity', 'order_total', 'order_date', 'is_returned', 'order_status'
    ]]

    return final_orders


def load_data(products_df: pd.DataFrame, inventory_df: pd.DataFrame, final_orders_df: pd.DataFrame, date_str: str):

    products_df.to_sql('products', ENGINE, if_exists='replace', index=False)
    inventory_df.to_sql('inventory', ENGINE, if_exists='replace', index=False)

    sql_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"

    delete_sql = f"DELETE FROM clean_orders WHERE order_date = '{sql_date}';"
    try:
        with ENGINE.begin() as connection:
            connection.execute(delete_sql)
    except:
        pass

    if not final_orders_df.empty:
        final_orders_df.to_sql('clean_orders', ENGINE,
                               if_exists='append', index=False)

    logging.info(f"Load complete. Data loaded for {sql_date}.")


def run_daily_pipeline(date_str: str):
    products, inventory, orders = extract_data(date_str)
    final_orders = transform_data(products, inventory, orders)
    load_data(products, inventory, final_orders, date_str)
