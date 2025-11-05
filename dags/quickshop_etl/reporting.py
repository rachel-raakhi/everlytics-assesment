

import pandas as pd
from sqlalchemy import create_engine
import json
import os
import logging

logging.basicConfig(level=logging.INFO)

# Get DB connection or use a safe default
DATABASE_URL = os.environ.get(
    "QUICKSHOP_DB_CONN",
    "postgresql+psycopg2://user:password@postgres:5432/quickshop_db"
)

try:
    ENGINE = create_engine(DATABASE_URL)
except Exception as e:
    logging.error(f"❌ Failed to create engine: {e}")
    ENGINE = None


def get_daily_revenue_data(**context):
    execution_date = context['ds']
    ti = context['ti']

    sql_query = f"SELECT total_daily_revenue, top_category FROM daily_revenue_report WHERE order_date = '{execution_date}';"

    try:
        df = pd.read_sql(sql_query, con=ENGINE)
        if not df.empty:
            ti.xcom_push(key='daily_revenue_summary',
                         value=df.iloc[0].to_dict())
            logging.info(
                f"Pushed revenue summary to XCom: {df.iloc[0].to_dict()}")
        else:
            ti.xcom_push(key='daily_revenue_summary', value=None)
            logging.warning(f"No revenue data found for {execution_date}")
    except Exception as e:
        logging.error(f"Failed to fetch revenue data: {e}")
        ti.xcom_push(key='daily_revenue_summary', value=None)


def create_summary_json(**context):
    execution_date = context['ds']
    ti = context['ti']
    summary_data = ti.xcom_pull(
        task_ids='get_daily_revenue_summary', key='daily_revenue_summary')

    report = {
        'date': execution_date,
        'total_revenue': 0.0,
        'top_category': 'N/A',
        'status': 'No data processed'
    }

    if summary_data:
        report['total_revenue'] = float(
            summary_data.get('total_daily_revenue', 0))
        report['top_category'] = summary_data.get('top_category', 'N/A')
        report['status'] = 'Success'

    report_path = f"/opt/airflow/reports/{execution_date}_summary.json"
    os.makedirs("/opt/airflow/reports", exist_ok=True)

    with open(report_path, 'w') as f:
        json.dump(report, f, indent=4)

    logging.info(f"Final JSON report written to {report_path}")
