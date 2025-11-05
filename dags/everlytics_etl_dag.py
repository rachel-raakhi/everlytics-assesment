from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# Postgres connection ID
POSTGRES_CONN_ID = 'postgres_quickshop'

# Default arguments for DAG
default_args = {
    "owner": "raakhi",
    "start_date": datetime(2025, 10, 25),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

# Define DAG
with DAG(
    dag_id="quickshop_daily_pipeline",
    default_args=default_args,
    schedule_interval=None,
    tags=["quickshop", "etl"],
) as dag:

    # -------------------------
    # 1. ETL Task
    # -------------------------
    run_etl_task = BashOperator(
        task_id="run_daily_etl",
        bash_command="PYTHONPATH=/opt/airflow/ python /opt/airflow/quickshop_etl/run_etl.py --start-date {{ ds_nodash }}",
    )

    # -------------------------
    # 2. SQL Reporting Tasks
    # -------------------------
    run_daily_revenue_sql = PostgresOperator(
        task_id='calculate_daily_revenue',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='sql/daily_revenue.sql',
    )

    run_product_performance_sql = PostgresOperator(
        task_id='calculate_product_performance',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='sql/product_performance.sql',
    )

    run_inventory_alerts_sql = PostgresOperator(
        task_id='generate_inventory_alerts',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='sql/inventory_alerts.sql',
    )

    run_returns_analysis_sql = PostgresOperator(
        task_id='analyze_product_returns',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='sql/returns_analysis.sql',
    )

    run_cohort_retention_sql = PostgresOperator(
        task_id='calculate_cohort_retention',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='sql/cohort_retention.sql',
    )

    # -------------------------
    # 3. Python Reporting Tasks
    # -------------------------

    # Lazy import inside task to avoid heavy module loading at DAG parse time
    def _get_daily_revenue_data(**context):
        from quickshop_etl.reporting import get_daily_revenue_data
        return get_daily_revenue_data()

    def _create_summary_json(**context):
        from quickshop_etl.reporting import create_summary_json
        return create_summary_json()

    get_revenue_summary = PythonOperator(
        task_id='get_daily_revenue_summary',
        python_callable=_get_daily_revenue_data,
    )

    create_json_report = PythonOperator(
        task_id='create_summary_json_report',
        python_callable=_create_summary_json,
    )

    # -------------------------
    # 4. Task Dependencies
    # -------------------------
    run_etl_task >> [
        run_daily_revenue_sql,
        run_product_performance_sql,
        run_inventory_alerts_sql,
        run_returns_analysis_sql,
        run_cohort_retention_sql
    ] >> get_revenue_summary >> create_json_report
