FROM apache/airflow:2.7.1-python3.11

USER airflow

RUN pip install --no-cache-dir pandas psycopg2-binary sqlalchemy apache-airflow-providers-postgres
