#  Quickshop Daily ETL Pipeline

**Automated data ingestion and reporting pipeline for an e-commerce platform built with Apache Airflow and Docker.**

This project demonstrates a robust, containerized solution for processing daily sales, product, and inventory data, generating critical business reports in parallel, and delivering a final summary output.

---

## ✨ Features

* **Containerized Environment:** Uses **Docker Compose** to manage Airflow (Webserver, Scheduler), and a dedicated **PostgreSQL** database.
* **Data Ingestion (ETL):** A Python script uses **Pandas** to extract and clean CSV data, joining orders with product information, and loading the result into a staging table (`clean_orders`).
* **Parallel Reporting:** Executes five concurrent **PostgreSQL** tasks to generate analytical reports (Revenue, Inventory, Returns, etc.).
* **Final Output:** Compiles all daily report data into a single, clean **JSON summary file**.
* **Idempotency:** SQL reports are designed to drop and re-create tables, ensuring clean daily runs.

---

## 🏗️ Architecture

The pipeline is defined by the `quickshop_daily_pipeline` DAG, which runs using Airflow's **LocalExecutor**.

| Step | Task Name | Type | Description |
| :--- | :--- | :--- | :--- |
| **1 (ETL)** | `run_daily_etl` | `BashOperator` (Python) | Reads raw CSVs, performs cleansing/joining, and loads data into the `clean_orders` table. |
| **2 (Reporting)** | 5 Tasks | `PostgresOperator` (SQL) | Runs five independent SQL queries concurrently to generate reports (e.g., `daily_revenue_report`). |
| **3 (Output)** | `create_summary_json_report` | `PythonOperator` | Connects to the database, extracts report results, and compiles the final `reports/YYYY-MM-DD_summary.json` file. |

---

## ⚙️ Tech Stack

| Tool | Purpose |
| :--- | :--- |
| **Apache Airflow 2.x** | Orchestration & Scheduling (LocalExecutor) |
| **Docker / Docker Compose** | Containerization and Environment Setup |
| **PostgreSQL 13** | Database for Airflow Metadata and Project Data |
| **Python 3.11** | ETL Logic and Final Report Generation |
| **Pandas** | Data manipulation in Python ETL |
| **SQL** | Analytical Querying for Report Generation |

---

## 🚀 Getting Started

### Prerequisites

You must have **Docker** and **Docker Compose** installed on your system.

### Setup and Run

1.  **Clone the repository and navigate to the directory:**
    ```bash
    git clone [YOUR_REPO_URL]
    cd quickshop-etl-pipeline
    ```

2.  **Build and Start Containers:**
    The `--build` flag is critical to ensure the custom Airflow image contains all necessary Python dependencies (`pandas`, `psycopg2`, etc.).

    ```bash
    docker-compose up -d --build
    ```

3.  **Access Airflow UI:**
    Open your browser and navigate to: **`http://localhost:8080`**

---

## 🎯 Usage

To run the pipeline, you **must** trigger it with an execution date that corresponds to one of the data files in the `./data` folder (e.g., `orders_20251025.csv`).

1.  **Unpause the DAG:** In the Airflow UI, find `quickshop_daily_pipeline` and toggle the switch to **'On'**.
2.  **Trigger the Run:** Click the **Trigger DAG** button (the play icon).
3.  **Set Execution Date:** Set the **Data Interval Start** to **`2025-10-25`**.
4.  **Monitor:** Use the **Grid View** until all 9 tasks turn **Green**.

### Check the Final Output

Once the DAG is successful, the final report is saved to your local machine:

```bash
cat reports/2025-10-25_summary.json
