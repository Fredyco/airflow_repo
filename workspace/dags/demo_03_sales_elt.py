'''
Demo 03 - Pipeline ELT (Extract -> Load -> Transform) yang menjalankan script Python terpisah.

    extract_load_raw  >>  transform_staging  >>  transform_mart  >>  quality_check

Perbedaan dengan ETL (demo_02):
- ETL : data ditransformasi DULU (di Python/Spark), baru dimuat ke database.
- ELT : data mentah dimuat APA ADANYA ke database (schema raw), lalu ditransformasi
        DI DALAM database dengan SQL (schema staging -> mart).

Sumber data : workspace/data/demo_sales.csv & demo_customers.csv
Tujuan      : PostgreSQL di laptop (setting PG_* di file .env), schema raw, staging, mart
Script      : workspace/scripts/elt/ (SQL di workspace/scripts/elt/sql/)
'''
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

SCRIPTS = "/workspace/scripts/elt"

with DAG(
    dag_id="demo_03_sales_elt",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    extract_load_raw = BashOperator(
        task_id="extract_load_raw",
        bash_command=f"python {SCRIPTS}/extract_load.py",
    )

    transform_staging = BashOperator(
        task_id="transform_staging",
        bash_command=f"python {SCRIPTS}/transform.py 01_staging.sql",
    )

    transform_mart = BashOperator(
        task_id="transform_mart",
        bash_command=f"python {SCRIPTS}/transform.py 02_mart.sql",
    )

    quality_check = BashOperator(
        task_id="quality_check",
        bash_command=f"python {SCRIPTS}/quality_check.py",
    )

    extract_load_raw >> transform_staging >> transform_mart >> quality_check
