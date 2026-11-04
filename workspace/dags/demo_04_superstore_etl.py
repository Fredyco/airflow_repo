'''
Demo 04 - ETL Superstore -> star schema dengan Spark (isi notebook 02_etl_superstore dijadikan script).

    extract  >>  transform  >>  load

- extract   : baca 3 CSV superstore                      -> data/superstore/raw/*.parquet
- transform : Spark, kode disalin dari notebook 02        -> data/superstore/star/*.parquet
- load      : Spark JDBC ke PostgreSQL (database dwh)     -> dim_* & fact_sales

Sebelum run pertama, buat database & tabel di pgAdmin: workspace/sql/01_create_database.sql
dan 02_create_tables.sql. Setiap run, load mengosongkan tabel dulu lalu mengisi ulang.
Cek hasil: workspace/sql/03_check.sql
'''
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

SCRIPTS = "/workspace/scripts/superstore"

with DAG(
    dag_id="demo_04_superstore_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,                    # hindari 2 Spark job bersamaan di laptop
) as dag:

    extract = BashOperator(task_id="extract", bash_command=f"python {SCRIPTS}/extract.py")
    transform = BashOperator(task_id="transform", bash_command=f"python {SCRIPTS}/transform.py")
    load = BashOperator(task_id="load", bash_command=f"python {SCRIPTS}/load.py")

    extract >> transform >> load
