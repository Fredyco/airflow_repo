'''
Demo 03 - Pipeline ELT: Extract -> Load -> Transform.

    extract_load  >>  transform  >>  quality_check

- ETL : data dibersihkan DULU (Python/Spark), baru dimasukkan ke database.
- ELT : data mentah dimasukkan DULU ke database (raw_sales), lalu dibersihkan DI DALAM database dengan SQL (clean_sales).

Script: workspace/scripts/elt/
'''
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="demo_03_sales_elt",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    extract_load = BashOperator(
        task_id="extract_load",
        bash_command="python /workspace/scripts/elt/extract_load.py",
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="python /workspace/scripts/elt/transform.py",
    )

    quality_check = BashOperator(
        task_id="quality_check",
        bash_command="python /workspace/scripts/elt/quality_check.py",
    )

    extract_load >> transform >> quality_check
