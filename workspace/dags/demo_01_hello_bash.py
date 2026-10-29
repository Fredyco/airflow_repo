'''
Demo 01 - DAG paling sederhana: dua BashOperator berurutan.

Konsep yang dipelajari:
- Membuat DAG dengan context manager `with DAG(...)`
- schedule (cron), start_date, catchup
- Dependency antar task dengan operator `>>`
'''
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="demo_01_hello_bash",
    description="Dua task Bash yang berjalan berurutan",
    start_date=datetime(2025, 1, 1),  # zona waktu default container: Asia/Jakarta
    schedule="*/5 * * * *",           # setiap 5 menit
    catchup=False,                    # tidak menjalankan jadwal yang terlewat
    tags=["demo", "basic"],
) as dag:

    task_1 = BashOperator(
        task_id="task_1",
        bash_command="echo 'Hello from task 1' && date",
    )

    task_2 = BashOperator(
        task_id="task_2",
        bash_command="echo 'Hello from task 2' && whoami",
    )

    task_1 >> task_2
