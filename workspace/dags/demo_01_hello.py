'''
Demo 01 - DAG sederhana: Bash -> Python -> Bash.

Konsep:
- Membuat DAG: dag_id, start_date, schedule, catchup
- BashOperator (perintah terminal) & PythonOperator (fungsi Python)
- Urutan task dengan >>
'''
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def hitung_transaksi():
    '''Membaca CSV dan menampilkan jumlah baris.'''
    import pandas as pd

    df = pd.read_csv("/workspace/data/demo_sales.csv")
    print(f"Jumlah transaksi: {len(df)}")


with DAG(
    dag_id="demo_01_hello",
    start_date=datetime(2025, 1, 1),
    schedule="0 7 * * *",
    catchup=False,
) as dag:

    mulai = BashOperator(task_id="mulai", bash_command="echo 'Pipeline mulai'")

    hitung = PythonOperator(task_id="hitung_transaksi", python_callable=hitung_transaksi)

    selesai = BashOperator(task_id="selesai", bash_command="echo 'Pipeline selesai'")

    mulai >> hitung >> selesai
