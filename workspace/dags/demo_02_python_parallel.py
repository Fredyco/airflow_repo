'''
Demo 02 - Kombinasi BashOperator & PythonOperator dengan task paralel.

Alur:  start_bash  >>  [python_1, python_2]  >>  end_bash
python_1 dan python_2 berjalan bersamaan setelah start_bash selesai.

Konsep yang dipelajari:
- PythonOperator memanggil fungsi Python
- Fan-out / fan-in (task paralel)
- Mengirim nilai antar task lewat XCom (return value)
'''
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def hitung_kuadrat():
    '''Menghitung kuadrat 1..5; nilai return otomatis disimpan sebagai XCom.'''
    hasil = [i ** 2 for i in range(1, 6)]
    print(f"Kuadrat: {hasil}")
    return hasil


def sapa(nama):
    '''Contoh PythonOperator dengan argumen.'''
    print(f"Halo {nama}, ini python task 2")


def ringkas(ti):
    '''Mengambil XCom dari python_1 lewat TaskInstance (ti).'''
    kuadrat = ti.xcom_pull(task_ids="python_1")
    print(f"Total kuadrat dari python_1 = {sum(kuadrat)}")


with DAG(
    dag_id="demo_02_python_parallel",
    description="Bash + Python dengan task paralel dan XCom",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["demo", "basic"],
) as dag:

    start_bash = BashOperator(task_id="start_bash", bash_command="echo Start task")

    python_1 = PythonOperator(task_id="python_1", python_callable=hitung_kuadrat)

    python_2 = PythonOperator(task_id="python_2", python_callable=sapa, op_kwargs={"nama": "Data Engineer"})

    summary = PythonOperator(task_id="summary", python_callable=ringkas)

    end_bash = BashOperator(task_id="end_bash", bash_command="echo Finish task")

    start_bash >> [python_1, python_2] >> summary >> end_bash
