'''
Demo 05 - Versi Airflow 3 dari contoh di slide "Workflow Orchestration: Airflow"
(AirflowCSV.py -> H8CSVDAG dan AirflowClean.py -> H8CleanData).

Kode di slide memakai sintaks Airflow 2 yang SUDAH DIHAPUS di Airflow 3:

    Airflow 2 (slide)                                          Airflow 3 (image ini)
    from airflow import DAG                                    from airflow.sdk import DAG
    from airflow.operators.bash_operator import BashOperator   from airflow.providers.standard.operators.bash import BashOperator
    from airflow.operators.python_operator import ...          from airflow.providers.standard.operators.python import PythonOperator
    schedule_interval=timedelta(minutes=5)                     schedule=timedelta(minutes=5)
    path /opt/airflow/dags/...                                 path /workspace/...

Dataset scooter.csv di slide diganti workspace/data/demo_sales.csv.
'''
import datetime as dt
from datetime import timedelta

import pandas as pd
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

DATA = "/workspace/data"
OUT = "/workspace/data/output"


def csvToJson():
    '''Seperti slide: baca CSV, print sebagian isi, simpan sebagai JSON.'''
    df = pd.read_csv(f"{DATA}/demo_customers.csv")
    for i, r in df.head(5).iterrows():
        print(r["CustomerName"])
    df.to_json(f"{OUT}/fromAirflow.json", orient="records")


def cleanSales():
    '''Seperti cleanScooter(): drop kolom, nama kolom jadi lowercase, ubah tipe tanggal, buang data kosong.'''
    df = pd.read_csv(f"{DATA}/demo_sales.csv")
    df.drop(columns=["PaymentMethod"], inplace=True)
    df.columns = [x.lower() for x in df.columns]
    df["orderdate"] = pd.to_datetime(df["orderdate"], format="%Y-%m-%d")
    df.dropna(inplace=True)
    df.drop_duplicates(subset="orderid", inplace=True)
    df.to_csv(f"{OUT}/cleansales.csv", index=False)


def filterData():
    '''Seperti filterData(): ambil transaksi pada rentang tanggal tertentu.'''
    df = pd.read_csv(f"{OUT}/cleansales.csv")
    fromd, tod = "2024-05-23", "2024-06-03"
    tofrom = df[(df["orderdate"] > fromd) & (df["orderdate"] < tod)]
    tofrom.to_csv(f"{OUT}/may23-june3.csv", index=False)
    print(f"{len(tofrom)} transaksi antara {fromd} dan {tod}")


default_args = {
    "owner": "raka",
    "start_date": dt.datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

with DAG(
    "demo_05_csv_clean_slide",
    default_args=default_args,
    schedule=timedelta(minutes=60),      # Airflow 2: schedule_interval=
    catchup=False,
    tags=["demo", "slide"],
) as dag:

    print_starting = BashOperator(
        task_id="starting",
        bash_command=f'mkdir -p {OUT} && echo "I am reading the CSV now....."',
    )

    csvJson = PythonOperator(task_id="convertCSVtoJson", python_callable=csvToJson)

    cleanData = PythonOperator(task_id="clean", python_callable=cleanSales)

    selectData = PythonOperator(task_id="filter", python_callable=filterData)

    copyFile = BashOperator(
        task_id="copy",
        bash_command=f"cp {OUT}/may23-june3.csv {OUT}/may23-june3_copy.csv && ls -l {OUT}",
    )

    # Sama dengan: print_starting.set_downstream(csvJson)  atau  csvJson << print_starting
    print_starting >> csvJson
    print_starting >> cleanData >> selectData >> copyFile
