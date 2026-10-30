'''
Demo 02 - Pipeline ETL harga crypto yang menjalankan script Python terpisah.

    extract_prices  >>  transform_with_spark  >>  load_to_postgres

- extract   : ambil harga crypto (USD) dari API CoinGecko          -> data/crypto/extract_result.csv
- transform : PySpark + kurs USD/IDR dari API Frankfurter          -> data/crypto/transform_result.csv
- load      : simpan ke PostgreSQL di laptop (setting PG_* di file .env), tabel crypto_price

Script ada di folder workspace/scripts/crypto/. Pola ini sama dengan tugas M3
(extract.py / transform.py / load.py dipanggil dari DAG), hanya tujuan load-nya PostgreSQL.
Cek hasil di pgAdmin: `SELECT * FROM crypto_price ORDER BY loaded_at DESC;`
'''
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

SCRIPTS = "/workspace/scripts/crypto"

default_args = {
    "owner": "instruktur",
    "retries": 2,                         # API publik kadang gagal / rate limit
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="demo_02_crypto_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",                 # setiap jam (menit ke-0)
    catchup=False,
    max_active_runs=1,                    # hindari 2 Spark job bersamaan di laptop
) as dag:

    extract_prices = BashOperator(
        task_id="extract_prices",
        bash_command=f"python {SCRIPTS}/extract.py",
    )

    transform_with_spark = BashOperator(
        task_id="transform_with_spark",
        bash_command=f"python {SCRIPTS}/transform.py",
    )

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"python {SCRIPTS}/load.py",
    )

    extract_prices >> transform_with_spark >> load_to_postgres
