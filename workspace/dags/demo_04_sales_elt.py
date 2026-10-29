'''
Demo 04 - Pipeline ELT (Extract -> Load -> Transform) dengan TaskFlow API.

    extract_load_raw  >>  transform_staging  >>  transform_mart  >>  quality_check

Perbedaan dengan ETL (demo_03):
- ETL : data ditransformasi DULU (di Python/Spark), baru dimuat ke database.
- ELT : data mentah dimuat APA ADANYA ke database (schema raw), lalu ditransformasi
        DI DALAM database dengan SQL (schema staging -> mart).

Sumber data : workspace/data/demo_sales.csv & demo_customers.csv
Tujuan      : PostgreSQL database dwh  (schema raw, staging, mart)
SQL         : workspace/scripts/elt/sql/
'''
import os
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task

DATA_DIR = Path("/workspace/data")
SQL_DIR = Path("/workspace/scripts/elt/sql")


def get_engine():
    '''Koneksi SQLAlchemy ke database dwh (kredensial dari environment container).'''
    from sqlalchemy import create_engine

    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "postgres")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:5432/dwh")


def run_sql_file(filename):
    '''Menjalankan seluruh statement di file .sql dalam satu transaksi.'''
    sql = (SQL_DIR / filename).read_text()
    with get_engine().begin() as conn:
        conn.exec_driver_sql(sql)
    print(f"Selesai menjalankan {filename}")


@dag(
    dag_id="demo_04_sales_elt",
    description="ELT: CSV mentah -> PostgreSQL raw -> SQL staging -> SQL mart",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["demo", "elt", "sql"],
)
def sales_elt():

    @task
    def extract_load_raw():
        '''E + L: baca CSV apa adanya (semua kolom teks) dan muat ke schema raw tanpa dibersihkan.'''
        import pandas as pd

        engine = get_engine()
        with engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS raw")

        counts = {}
        for table, file in [("sales", "demo_sales.csv"), ("customers", "demo_customers.csv")]:
            df = pd.read_csv(DATA_DIR / file, dtype=str)            # tanpa konversi tipe
            df["_loaded_at"] = pd.Timestamp.now()                   # metadata waktu load
            df.to_sql(table, engine, schema="raw", if_exists="replace", index=False)
            counts[table] = len(df)
            print(f"raw.{table}: {len(df)} baris")
        return counts

    @task
    def transform_staging():
        '''T (1): bersihkan & cast tipe data di dalam database -> schema staging.'''
        run_sql_file("01_staging.sql")

    @task
    def transform_mart():
        '''T (2): agregasi siap analisis -> schema mart.'''
        run_sql_file("02_mart.sql")

    @task
    def quality_check(raw_counts):
        '''Validasi sederhana: tabel tidak kosong & tidak ada duplikat order_id. Gagal -> task merah.'''
        checks = {
            "stg_sales tidak kosong": "SELECT COUNT(*) > 0 FROM staging.stg_sales",
            "order_id unik": "SELECT COUNT(*) = COUNT(DISTINCT order_id) FROM staging.stg_sales",
            "mart tidak kosong": "SELECT COUNT(*) > 0 FROM mart.sales_by_city_month",
        }
        with get_engine().connect() as conn:
            for name, sql in checks.items():
                ok = conn.exec_driver_sql(sql).scalar()
                print(f"[{'OK' if ok else 'GAGAL'}] {name}")
                if not ok:
                    raise ValueError(f"Quality check gagal: {name}")
            staged = conn.exec_driver_sql("SELECT COUNT(*) FROM staging.stg_sales").scalar()
        print(f"raw.sales {raw_counts['sales']} baris -> staging.stg_sales {staged} baris (setelah cleaning)")

    raw_counts = extract_load_raw()
    raw_counts >> transform_staging() >> transform_mart() >> quality_check(raw_counts)


sales_elt()
