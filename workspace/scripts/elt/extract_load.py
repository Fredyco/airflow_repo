'''
E + L: baca CSV apa adanya (semua kolom teks) dan muat ke schema raw tanpa dibersihkan.
'''
import pandas as pd

from db import get_engine

DATA_DIR = "/workspace/data"

if __name__ == "__main__":
    engine = get_engine()
    with engine.begin() as conn:
        conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS raw")

    for table, file in [("sales", "demo_sales.csv"), ("customers", "demo_customers.csv")]:
        df = pd.read_csv(f"{DATA_DIR}/{file}", dtype=str)       # tanpa konversi tipe
        df["_loaded_at"] = pd.Timestamp.now()                   # metadata waktu load
        df.to_sql(table, engine, schema="raw", if_exists="replace", index=False)
        print(f"raw.{table}: {len(df)} baris")
