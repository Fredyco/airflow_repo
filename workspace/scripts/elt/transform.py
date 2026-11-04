'''
T: menjalankan file SQL di dalam database (raw -> staging -> mart).
Contoh: python transform.py 01_staging.sql
'''
import sys

from db import get_engine

SQL_DIR = "/workspace/scripts/elt/sql"

if __name__ == "__main__":
    filename = sys.argv[1]
    with open(f"{SQL_DIR}/{filename}") as f:
        sql = f.read()
    with get_engine().begin() as conn:      # semua statement dalam satu transaksi
        conn.exec_driver_sql(sql)
    print(f"Selesai menjalankan {filename}")
