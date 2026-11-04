'''
Validasi sederhana: tabel tidak kosong & tidak ada duplikat order_id.
Gagal -> script error -> task merah di Airflow.
'''
from db import get_engine

CHECKS = {
    "stg_sales tidak kosong": "SELECT COUNT(*) > 0 FROM staging.stg_sales",
    "order_id unik": "SELECT COUNT(*) = COUNT(DISTINCT order_id) FROM staging.stg_sales",
    "mart tidak kosong": "SELECT COUNT(*) > 0 FROM mart.sales_by_city_month",
}

if __name__ == "__main__":
    with get_engine().connect() as conn:
        for name, sql in CHECKS.items():
            ok = conn.exec_driver_sql(sql).scalar()
            print(f"[{'OK' if ok else 'GAGAL'}] {name}")
            if not ok:
                raise ValueError(f"Quality check gagal: {name}")
        raw = conn.exec_driver_sql("SELECT COUNT(*) FROM raw.sales").scalar()
        staged = conn.exec_driver_sql("SELECT COUNT(*) FROM staging.stg_sales").scalar()
    print(f"raw.sales {raw} baris -> staging.stg_sales {staged} baris (setelah cleaning)")
