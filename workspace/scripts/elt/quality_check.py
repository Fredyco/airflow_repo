'''
Cek hasil: tabel clean_sales tidak boleh kosong. Kalau kosong -> error -> task merah di Airflow.
'''
import os

from sqlalchemy import create_engine


if __name__ == "__main__":
    engine = create_engine(f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}"
                           f"@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DB']}")

    with engine.connect() as conn:
        raw = conn.exec_driver_sql("SELECT COUNT(*) FROM raw_sales").scalar()
        clean = conn.exec_driver_sql("SELECT COUNT(*) FROM clean_sales").scalar()

    print(f"raw_sales {raw} baris -> clean_sales {clean} baris")
    if clean == 0:
        raise ValueError("clean_sales kosong!")
