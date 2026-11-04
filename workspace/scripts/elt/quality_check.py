'''
Cek hasil: tabel clean_sales tidak boleh kosong. Kalau kosong -> error -> task merah di Airflow.
'''
import os

from sqlalchemy import create_engine


def get_engine():
    '''Koneksi ke PostgreSQL (setting dari .env).'''
    return create_engine(f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}"
                         f"@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DB']}")


def count_rows(engine, table):
    '''Hitung jumlah baris sebuah tabel.'''
    with engine.connect() as conn:
        return conn.exec_driver_sql(f"SELECT COUNT(*) FROM {table}").scalar()


def quality_check(engine):
    '''Bandingkan raw vs clean, error kalau clean_sales kosong.'''
    raw = count_rows(engine, "raw_sales")
    clean = count_rows(engine, "clean_sales")
    print(f"raw_sales {raw} baris -> clean_sales {clean} baris")
    if clean == 0:
        raise ValueError("clean_sales kosong!")


if __name__ == "__main__":
    quality_check(get_engine())
