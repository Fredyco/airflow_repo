'''
Load: menyimpan hasil transform ke PostgreSQL di laptop (tabel crypto_price).
Host, user, dan password diambil dari file .env (PG_*), tidak ditulis di kode.
'''
import os

import pandas as pd
from sqlalchemy import create_engine

INPUT = "/workspace/data/crypto/transform_result.csv"


def get_engine():
    '''Membuat koneksi SQLAlchemy ke PostgreSQL (setting dari .env).'''
    env = os.environ
    return create_engine(
        f"postgresql+psycopg2://{env['PG_USER']}:{env['PG_PASSWORD']}@{env['PG_HOST']}:{env['PG_PORT']}/{env['PG_DB']}"
    )


def load(df, engine):
    '''Tambahkan waktu load, lalu append ke tabel crypto_price.'''
    df["loaded_at"] = pd.Timestamp.now(tz="Asia/Jakarta").tz_localize(None)
    df.to_sql("crypto_price", con=engine, if_exists="append", index=False)
    print(f"{len(df)} baris ditambahkan ke tabel crypto_price")


if __name__ == "__main__":
    df = pd.read_csv(INPUT, parse_dates=["processed_at"])
    load(df, get_engine())
