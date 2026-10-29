'''
Load: menyimpan hasil transform ke PostgreSQL lokal (database dwh, tabel crypto_price).
Kredensial diambil dari environment variable container (lihat docker-compose.yml / .env).
'''
import os

import pandas as pd
from sqlalchemy import create_engine

INPUT = "/workspace/data/crypto/transform_result.csv"


def get_engine():
    '''Membuat koneksi SQLAlchemy ke database dwh.'''
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "postgres")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:5432/dwh")


if __name__ == "__main__":
    df = pd.read_csv(INPUT, parse_dates=["processed_at"])
    df["loaded_at"] = pd.Timestamp.now(tz="Asia/Jakarta").tz_localize(None)
    df.to_sql("crypto_price", con=get_engine(), if_exists="append", index=False)
    print(f"{len(df)} baris ditambahkan ke dwh.crypto_price")
