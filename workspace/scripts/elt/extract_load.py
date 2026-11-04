'''
E + L: baca CSV apa adanya, langsung masukkan ke database (tabel raw_sales). Belum dibersihkan.
'''
import os

import pandas as pd
from sqlalchemy import create_engine

INPUT = "/workspace/data/demo_sales.csv"
TABLE = "raw_sales"


def get_engine():
    '''Koneksi ke PostgreSQL (setting dari .env).'''
    return create_engine(f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}"
                         f"@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DB']}")


def extract():
    '''Baca CSV tanpa dibersihkan.'''
    return pd.read_csv(INPUT)


def load(df, engine):
    '''Masukkan DataFrame apa adanya ke tabel raw_sales.'''
    df.to_sql(TABLE, engine, if_exists="replace", index=False)
    print(f"{TABLE}: {len(df)} baris")


if __name__ == "__main__":
    df = extract()
    load(df, get_engine())
