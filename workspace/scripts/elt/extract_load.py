'''
E + L: baca CSV apa adanya, langsung masukkan ke database (tabel raw_sales). Belum dibersihkan.
'''
import os

import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}"
                       f"@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DB']}")

df = pd.read_csv("/workspace/data/demo_sales.csv")
df.to_sql("raw_sales", engine, if_exists="replace", index=False)
print(f"raw_sales: {len(df)} baris")
