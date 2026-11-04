'''
T: transformasi DI DALAM database dengan SQL: raw_sales -> clean_sales.
'''
import os

from sqlalchemy import create_engine

SQL = '''
DROP TABLE IF EXISTS clean_sales;
CREATE TABLE clean_sales AS
SELECT DISTINCT
       "OrderID"                    AS order_id,
       "OrderDate"::date            AS order_date,
       INITCAP(TRIM("Product"))     AS product,
       "Quantity"                   AS quantity,
       "UnitPrice"                  AS unit_price,
       "Quantity" * "UnitPrice"     AS total_price
FROM raw_sales
WHERE "Quantity" IS NOT NULL;
'''


def get_engine():
    '''Koneksi ke PostgreSQL (setting dari .env).'''
    return create_engine(f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PASSWORD']}"
                         f"@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DB']}")


def transform(engine):
    '''Jalankan SQL di database: raw_sales -> clean_sales.'''
    with engine.begin() as conn:
        conn.exec_driver_sql(SQL)
    print("clean_sales selesai dibuat")


if __name__ == "__main__":
    transform(get_engine())
