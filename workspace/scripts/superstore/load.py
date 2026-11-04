'''
Load: isi tabel star schema di PostgreSQL (notebook 02, bagian 3).
Tabel dibuat dulu dengan workspace/sql/02_create_tables.sql di pgAdmin.
Supaya DAG bisa dijalankan berulang, tabel dikosongkan (TRUNCATE) sebelum diisi.
'''
import os

import psycopg2
from pyspark.sql import SparkSession

INPUT = "/workspace/data/superstore/star"
TABLES = ['dim_customer', 'dim_product', 'dim_location', 'dim_date', 'fact_sales']  # dimension dulu, baru fact

env = os.environ
jdbc_url = f"jdbc:postgresql://{env['PG_HOST']}:{env['PG_PORT']}/{env['PG_DB']}"
jdbc_props = {'user': env['PG_USER'], 'password': env['PG_PASSWORD'], 'driver': 'org.postgresql.Driver'}

# Kosongkan tabel (fact ikut terhapus lewat CASCADE)
conn = psycopg2.connect(host=env['PG_HOST'], port=env['PG_PORT'], dbname=env['PG_DB'],
                        user=env['PG_USER'], password=env['PG_PASSWORD'], connect_timeout=5)
with conn, conn.cursor() as cur:
    cur.execute(f"TRUNCATE {', '.join(TABLES)} CASCADE")
conn.close()
print('Tabel dikosongkan')

spark = SparkSession.builder.appName("superstore-load").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# mode='append' supaya primary key & foreign key dari 02_create_tables.sql tetap ada
for name in TABLES:
    spark.read.parquet(f'{INPUT}/{name}').write.jdbc(jdbc_url, name, mode='append', properties=jdbc_props)
    print('masuk:', name)

# Cek hasil (notebook 02, bagian 4)
query = '''
(SELECT l.region, p.category, ROUND(SUM(f.sales), 2)::float AS total_sales
 FROM fact_sales f
 JOIN dim_location l ON f.location_id = l.location_id
 JOIN dim_product  p ON f.product_id  = p.product_id
 GROUP BY l.region, p.category
 ORDER BY l.region, p.category) AS t
'''
spark.read.jdbc(jdbc_url, query, properties=jdbc_props).show()

spark.stop()
