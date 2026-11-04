'''
Transform: bersihkan, join, buat dimension & fact.
Kode disalin dari notebook 02_etl_superstore (bagian 2, Step 3 - Step 6).
'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

INPUT = "/workspace/data/superstore/raw"
OUTPUT = "/workspace/data/superstore/star"

spark = SparkSession.builder.appName("superstore-transform").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

orders = spark.read.parquet(f'{INPUT}/orders')
customers = spark.read.parquet(f'{INPUT}/customers')
products = spark.read.parquet(f'{INPUT}/products')

# Step 3. Bersihkan setiap tabel
customers_clean = (customers
    .drop_duplicates()
    .withColumn('customer_name', F.trim('customer_name'))
    .withColumn('segment', F.initcap(F.trim('segment')))
    .drop_duplicates(subset=['customer_id']))

products_clean = (products
    .drop_duplicates()
    .withColumn('category', F.initcap(F.trim('category')))
    .drop_duplicates(subset=['product_id']))

orders_clean = (orders
    .drop_duplicates()
    .withColumn('region', F.initcap(F.trim('region')))
    .withColumn('postal_code', F.lpad(F.col('postal_code').astype('string'), 5, '0'))
    .fillna({'postal_code': 'Unknown'})
    .withColumn('order_date', F.to_date('order_date', 'M/d/yyyy'))
    .withColumn('ship_date', F.to_date('ship_date', 'M/d/yyyy'))
    .dropna(subset=['sales']))

print('customers:', customers.count(), '->', customers_clean.count())
print('products :', products.count(), '->', products_clean.count())
print('orders   :', orders.count(), '->', orders_clean.count())

# Step 4. Hanya simpan order yang produknya dikenal (inner join)
orders_valid = orders_clean.join(products_clean[['product_id']], on='product_id', how='inner')
print('orders valid:', orders_clean.count(), '->', orders_valid.count())

# Step 5. Tabel dimension
dim_customer = customers_clean[['customer_id', 'customer_name', 'segment']]
dim_product = products_clean[['product_id', 'product_name', 'category', 'sub_category']]

location_cols = ['country_region', 'region', 'state', 'city', 'postal_code']

dim_location = (orders_valid[location_cols].drop_duplicates()
                .withColumn('location_id', F.row_number().over(Window.orderBy('state', 'city', 'postal_code'))))

dates = (orders_valid.select(F.col('order_date').alias('full_date'))
         .union(orders_valid.select(F.col('ship_date').alias('full_date')))
         .distinct())

dim_date = (dates
    .withColumn('date_id', F.date_format('full_date', 'yyyyMMdd').cast('int'))
    .withColumn('year', F.year('full_date'))
    .withColumn('quarter', F.quarter('full_date'))
    .withColumn('month', F.month('full_date'))
    .withColumn('month_name', F.date_format('full_date', 'MMMM'))
    .withColumn('day_name', F.date_format('full_date', 'EEEE')))

# Step 6. Tabel fact
fact_sales = (orders_valid
    .join(dim_location, on=location_cols, how='left')
    .withColumn('order_date_id', F.date_format('order_date', 'yyyyMMdd').cast('int'))
    .withColumn('ship_date_id', F.date_format('ship_date', 'yyyyMMdd').cast('int'))
    .select('row_id', 'order_id', 'order_date_id', 'ship_date_id', 'customer_id', 'product_id',
            'location_id', 'ship_mode', 'sales', 'quantity', 'discount', 'profit'))

# Cek: tidak ada location_id yang kosong
if fact_sales[fact_sales['location_id'].isNull()].count() > 0:
    raise ValueError('Ada location_id kosong di fact_sales')

# Simpan hasil untuk task load
tables = [('dim_customer', dim_customer), ('dim_product', dim_product), ('dim_location', dim_location),
          ('dim_date', dim_date), ('fact_sales', fact_sales)]

for name, table in tables:
    table.write.mode('overwrite').parquet(f'{OUTPUT}/{name}')
    print(name, table.count())

spark.stop()
