'''
Transform: bersihkan, join, buat dimension & fact.
Kode disalin dari notebook 02_etl_superstore (bagian 2, Step 3 - Step 6).
'''
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

INPUT = "/workspace/data/superstore/raw"
OUTPUT = "/workspace/data/superstore/star"
LOCATION_COLS = ['country_region', 'region', 'state', 'city', 'postal_code']


def read_raw(spark):
    '''Baca hasil extract.'''
    return (spark.read.parquet(f'{INPUT}/orders'),
            spark.read.parquet(f'{INPUT}/customers'),
            spark.read.parquet(f'{INPUT}/products'))


def clean(orders, customers, products):
    '''Step 3 & 4: bersihkan setiap tabel, simpan hanya order yang produknya dikenal.'''
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

    orders_valid = orders_clean.join(products_clean[['product_id']], on='product_id', how='inner')

    print('customers:', customers.count(), '->', customers_clean.count())
    print('products :', products.count(), '->', products_clean.count())
    print('orders   :', orders.count(), '->', orders_valid.count())
    return orders_valid, customers_clean, products_clean


def build_dimensions(orders_valid, customers_clean, products_clean):
    '''Step 5: tabel dimension.'''
    dim_customer = customers_clean[['customer_id', 'customer_name', 'segment']]
    dim_product = products_clean[['product_id', 'product_name', 'category', 'sub_category']]

    dim_location = (orders_valid[LOCATION_COLS].drop_duplicates()
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

    return dim_customer, dim_product, dim_location, dim_date


def build_fact(orders_valid, dim_location):
    '''Step 6: tabel fact.'''
    fact_sales = (orders_valid
        .join(dim_location, on=LOCATION_COLS, how='left')
        .withColumn('order_date_id', F.date_format('order_date', 'yyyyMMdd').cast('int'))
        .withColumn('ship_date_id', F.date_format('ship_date', 'yyyyMMdd').cast('int'))
        .select('row_id', 'order_id', 'order_date_id', 'ship_date_id', 'customer_id', 'product_id',
                'location_id', 'ship_mode', 'sales', 'quantity', 'discount', 'profit'))

    if fact_sales[fact_sales['location_id'].isNull()].count() > 0:
        raise ValueError('Ada location_id kosong di fact_sales')
    return fact_sales


def save(tables):
    '''Simpan hasil untuk task load.'''
    for name, table in tables.items():
        table.write.mode('overwrite').parquet(f'{OUTPUT}/{name}')
        print(name, table.count())


if __name__ == "__main__":
    spark = SparkSession.builder.appName("superstore-transform").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    orders, customers, products = read_raw(spark)
    orders_valid, customers_clean, products_clean = clean(orders, customers, products)
    dim_customer, dim_product, dim_location, dim_date = build_dimensions(orders_valid, customers_clean, products_clean)
    fact_sales = build_fact(orders_valid, dim_location)

    save({'dim_customer': dim_customer, 'dim_product': dim_product, 'dim_location': dim_location,
          'dim_date': dim_date, 'fact_sales': fact_sales})

    spark.stop()
