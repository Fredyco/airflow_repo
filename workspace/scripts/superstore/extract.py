'''
Extract: baca 3 file CSV superstore (sama dengan notebook 02, bagian 1),
lalu simpan apa adanya sebagai parquet supaya bisa dibaca task berikutnya.
'''
from pyspark.sql import SparkSession

OUTPUT = "/workspace/data/superstore/raw"


def read_csv(spark, file_name):
    return spark.read.csv(f'/workspace/data/{file_name}', header=True, inferSchema=True, escape='"')


# Parquet tidak boleh punya spasi di nama kolom -> langsung ubah ke snake_case (Step 1 notebook)
def to_snake_case(df):
    new_names = [c.lower().replace(' ', '_').replace('-', '_').replace('/', '_') for c in df.columns]
    return df.toDF(*new_names)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("superstore-extract").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    orders_raw = read_csv(spark, 'superstore_orders.csv')
    customers_raw = read_csv(spark, 'superstore_customers.csv')
    products_raw = read_csv(spark, 'superstore_products.csv')

    print('orders:', orders_raw.count(), '| customers:', customers_raw.count(), '| products:', products_raw.count())

    for name, df in [('orders', orders_raw), ('customers', customers_raw), ('products', products_raw)]:
        to_snake_case(df).write.mode('overwrite').parquet(f'{OUTPUT}/{name}')
        print('disimpan:', f'{OUTPUT}/{name}')

    spark.stop()
