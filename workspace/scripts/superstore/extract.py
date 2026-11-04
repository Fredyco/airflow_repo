'''
Extract: baca 3 file CSV superstore (sama dengan notebook 02, bagian 1),
lalu simpan sebagai parquet supaya bisa dibaca task berikutnya.
'''
from pyspark.sql import SparkSession

OUTPUT = "/workspace/data/superstore/raw"
FILES = {'orders': 'superstore_orders.csv',
         'customers': 'superstore_customers.csv',
         'products': 'superstore_products.csv'}


def read_csv(spark, file_name):
    return spark.read.csv(f'/workspace/data/{file_name}', header=True, inferSchema=True, escape='"')


def to_snake_case(df):
    '''Parquet tidak boleh punya spasi di nama kolom -> ubah ke snake_case (Step 1 notebook).'''
    new_names = [c.lower().replace(' ', '_').replace('-', '_').replace('/', '_') for c in df.columns]
    return df.toDF(*new_names)


def extract(spark):
    '''Baca semua CSV, return dict {nama: DataFrame}.'''
    data = {name: read_csv(spark, file) for name, file in FILES.items()}
    for name, df in data.items():
        print(name, df.count())
    return data


def save(data):
    '''Simpan setiap DataFrame ke parquet.'''
    for name, df in data.items():
        to_snake_case(df).write.mode('overwrite').parquet(f'{OUTPUT}/{name}')
        print('disimpan:', f'{OUTPUT}/{name}')


if __name__ == "__main__":
    spark = SparkSession.builder.appName("superstore-extract").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    data = extract(spark)
    save(data)

    spark.stop()
