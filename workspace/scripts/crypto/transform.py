'''
Transform: membaca hasil extract dengan PySpark, menambahkan kurs USD->IDR
dari API Frankfurter, harga dalam IDR, dan waktu proses.
'''
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT = "/workspace/data/crypto/extract_result.csv"
OUTPUT = "/workspace/data/crypto/transform_result.csv"


def get_usd_idr_rate():
    '''Mengambil kurs 1 USD dalam IDR dari API Frankfurter.'''
    response = requests.get("https://api.frankfurter.dev/v1/latest", params={"base": "USD", "symbols": "IDR"}, timeout=30)
    response.raise_for_status()
    return response.json()["rates"]["IDR"]


def transform(df, rate):
    '''
    Menambah kolom hasil transformasi.

    Parameters:
        df: Spark DataFrame hasil extract
        rate: float - kurs USD ke IDR

    Return:
        Spark DataFrame dengan kolom usd_idr_rate, coin_price_idr, processed_at
    '''
    return (
        df.withColumn("coin_name", F.initcap("coin_name"))
          .withColumn("usd_idr_rate", F.lit(rate))
          .withColumn("coin_price_idr", F.round(F.col("coin_price") * F.lit(rate), 2))
          .withColumn("processed_at", F.current_timestamp())
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("CryptoTransform").getOrCreate()
    rate = get_usd_idr_rate()
    print(f"Kurs USD -> IDR: {rate}")

    df = spark.read.csv(INPUT, header=True, inferSchema=True)
    result = transform(df, rate)
    result.show(truncate=False)

    # Data kecil -> simpan sebagai 1 file CSV lewat pandas
    result.toPandas().to_csv(OUTPUT, index=False)
    print(f"Tersimpan: {OUTPUT}")
    spark.stop()
