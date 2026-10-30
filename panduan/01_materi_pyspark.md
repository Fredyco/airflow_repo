# Materi PySpark — Step by Step

> Batch Processing using Spark • CODA Phase 2
> Kunci jawaban lengkap: `workspace/notebooks/demo_spark.ipynb`

## Peta folder & file

```
airflow_repo/
├── docker-compose.yml                  ← langkah 0 (menjalankan lab)
└── workspace/                          ← = /workspace di dalam container
    ├── data/
    │   ├── demo_sales.csv              ← langkah 5 (dataset transaksi)
    │   ├── demo_customers.csv          ← langkah 10 (dataset customer)
    │   └── output/                     ← langkah 5 & 13 (hasil tulis, dibuat otomatis)
    └── notebooks/
        ├── demo_spark.ipynb            ← kunci jawaban
        └── latihan_spark.ipynb         ← langkah 1 (kita buat)

docker/spark-defaults.conf              ← setting Spark (2 core, memory 1 GB)
```

---

## 0. Jalankan lab

**Terminal** (di folder `airflow_repo`):

```bash
docker compose up -d
docker compose ps
```

- `docker compose up -d` → menyalakan 2 container: `lab` (Python, Spark, Jupyter, Airflow) dan `postgres`. `-d` = jalan di background.
- `docker compose ps` → cek status. Berhasil kalau `postgres` **healthy** dan `lab` **running**.

> Laptop RAM kecil? Ubah `SERVICES=jupyter` di file `.env` supaya Airflow tidak ikut jalan, lalu `docker compose up -d` lagi.

## 1. Buat notebook

1. Buka **http://localhost:8888** (JupyterLab, tanpa password).
2. Masuk folder `notebooks` → **File → New → Notebook** → pilih kernel **Python 3 (ipykernel)**.
3. Klik kanan tab notebook → **Rename** → `latihan_spark.ipynb`.

Setiap blok **Cell** di bawah diketik di cell baru, lalu jalankan dengan `Shift + Enter`.

---

## 2. SparkSession

**Cell:**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("demo-spark").getOrCreate()

print("Versi Spark :", spark.version)
print("Master      :", spark.sparkContext.master)
```

- Baris 1 → `SparkSession` = pintu masuk ke semua fitur Spark.
- Baris 2 → `functions as F` = kumpulan fungsi kolom (`F.col`, `F.sum`, `F.when`, ...). Disingkat `F` supaya tidak bentrok dengan fungsi Python bawaan seperti `sum`.
- Baris 3 → tipe data untuk membuat **schema** (langkah 4).
- Baris 4 → `Window` untuk window function (langkah 12).
- `.builder.appName("demo-spark")` → memberi nama aplikasi (terlihat di Spark UI).
- `.getOrCreate()` → membuat session baru, atau memakai yang sudah ada kalau cell dijalankan ulang.
- `spark.sparkContext.master` → mode Spark. Hasilnya `local[2]` = Spark jalan di laptop sendiri dengan 2 core (bukan cluster).

**Output:**

```
Versi Spark : 4.2.1
Master      : local[2]
```

> Selama session hidup, buka **http://localhost:4040** (Spark UI) untuk melihat job yang berjalan.

## 3. DataFrame pertama

**Cell:**

```python
data = [("Alice", 34, "Jakarta"), ("Bob", 45, "Bandung"), ("Cathy", 29, "Surabaya")]
df_people = spark.createDataFrame(data, ["Name", "Age", "City"])

df_people.show()
df_people.printSchema()
```

- `data` → list of tuple; 1 tuple = 1 baris.
- `spark.createDataFrame(data, [...])` → mengubah data Python jadi DataFrame Spark; list kedua = nama kolom.
- `.show()` → menampilkan isi (default 20 baris).
- `.printSchema()` → menampilkan nama kolom, tipe data, dan boleh null atau tidak. Tipe ditebak otomatis: angka Python → `long`.

**Output:**

```
+-----+---+--------+
| Name|Age|    City|
+-----+---+--------+
|Alice| 34| Jakarta|
|  Bob| 45| Bandung|
|Cathy| 29|Surabaya|
+-----+---+--------+

root
 |-- Name: string (nullable = true)
 |-- Age: long (nullable = true)
 |-- City: string (nullable = true)
```

## 4. RDD (Resilient Distributed Dataset)

RDD = bentuk data paling dasar di Spark. Sifatnya: **in-memory**, **immutable** (tidak bisa diubah, setiap transformasi menghasilkan RDD baru), **fault tolerant**, **lazy**, dan **terpartisi**.

**Cell:**

```python
sc = spark.sparkContext

rdd = sc.parallelize(range(1, 11), numSlices=2)
print("Partisi      :", rdd.getNumPartitions())
print("Isi partisi  :", rdd.glom().collect())

squared_even = rdd.map(lambda x: x * x).filter(lambda x: x % 2 == 0)
print("Kuadrat genap:", squared_even.collect())
print("Total        :", squared_even.reduce(lambda a, b: a + b))
```

- `sc = spark.sparkContext` → RDD dibuat lewat `SparkContext`, bukan `SparkSession`.
- `sc.parallelize(range(1, 11), numSlices=2)` → angka 1–10 dipecah jadi **2 partisi**.
- `rdd.glom().collect()` → `glom` mengelompokkan isi per partisi; `collect` membawa hasilnya ke Python. Terlihat partisi 1 berisi 1–5 dan partisi 2 berisi 6–10.
- `.map(lambda x: x * x)` → **transformation**: setiap elemen dikuadratkan.
- `.filter(lambda x: x % 2 == 0)` → **transformation**: ambil yang genap saja.
- Sampai di sini belum ada yang dihitung (**lazy**). Perhitungan baru jalan saat **action** `collect()` / `reduce()` dipanggil.
- `.reduce(lambda a, b: a + b)` → menjumlahkan semua elemen jadi 1 nilai.

**Output:**

```
Partisi      : 2
Isi partisi  : [[1, 2, 3, 4, 5], [6, 7, 8, 9, 10]]
Kuadrat genap: [4, 16, 36, 64, 100]
Total        : 220
```

**Cell** (word count + RDD → DataFrame):

```python
lines = sc.parallelize(["spark itu cepat", "airflow untuk orkestrasi", "spark dan airflow"])
word_count = lines.flatMap(lambda l: l.split()).map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
print(sorted(word_count.collect(), key=lambda x: -x[1]))

df_wc = spark.createDataFrame(word_count, ["word", "count"])
df_wc.orderBy("count", ascending=False).show()
```

- `flatMap(lambda l: l.split())` → setiap kalimat dipecah jadi kata. `flatMap` meratakan hasilnya: 3 kalimat jadi 9 kata (bukan 3 list).
- `.map(lambda w: (w, 1))` → setiap kata jadi pasangan `(kata, 1)`.
- `.reduceByKey(lambda a, b: a + b)` → pasangan dengan kata (key) yang sama dijumlahkan.
- `sorted(..., key=lambda x: -x[1])` → sort di Python dari jumlah terbanyak.
- `spark.createDataFrame(word_count, [...])` → cara membuat DataFrame **dari RDD**.
- `.orderBy("count", ascending=False)` → urutkan dari terbesar.

**Output:**

```
[('airflow', 2), ('spark', 2), ('itu', 1), ('orkestrasi', 1), ('cepat', 1), ('untuk', 1), ('dan', 1)]
+----------+-----+
|      word|count|
+----------+-----+
|   airflow|    2|
|     spark|    2|
...
```

> Sekarang kita lebih sering pakai **DataFrame** karena dioptimasi otomatis oleh Spark (Catalyst Optimizer). Tapi di balik layar, DataFrame tetap berjalan di atas RDD.

## 5. Membaca CSV & schema

**Cell** (cara 1: `inferSchema`):

```python
df_infer = spark.read.csv("/workspace/data/demo_sales.csv", header=True, inferSchema=True)
df_infer.printSchema()
```

- `spark.read.csv(path, ...)` → membaca file CSV. Path dimulai `/workspace/...` karena kode berjalan **di dalam container**.
- `header=True` → baris pertama dipakai sebagai nama kolom.
- `inferSchema=True` → Spark menebak tipe tiap kolom. Praktis, tapi file dibaca **2 kali** (sekali untuk menebak, sekali untuk memuat).

**Output:**

```
root
 |-- OrderID: string (nullable = true)
 |-- OrderDate: date (nullable = true)
 |-- Quantity: integer (nullable = true)
 |-- UnitPrice: integer (nullable = true)
 ...
```

**Cell** (cara 2: schema eksplisit):

```python
sales_schema = StructType([
    StructField("OrderID", StringType()),
    StructField("OrderDate", DateType()),
    StructField("CustomerID", StringType()),
    StructField("Product", StringType()),
    StructField("Category", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", LongType()),
    StructField("PaymentMethod", StringType()),
])
sales = spark.read.csv("/workspace/data/demo_sales.csv", header=True, schema=sales_schema)

sales.show(5)
print("Jumlah baris :", sales.count())
print("Jumlah partisi:", sales.rdd.getNumPartitions())
```

- `StructType([...])` → definisi schema = daftar kolom.
- `StructField("OrderID", StringType())` → 1 kolom: nama + tipe. Urutannya harus sama dengan urutan kolom di CSV.
- `LongType()` untuk `UnitPrice` → angka besar (harga × qty bisa melebihi batas `Integer` ±2,1 miliar).
- `schema=sales_schema` → Spark tidak perlu menebak, jadi lebih cepat dan tipenya pasti. **Disarankan untuk pipeline.**
- `.show(5)` → tampilkan 5 baris saja.
- `.count()` → **action**: hitung jumlah baris.
- `sales.rdd.getNumPartitions()` → DataFrame juga terdiri dari partisi. File kecil = 1 partisi.

**Output:**

```
+-------+----------+----------+--------+-----------+--------+---------+-------------+
|OrderID| OrderDate|CustomerID| Product|   Category|Quantity|UnitPrice|PaymentMethod|
+-------+----------+----------+--------+-----------+--------+---------+-------------+
|ORD0001|2024-01-02|      C047|Keyboard|Electronics|       4|   450000|  Credit Card|
...
only showing top 5 rows
Jumlah baris : 301
Jumlah partisi: 1
```

**Cell:**

```python
sales.describe("Quantity", "UnitPrice").show()
```

- `describe` → statistik deskriptif: count, mean, stddev, min, max.
- Perhatikan **count Quantity = 293**, sedangkan UnitPrice = 301 → ada **8 Quantity kosong**. Ini akan dibersihkan di langkah 8.

**Cell** (JSON & TXT):

```python
import json, os
os.makedirs("/workspace/data/output", exist_ok=True)
with open("/workspace/data/output/demo_products.json", "w") as f:
    for p in [{"Product": "Laptop", "Brand": "Lenovo", "Warranty": 2}, {"Product": "Mouse", "Brand": "Logitech", "Warranty": 1}]:
        f.write(json.dumps(p) + "\n")

spark.read.json("/workspace/data/output/demo_products.json").show()

spark.read.text("/workspace/data/demo_customers.csv").show(3, truncate=False)
```

- Baris 2–5 → membuat contoh file JSON dulu. Formatnya **1 objek JSON per baris** (*JSON Lines*), format yang dibaca Spark secara default.
- `spark.read.json(...)` → membaca JSON; schema langsung terbentuk dari key-nya.
- `spark.read.text(...)` → membaca file teks; setiap baris jadi 1 row dengan 1 kolom bernama `value`.
- `truncate=False` → tampilkan isi kolom lengkap tanpa dipotong.

## 6. Transformasi dasar

**Cell:**

```python
sales_tx = (
    sales
    .withColumn("TotalPrice", F.col("Quantity") * F.col("UnitPrice"))
    .withColumn("OrderMonth", F.date_format("OrderDate", "yyyy-MM"))
    .withColumn("PriceTier", F.when(F.col("UnitPrice") >= 1_000_000, "High")
                               .when(F.col("UnitPrice") >= 100_000, "Medium")
                               .otherwise("Low"))
)

sales_tx.select("OrderID", "Product", "Quantity", "UnitPrice", "TotalPrice", "PriceTier") \
        .filter(F.col("Category") == "Electronics") \
        .show(5)
```

- Tanda kurung `( ... )` → supaya bisa menulis *method chaining* ke bawah tanpa `\`.
- `.withColumn("TotalPrice", ...)` → menambah kolom baru (kalau namanya sudah ada, kolom itu diganti).
- `F.col("Quantity") * F.col("UnitPrice")` → operasi antar kolom.
- `F.date_format("OrderDate", "yyyy-MM")` → tanggal jadi teks bulan, contoh `2024-01`.
- `F.when(kondisi, nilai).when(...).otherwise(nilai)` → seperti `if / elif / else` (atau `CASE WHEN` di SQL).
- `1_000_000` → cara Python menulis 1000000 supaya mudah dibaca.
- `.select(...)` → pilih kolom yang ditampilkan.
- `.filter(F.col("Category") == "Electronics")` → ambil baris Electronics saja (sama dengan `WHERE`).
- `\` di akhir baris → melanjutkan perintah ke baris berikutnya.

**Output:**

```
+-------+--------+--------+---------+----------+---------+
|OrderID| Product|Quantity|UnitPrice|TotalPrice|PriceTier|
+-------+--------+--------+---------+----------+---------+
|ORD0001|Keyboard|       4|   450000|   1800000|   Medium|
|ORD0004|  Laptop|       5|  9500000|  47500000|     High|
...
```

## 7. Lazy evaluation

**Cell:**

```python
plan = sales_tx.filter(F.col("TotalPrice") > 1_000_000).select("OrderID", "TotalPrice")
plan.explain()
plan.count()
```

- Baris 1 → hanya **mencatat rencana**, belum ada data yang diproses.
- `plan.explain()` → menampilkan *physical plan* hasil optimasi. Perhatikan `PushedFilters` dan `ReadSchema`: Spark hanya membaca 3 kolom yang dibutuhkan, dan filter dijalankan sedini mungkin.
- `plan.count()` → **action**, baru sekarang job dijalankan. Hasilnya `84`. Cek job-nya di Spark UI (http://localhost:4040).

| Transformation (lazy) | Action (jalan) |
| --- | --- |
| `select`, `filter`, `withColumn`, `groupBy`, `join`, `orderBy` | `show`, `count`, `collect`, `take`, `write`, `toPandas` |

## 8. Data cleaning

**Cell** (cek duplikat):

```python
sales.groupBy("OrderID").count().filter("count > 1").show()
```

- `groupBy("OrderID").count()` → hitung jumlah baris per OrderID.
- `.filter("count > 1")` → filter boleh ditulis sebagai string SQL. Hasilnya `ORD0010` muncul 2 kali.

**Cell** (cek null per kolom):

```python
sales.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in sales.columns]).show()
```

Dibaca dari dalam ke luar:

- `for c in sales.columns` → ulangi untuk setiap nama kolom (*list comprehension*).
- `F.col(c).isNull()` → `true` kalau kosong.
- `.cast("int")` → `true` jadi 1, `false` jadi 0.
- `F.sum(...)` → jumlahkan → jumlah null di kolom itu.
- `.alias(c)` → beri nama hasil sesuai nama kolomnya.

**Output:**

```
|OrderID|OrderDate|CustomerID|Product|Category|Quantity|UnitPrice|PaymentMethod|
|      0|        0|         0|      0|       0|       8|        0|            5|
```

**Cell** (cek konsistensi teks):

```python
sales.groupBy("Product").count().orderBy("Product").show()
```

- Terlihat ada `LAPTOP` dan `Laptop`, `BACKPACK` dan `Backpack` → produk sama tapi tertulis beda.

**Cell** (bersihkan):

```python
sales_clean = (
    sales
    .dropDuplicates(["OrderID"])
    .withColumn("Product", F.initcap(F.trim("Product")))
    .fillna({"PaymentMethod": "Unknown"})
    .dropna(subset=["Quantity"])
    .withColumn("TotalPrice", F.col("Quantity") * F.col("UnitPrice"))
)

print("Sebelum:", sales.count(), "| Sesudah:", sales_clean.count())
sales_clean.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in sales_clean.columns]).show()
```

- `.dropDuplicates(["OrderID"])` → sisakan 1 baris per OrderID.
- `F.trim("Product")` → hapus spasi di awal/akhir. `F.initcap(...)` → huruf pertama kapital (`LAPTOP` → `Laptop`). Efek samping: `T-Shirt` → `T-shirt`.
- `.fillna({"PaymentMethod": "Unknown"})` → isi null di kolom kategori dengan `Unknown` (data tidak dibuang).
- `.dropna(subset=["Quantity"])` → buang baris yang Quantity-nya kosong, karena total harga tidak bisa dihitung.
- Baris terakhir → cek ulang null, sekarang semuanya 0.

**Output:**

```
Sebelum: 301 | Sesudah: 292
```

> 301 − 1 duplikat − 8 Quantity kosong = **292 baris**.

## 9. Agregasi

**Cell:**

```python
(
    sales_clean.groupBy("Category")
    .agg(
        F.count("OrderID").alias("TotalOrders"),
        F.sum("Quantity").alias("TotalQty"),
        F.sum("TotalPrice").alias("Revenue"),
        F.round(F.avg("TotalPrice"), 0).alias("AvgOrderValue"),
    )
    .orderBy(F.desc("Revenue"))
    .show()
)
```

- `.groupBy("Category")` → kelompokkan per kategori.
- `.agg(...)` → beberapa agregasi sekaligus: `count`, `sum`, `avg`.
- `F.round(..., 0)` → bulatkan tanpa desimal.
- `.alias(...)` → nama kolom hasil.
- `F.desc("Revenue")` → urutkan dari revenue terbesar.

**Output:**

```
+-----------+-----------+--------+----------+-------------+
|   Category|TotalOrders|TotalQty|   Revenue|AvgOrderValue|
+-----------+-----------+--------+----------+-------------+
|Electronics|        112|     349|1285450000|  1.1477232E7|
|    Fashion|         67|     199|  42740000|     637910.0|
|       Food|         48|     141|   9165000|     190938.0|
| Stationery|         65|     184|   3240000|      49846.0|
+-----------+-----------+--------+----------+-------------+
```

## 10. Join

**Cell:**

```python
customers = spark.read.csv("/workspace/data/demo_customers.csv", header=True, inferSchema=True)

sales_cust = sales_clean.join(customers, on="CustomerID", how="left")

(
    sales_cust.groupBy("City")
    .agg(F.countDistinct("CustomerID").alias("Customers"), F.sum("TotalPrice").alias("Revenue"))
    .orderBy(F.desc("Revenue"))
    .show()
)
```

- Baris 1 → baca tabel customer.
- `.join(customers, on="CustomerID", how="left")` → gabungkan berdasarkan kolom yang sama. `left` = semua transaksi tetap ada walau customer-nya tidak ditemukan. Pilihan lain: `inner`, `right`, `outer`, `left_anti`.
- `F.countDistinct("CustomerID")` → jumlah customer unik per kota.

**Output:**

```
+----------+---------+---------+
|      City|Customers|  Revenue|
+----------+---------+---------+
|   Jakarta|       15|548307000|
|   Bandung|       12|335670000|
...
```

## 11. Spark SQL

**Cell:**

```python
sales_cust.createOrReplaceTempView("sales")

spark.sql('''
    SELECT Segment,
           PaymentMethod,
           COUNT(*)        AS Orders,
           SUM(TotalPrice) AS Revenue
    FROM sales
    GROUP BY Segment, PaymentMethod
    ORDER BY Segment, Revenue DESC
''').show()
```

- `createOrReplaceTempView("sales")` → mendaftarkan DataFrame sebagai "tabel" bernama `sales`. Hanya ada selama session hidup.
- `spark.sql('''...''')` → menjalankan SQL biasa; hasilnya **DataFrame** juga, jadi bisa lanjut `.show()`, `.filter()`, dst.
- `'''` → string multi-baris di Python.

> Bisa pilih gaya DataFrame API atau SQL; performanya sama karena sama-sama dioptimasi Catalyst.

## 12. Window function

**Cell** (ranking per kategori):

```python
product_rev = sales_clean.groupBy("Category", "Product").agg(F.sum("TotalPrice").alias("Revenue"))

w_rank = Window.partitionBy("Category").orderBy(F.desc("Revenue"))
product_rev.withColumn("RankInCategory", F.dense_rank().over(w_rank)).orderBy("Category", "RankInCategory").show()
```

- Baris 1 → revenue per produk.
- `Window.partitionBy("Category")` → ranking dihitung **terpisah di tiap kategori**.
- `.orderBy(F.desc("Revenue"))` → urutan di dalam tiap kategori.
- `F.dense_rank().over(w_rank)` → beri peringkat 1, 2, 3, ... Bedanya dengan `groupBy`: jumlah baris **tidak berkurang**.

**Output:**

```
+-----------+--------+----------+--------------+
|   Category| Product|   Revenue|RankInCategory|
+-----------+--------+----------+--------------+
|Electronics|  Laptop|1216000000|             1|
|Electronics|Keyboard|  54450000|             2|
|Electronics|   Mouse|  15000000|             3|
|    Fashion|Backpack|  28700000|             1|
...
```

**Cell** (running total):

```python
monthly = (
    sales_clean.withColumn("OrderMonth", F.date_format("OrderDate", "yyyy-MM"))
    .groupBy("OrderMonth").agg(F.sum("TotalPrice").alias("Revenue"))
)

w_running = Window.orderBy("OrderMonth").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly.withColumn("RunningRevenue", F.sum("Revenue").over(w_running)).orderBy("OrderMonth").show(12)
```

- Baris 1–4 → revenue per bulan.
- `rowsBetween(Window.unboundedPreceding, Window.currentRow)` → jendela = dari baris pertama sampai baris sekarang.
- `F.sum("Revenue").over(w_running)` → jumlah kumulatif (running total).
- Muncul warning `No Partition Defined for Window operation` karena tidak ada `partitionBy`, sehingga semua data dipindah ke 1 partisi. Aman untuk data kecil, tapi hindari untuk data besar.

**Output:**

```
+----------+---------+--------------+
|OrderMonth|  Revenue|RunningRevenue|
+----------+---------+--------------+
|   2024-01|129963000|     129963000|
|   2024-02|129292000|     259255000|
|   2024-03| 90580000|     349835000|
...
```

## 13. Menyimpan hasil (Parquet)

**Cell:**

```python
output_path = "/workspace/data/output/sales_clean_parquet"
sales_clean.write.mode("overwrite").partitionBy("Category").parquet(output_path)

spark.read.parquet(output_path).filter(F.col("Category") == "Electronics").show(3)
```

- `.write.mode("overwrite")` → kalau folder sudah ada, timpa. Mode lain: `append`, `ignore`, `error` (default).
- `.partitionBy("Category")` → output dipecah jadi subfolder per kategori.
- `.parquet(path)` → format **kolumnar**: lebih kecil dan cepat dari CSV, dan schema ikut tersimpan.
- Baris terakhir → baca kembali. Karena di-partition, filter `Electronics` hanya membaca 1 subfolder.

**Cek di file explorer:** `workspace/data/output/sales_clean_parquet/` berisi folder `Category=Electronics`, `Category=Fashion`, ... dan di dalamnya file `part-*.parquet`.

> Output Spark selalu berupa **folder**, bukan 1 file, karena tiap partisi menulis filenya sendiri.

## 14. Load & read PostgreSQL (JDBC)

**Cell** (tulis):

```python
import os

jdbc_url = "jdbc:postgresql://postgres:5432/dwh"
jdbc_props = {
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "driver": "org.postgresql.Driver",
}

customers.write.jdbc(jdbc_url, "demo_dim_customer", mode="overwrite", properties=jdbc_props)
sales_clean.select("OrderID", "OrderDate", "CustomerID", "Product", "Quantity", "UnitPrice", "TotalPrice") \
           .write.jdbc(jdbc_url, "demo_fact_sales", mode="overwrite", properties=jdbc_props)

query = "(SELECT COUNT(*) AS jumlah_baris FROM demo_fact_sales) AS t"
spark.read.jdbc(jdbc_url, query, properties=jdbc_props).show()
```

- `jdbc:postgresql://postgres:5432/dwh` → format `jdbc:postgresql://<host>:<port>/<database>`. Host-nya **`postgres`** (nama service di docker-compose), **bukan** `localhost`, karena kode jalan di dalam container.
- `os.getenv("POSTGRES_USER", "postgres")` → ambil user dari environment; kalau tidak ada, pakai `postgres`. Password tidak di-hardcode.
- `"driver": "org.postgresql.Driver"` → driver JDBC (file `.jar`-nya sudah terpasang di image, tidak perlu `spark.jars.packages`).
- `.write.jdbc(url, "nama_tabel", mode, properties)` → tabel dibuat otomatis kalau belum ada.
- `query = "(SELECT ...) AS t"` → untuk membaca hasil query, query dibungkus kurung dan diberi alias.

**Output:**

```
+------------+
|jumlah_baris|
+------------+
|         292|
+------------+
```

**Cek dari laptop (DBeaver/pgAdmin):** host `localhost`, port **5433**, user/pass `postgres`/`postgres`, database `dwh` → tabel `demo_dim_customer` dan `demo_fact_sales`.

Atau dari **Terminal:**

```bash
docker compose exec postgres psql -U postgres -d dwh -c "SELECT * FROM demo_fact_sales LIMIT 5;"
```

**Cell** (baca tabel besar secara paralel):

```python
query_big = "(SELECT *, EXTRACT(MONTH FROM \"OrderDate\")::int AS order_month FROM demo_fact_sales) AS t"
df_parallel = spark.read.jdbc(
    url=jdbc_url,
    table=query_big,
    column="order_month",
    lowerBound=1,
    upperBound=12,
    numPartitions=4,
    properties=jdbc_props,
)
print("Partisi:", df_parallel.rdd.getNumPartitions(), "| Baris:", df_parallel.count())
```

- `\"OrderDate\"` → nama kolom yang ada huruf besarnya harus pakai tanda kutip ganda di PostgreSQL. `\"` = tanda kutip di dalam string Python.
- `EXTRACT(MONTH FROM ...)::int` → buat kolom angka bulan (1–12) sebagai pembagi.
- `column`, `lowerBound`, `upperBound`, `numPartitions` → Spark membagi rentang 1–12 jadi 4 bagian dan membuka **4 koneksi** sekaligus; tiap koneksi membaca sebagian bulan.
- `lowerBound`/`upperBound` hanya untuk membagi rentang, **bukan filter**: data di luar rentang tetap terbaca.

**Output:**

```
Partisi: 4 | Baris: 292
```

## 15. Spark → pandas & tutup session

**Cell:**

```python
summary_pdf = sales_clean.groupBy("Category").agg(F.sum("TotalPrice").alias("Revenue")).toPandas()
summary_pdf
```

- `.toPandas()` → membawa **semua data ke memori laptop** (driver). Hanya untuk data yang sudah kecil, misalnya hasil agregasi untuk visualisasi.

**Cell:**

```python
spark.stop()
```

- Menghentikan session dan melepas memori. Spark UI (port 4040) ikut mati.

---

## Ringkasan

| Perintah | Fungsi |
| --- | --- |
| `SparkSession.builder.appName(..).getOrCreate()` | Membuat session |
| `sc.parallelize`, `map`, `filter`, `reduce` | Operasi RDD |
| `spark.read.csv / json / text / parquet / jdbc` | Membaca data |
| `select`, `filter`, `withColumn`, `F.when` | Transformasi dasar |
| `dropDuplicates`, `fillna`, `dropna`, `F.initcap(F.trim())` | Cleaning |
| `groupBy().agg()` | Agregasi |
| `join(on=, how=)` | Menggabungkan tabel |
| `createOrReplaceTempView` + `spark.sql` | Spark SQL |
| `Window.partitionBy().orderBy()` + `.over()` | Window function |
| `df.write.mode().partitionBy().parquet()` | Simpan ke file |
| `df.write.jdbc()` | Simpan ke PostgreSQL |
| `explain()` | Lihat rencana eksekusi |
| `spark.stop()` | Tutup session |

▎ ***SparkSession** = pintu masuk, **RDD** = data mentah terdistribusi, **DataFrame** = tabel terdistribusi, **transformation** = resep (lazy), **action** = perintah memasak, **partisi** = potongan data yang dikerjakan paralel.*
