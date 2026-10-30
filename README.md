# DE Lab — Docker untuk Graded Challenge 6 & Milestone 3

Satu image berisi semua yang dibutuhkan:

| Komponen | Versi |
|---|---|
| Python | 3.13 |
| PySpark | 4.2.1 (+ Java 21, driver JDBC PostgreSQL sudah terpasang) |
| Apache Airflow | 3.3.2 |
| Great Expectations | 1.24.1 |
| pymongo | dari constraints Airflow (4.18.x) |
| JupyterLab + ipykernel | ✔ |
| PostgreSQL | 17 (container terpisah `postgres:17-alpine`) |

## 1. Persiapan (sekali saja)
Bisa dipakai di **Windows 10/11, macOS (Intel & Apple Silicon M1–M4), dan Linux**.

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/).
   - **Windows:** saat install pilih *Use WSL 2* (default). Jika diminta, jalankan `wsl --install` di PowerShell (Run as Administrator) lalu restart.
   - **Mac:** pilih installer sesuai chip (*Apple Silicon* atau *Intel*).
   - Buka Docker Desktop dan tunggu sampai status **Engine running**.
   - Memory: Mac → *Settings → Resources* set minimal **4 GB**. Windows (WSL 2) otomatis memakai sampai 50% RAM laptop, tidak perlu diubah.
2. Download repo ini (pilih salah satu):
   - **Pakai git:**
     ```bash
     git clone https://github.com/Fredyco/airflow_repo.git
     cd airflow_repo
     ```
   - **Tanpa git:** di halaman GitHub klik **Code → Download ZIP**, extract, lalu buka folder hasil extract.
3. Buka terminal di folder `airflow_repo` (Windows: **PowerShell**, Mac: **Terminal**), salin file env:
   ```bash
   # Mac / Linux
   cp .env.example .env
   ```
   ```powershell
   # Windows (PowerShell)
   Copy-Item .env.example .env
   ```
   Buka `.env` dengan editor (VS Code/Notepad) dan isi `MONGO_URI` dengan connection string MongoDB Atlas kamu (untuk M3).
   File `.env` boleh tidak dibuat — semua tetap jalan dengan nilai default.

## 2. Menjalankan
```bash
docker compose up -d --build     # build pertama kali agak lama (download ±1 GB)
```
| Layanan | Alamat |
|---|---|
| JupyterLab | http://localhost:8888 (tanpa password) |
| Airflow UI | http://localhost:8080 (tanpa login) |
| Spark UI | http://localhost:4040 (saat SparkSession aktif) |
| PostgreSQL | `localhost:5433`, user/pass `postgres`/`postgres`, database `dwh` |

Pause sementara: `docker compose stop` → lanjut lagi: `docker compose start`.
Mematikan & hapus container: `docker compose down` (data tetap aman, lihat bagian 3).
Hapus total termasuk data: `docker compose down -v`.

Perintah `docker compose ...` sama persis di Windows (PowerShell/CMD) dan Mac.

**Laptop RAM kecil?** Untuk GC6 cukup Jupyter saja: di file `.env` ubah `SERVICES=all` menjadi `SERVICES=jupyter`, lalu jalankan ulang `docker compose up -d`.

## 3. Data & update file (tanpa restart)

| Yang diubah / disimpan | Tersimpan di | Perlu restart? |
|---|---|---|
| Notebook, DAG, script, CSV di `workspace/` | **folder laptop** (bind mount `./workspace` ↔ `/workspace`) | Tidak. DAG baru/berubah muncul di Airflow ±30 detik |
| Tabel PostgreSQL (`dwh`) & riwayat run Airflow | volume Docker `pgdata` | – |
| Log task Airflow | volume Docker `airflow-logs` | – |
| `.env` / `docker-compose.yml` | – | `docker compose up -d` |
| `Dockerfile` / `requirements.txt` | – | `docker compose up -d --build` |

Semua data di atas **tetap aman** walau container di-stop, di-pause, dihapus (`docker compose down`), bahkan image-nya dihapus lalu di-build ulang.
Data hanya hilang jika volume ikut dihapus: `docker compose down -v`.

> Paket yang di-`pip install` manual di dalam container akan hilang saat container dibuat ulang. Supaya permanen, tambahkan ke `requirements.txt` lalu `docker compose up -d --build`.

## 4. Struktur folder
Folder `workspace/` di laptop = `/workspace` di container. Semua file tersimpan di laptop kamu.
```
airflow_repo/
├── docker-compose.yml, Dockerfile, ...   # setup lab (tidak perlu diubah)
├── panduan/                              # materi step by step
│   ├── 01_materi_pyspark.md
│   └── 02_materi_airflow.md
└── workspace/                            # tempat kerja (notebook, DAG, data)
    ├── notebooks/
    │   └── demo_spark.ipynb          # materi PySpark
    ├── data/
    │   ├── demo_sales.csv            # dataset demo (ada data kotor untuk latihan cleaning)
    │   └── demo_customers.csv
    ├── dags/                         # semua DAG Airflow
    │   ├── demo_01_hello_bash.py
    │   ├── demo_02_python_parallel.py
    │   ├── demo_03_crypto_etl.py
    │   ├── demo_04_sales_elt.py
    │   └── demo_05_csv_clean_slide.py
    └── scripts/
        ├── crypto/                   # extract.py, transform.py, load.py (dipanggil demo_03)
        └── elt/sql/                  # 01_staging.sql, 02_mart.sql (dipanggil demo_04)
```

## 5. Materi demo

Panduan step by step (penjelasan baris per baris):
- [`panduan/01_materi_pyspark.md`](panduan/01_materi_pyspark.md)
- [`panduan/02_materi_airflow.md`](panduan/02_materi_airflow.md)

### Spark — `notebooks/demo_spark.ipynb`
Buka di JupyterLab lalu *Run All*. Isi: SparkSession & Spark UI, membuat DataFrame, RDD (map/filter/reduce, word count, RDD→DataFrame), baca CSV/JSON/TXT (inferSchema vs schema eksplisit),
select/filter/withColumn, lazy evaluation & `explain()`, data cleaning (duplikat, null, standarisasi teks), agregasi, join,
Spark SQL, window function, simpan Parquet + `partitionBy`, load & read PostgreSQL via JDBC (termasuk baca paralel dengan `numPartitions`), `toPandas()`.

### Airflow — folder `dags/`
Di http://localhost:8080 aktifkan (toggle) DAG lalu klik ▶ *Trigger*.

| DAG | Konsep |
|---|---|
| `demo_01_hello_bash` | DAG paling sederhana: 2 BashOperator berurutan, cron schedule, `catchup` |
| `demo_02_python_parallel` | PythonOperator, task paralel (fan-out/fan-in), `op_kwargs`, XCom |
| `demo_03_crypto_etl` | **ETL**: DAG menjalankan script terpisah — extract (API CoinGecko) → transform (PySpark + kurs USD/IDR) → load (PostgreSQL), retries |
| `demo_04_sales_elt` | **ELT** dengan TaskFlow API: CSV mentah dimuat ke schema `raw` → transform dengan SQL di database (`staging` → `mart`) → quality check |
| `demo_05_csv_clean_slide` | Contoh slide (`H8CSVDAG` CSV→JSON & `H8CleanData` clean → filter → copy) versi Airflow 3 |

### Kode di slide (Airflow 2) → Airflow 3
Image ini memakai **Airflow 3**, beberapa sintaks lama di slide sudah dihapus dan akan error:

| Airflow 2 (slide) | Airflow 3 |
|---|---|
| `from airflow import DAG` | `from airflow.sdk import DAG` (cara lama masih jalan, tapi deprecated) |
| `from airflow.operators.bash_operator import BashOperator` | `from airflow.providers.standard.operators.bash import BashOperator` |
| `from airflow.operators.python_operator import PythonOperator` | `from airflow.providers.standard.operators.python import PythonOperator` |
| `schedule_interval=...` | `schedule=...` |
| `/opt/airflow/dags/data.csv` | `/workspace/data/data.csv` |
| login `airflow` / `airflow` | tanpa login |
| `sudo -u airflow python script.py` | `python /workspace/scripts/script.py` |

Hasil demo 03 & 04 bisa dicek di database `dwh`:
```sql
SELECT * FROM crypto_price ORDER BY loaded_at DESC;
SELECT * FROM mart.sales_by_city_month;
SELECT * FROM mart.top_products WHERE rank_in_category = 1;
```
Contoh perintah: `docker compose exec postgres psql -U postgres -d dwh` (atau DBeaver ke `localhost:5433`).

> demo_03 butuh internet (API publik). Jika API sedang membatasi request, task otomatis di-retry 2x.

## 6. Contoh kode

**PySpark → PostgreSQL (GC6)**
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("gc6").getOrCreate()
df = spark.read.csv("/workspace/data/orders.csv", header=True, inferSchema=True)

df.write.jdbc(
    url="jdbc:postgresql://postgres:5432/dwh",
    table="fact_orders",
    mode="append",
    properties={"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"},
)
```
Dari laptop (DBeaver/pgAdmin) gunakan `localhost:5433`, dari dalam container (notebook/DAG) gunakan `postgres:5432`.

**MongoDB Atlas (M3)**
```python
import os
from pymongo import MongoClient
client = MongoClient(os.environ["MONGO_URI"])
client["p2m3"]["data_clean"].insert_many([r.asDict() for r in df.collect()])
```

**Great Expectations 1.x (M3)**
```python
import great_expectations as gx
context = gx.get_context()
batch = (context.data_sources.add_pandas("pandas")
         .add_dataframe_asset("raw")
         .add_batch_definition_whole_dataframe("batch")
         .get_batch(batch_parameters={"dataframe": df_pandas}))
batch.validate(gx.expectations.ExpectColumnValuesToBeUnique(column="id"))
```

**Jadwal DAG M3** — Sabtu 09:10–09:30 tiap 10 menit, mulai 1 Nov 2024 (zona waktu sudah Asia/Jakarta):
```python
schedule="10-30/10 9 * * 6", start_date=datetime(2024, 11, 1)
```
Agar DAG bisa `import extract`, simpan `extract.py`, `transform.py`, `load.py` di folder `workspace/dags/` yang sama.

## 7. Troubleshooting
- **Windows: `entrypoint.sh: no such file or directory` / `\r: command not found`** — sudah ditangani otomatis saat build. Jika masih muncul, jalankan `docker compose build --no-cache`.
- **Windows: folder proyek sebaiknya tidak di dalam OneDrive** (sinkronisasi OneDrive bisa mengunci file). Taruh misalnya di `C:\Users\<nama>\airflow_repo`.
- **Mac Apple Silicon & Windows/Intel** memakai image yang sama; Docker otomatis build sesuai arsitektur laptop.
- Log Airflow: `docker compose exec lab tail -f /opt/airflow/logs/scheduler.out`
- Jalankan perintah di container: `docker compose exec lab bash`
- Port bentrok: ubah `POSTGRES_HOST_PORT` di `.env` (Postgres), atau port `8888`/`8080` di `docker-compose.yml`.
