# Materi Airflow — Step by Step

> Workflow Orchestration: Airflow • CODA Phase 2
> Versi: **Apache Airflow 3.3** (sintaks di slide lama = Airflow 2, lihat langkah 7)

## Peta folder & file

```
airflow_repo/
├── docker-compose.yml                        ← langkah 0 (menjalankan lab)
└── workspace/                                ← = /workspace di dalam container
    ├── dags/                                 ← SEMUA file DAG ditaruh di sini
    │   ├── demo_01_hello_bash.py             ← langkah 2
    │   ├── demo_02_python_parallel.py        ← langkah 4
    │   ├── demo_03_crypto_etl.py             ← langkah 5
    │   ├── demo_04_sales_elt.py              ← langkah 6
    │   ├── demo_05_csv_clean_slide.py        ← langkah 7
    │   └── latihan_dag_saya.py               ← langkah 8 (kita buat)
    ├── scripts/
    │   ├── crypto/
    │   │   ├── extract.py                    ← langkah 5
    │   │   ├── transform.py                  ← langkah 5
    │   │   └── load.py                       ← langkah 5
    │   └── elt/sql/
    │       ├── 01_staging.sql                ← langkah 6
    │       └── 02_mart.sql                   ← langkah 6
    └── data/
        ├── demo_sales.csv                    ← sumber data langkah 6 & 7
        └── demo_customers.csv
```

> Airflow **bukan** alat pemrosesan data dan **bukan** streaming. Airflow adalah **orkestrator**: mengatur *kapan* dan *urutan* script dijalankan, lalu memantau hasilnya.

---

## 0. Jalankan lab & buka Airflow

> Belum punya foldernya? Ikuti **README → 1. Persiapan** (install Docker Desktop & download repo `airflow_repo`).

**Terminal** (di folder `airflow_repo`):

```bash
docker compose up -d
docker compose ps
```

- `docker compose up -d` → menyalakan container `lab` (berisi Airflow + Spark + Jupyter) dan `postgres`.
- Pastikan `SERVICES=all` di file `.env` (atau `.env` tidak ada), supaya Airflow ikut jalan.

Buka **http://localhost:8080**. Tidak perlu login (di slide lama: `airflow`/`airflow`).

> Folder `workspace/` di laptop **terhubung langsung** ke container. File DAG yang disimpan dari VS Code/Notepad otomatis terbaca Airflow dalam ±30 detik, **tanpa restart**. Riwayat run dan log tetap ada walau container di-stop atau dihapus.

Di dalam container ada 3 proses Airflow:

| Proses | Tugas |
| --- | --- |
| `dag-processor` | membaca file `.py` di folder `dags/` (dicek ulang tiap ±30 detik) |
| `scheduler` | memutuskan kapan DAG/task harus jalan, lalu menjalankannya |
| `api-server` | web UI di port 8080 |

## 1. Kenali UI

1. Menu **Dags** → daftar semua DAG dari folder `workspace/dags/`.
2. **Toggle** di kiri nama DAG → DAG baru selalu dalam keadaan **paused**. Nyalakan supaya scheduler mau menjalankan jadwalnya.
3. Tombol **▶ Trigger** → menjalankan DAG sekarang juga (manual), tanpa menunggu jadwal.
4. Klik **nama DAG** → halaman detail:
   - **Grid** → riwayat run; 1 kolom = 1 run, 1 kotak = 1 task. Hijau = success, merah = failed, kuning = up for retry.
   - **Graph** → bentuk DAG (urutan task).
   - **Code** → isi file `.py`-nya.
5. Klik **kotak task** → tab **Logs** untuk melihat output `print`/`echo`, dan tab **XCom** untuk melihat nilai yang dikirim antar task.

## 2. DAG pertama (Bash)

**Buka:** `workspace/dags/demo_01_hello_bash.py`

```python
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="demo_01_hello_bash",
    description="Dua task Bash yang berjalan berurutan",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["demo", "basic"],
) as dag:

    task_1 = BashOperator(
        task_id="task_1",
        bash_command="echo 'Hello from task 1' && date",
    )

    task_2 = BashOperator(
        task_id="task_2",
        bash_command="echo 'Hello from task 2' && whoami",
    )

    task_1 >> task_2
```

- `from airflow.providers.standard.operators.bash import BashOperator` → **operator** = jenis task. `BashOperator` menjalankan perintah terminal.
- `from airflow.sdk import DAG` → class untuk membuat DAG (*Directed Acyclic Graph* = kumpulan task yang punya arah dan tidak berputar).
- `with DAG(...) as dag:` → semua task yang ditulis di dalam blok `with` otomatis masuk ke DAG ini.
- `dag_id` → nama unik DAG; inilah yang tampil di UI.
- `description` → keterangan singkat di UI.
- `start_date=datetime(2025, 1, 1)` → mulai kapan jadwal berlaku. Zona waktu lab = **Asia/Jakarta (WIB)**.
- `schedule="*/5 * * * *"` → cron: setiap 5 menit (lihat tabel cron di langkah 3).
- `catchup=False` → jadwal yang terlewat sejak `start_date` **tidak** dikejar. Kalau `True`, Airflow menjalankan semua run dari 2025 sampai sekarang.
- `tags` → label untuk filter di UI.
- `BashOperator(task_id=..., bash_command=...)` → 1 task. `task_id` harus unik di dalam DAG.
- `&& date` / `&& whoami` → setelah echo, tampilkan tanggal / user yang menjalankan.
- `task_1 >> task_2` → **dependency**: `task_2` baru jalan setelah `task_1` sukses.

Cara lain menulis dependency (hasilnya sama):

```python
task_1 >> task_2
task_2 << task_1
task_1.set_downstream(task_2)
task_2.set_upstream(task_1)
```

**Jalankan:**

1. Di UI, nyalakan toggle `demo_01_hello_bash` → klik **▶ Trigger**.
2. Klik nama DAG → **Grid** → tunggu kedua kotak hijau.
3. Klik kotak `task_1` → **Logs**. Cari baris:

```
Running command: ['/usr/bin/bash', '-c', "echo 'Hello from task 1' && date"]
Hello from task 1
```

> Setelah selesai, **matikan lagi toggle-nya** supaya DAG tidak jalan terus setiap 5 menit.

## 3. Schedule (cron)

Format cron: `menit jam tanggal bulan hari_dalam_minggu`

| Cron | Artinya |
| --- | --- |
| `*/5 * * * *` | setiap 5 menit |
| `0 * * * *` | setiap jam, menit ke-0 |
| `0 7 * * *` | setiap hari jam 07:00 |
| `30 8 * * 1-5` | Senin–Jumat jam 08:30 |
| `10-30/10 9 * * 6` | Sabtu jam 09:10, 09:20, 09:30 |
| `@daily` | setiap hari jam 00:00 (sama dengan `0 0 * * *`) |
| `None` | tidak terjadwal, hanya jalan kalau di-Trigger manual |

- Hari: `0` = Minggu, `1` = Senin, ..., `6` = Sabtu.
- Bisa juga pakai `timedelta`: `schedule=timedelta(minutes=60)`.
- Coba cron sendiri di https://crontab.guru.

**Terminal** (cek jadwal berikutnya):

```bash
docker compose exec lab airflow dags details demo_01_hello_bash | grep next_dagrun
```

> Waktu di output CLI memakai **UTC** (+00:00). Tambah 7 jam untuk WIB.

## 4. PythonOperator, task paralel & XCom

**Buka:** `workspace/dags/demo_02_python_parallel.py`

```python
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def hitung_kuadrat():
    hasil = [i ** 2 for i in range(1, 6)]
    print(f"Kuadrat: {hasil}")
    return hasil


def sapa(nama):
    print(f"Halo {nama}, ini python task 2")


def ringkas(ti):
    kuadrat = ti.xcom_pull(task_ids="python_1")
    print(f"Total kuadrat dari python_1 = {sum(kuadrat)}")


with DAG(
    dag_id="demo_02_python_parallel",
    ...
) as dag:

    start_bash = BashOperator(task_id="start_bash", bash_command="echo Start task")

    python_1 = PythonOperator(task_id="python_1", python_callable=hitung_kuadrat)

    python_2 = PythonOperator(task_id="python_2", python_callable=sapa, op_kwargs={"nama": "Data Engineer"})

    summary = PythonOperator(task_id="summary", python_callable=ringkas)

    end_bash = BashOperator(task_id="end_bash", bash_command="echo Finish task")

    start_bash >> [python_1, python_2] >> summary >> end_bash
```

- `PythonOperator` → task yang menjalankan **fungsi Python**.
- `def hitung_kuadrat()` → fungsi biasa. `return hasil` → nilai yang di-return **otomatis disimpan sebagai XCom** (*cross-communication*, cara task mengirim data kecil ke task lain).
- `def sapa(nama)` → fungsi dengan parameter.
- `def ringkas(ti)` → parameter bernama `ti` (*task instance*) otomatis diisi Airflow.
- `ti.xcom_pull(task_ids="python_1")` → ambil nilai return dari task `python_1`.
- `python_callable=hitung_kuadrat` → nama fungsi **tanpa tanda kurung** (yang memanggilnya Airflow, bukan kita).
- `op_kwargs={"nama": "Data Engineer"}` → argumen yang dikirim ke fungsi `sapa`.
- `start_bash >> [python_1, python_2]` → **fan-out**: `python_1` & `python_2` jalan **bersamaan** setelah `start_bash`.
- `[python_1, python_2] >> summary` → **fan-in**: `summary` menunggu keduanya selesai.

**Graph:**

```
              ┌─> python_1 ─┐
start_bash ───┤             ├─> summary ──> end_bash
              └─> python_2 ─┘
```

**Jalankan** (cara cepat untuk debugging, tanpa UI):

```bash
docker compose exec lab airflow dags test demo_02_python_parallel
```

- `airflow dags test <dag_id>` → menjalankan 1 run lengkap langsung di terminal dan menampilkan log semua task. Cocok untuk cek error sebelum dijadwalkan.

Cari di output:

```
Kuadrat: [1, 4, 9, 16, 25]
Halo Data Engineer, ini python task 2
Total kuadrat dari python_1 = 55
```

Menjalankan **1 task saja**:

```bash
docker compose exec lab airflow tasks test demo_02_python_parallel python_1
```

**Cek di UI:** run → kotak `python_1` → tab **XCom** → `return_value = [1, 4, 9, 16, 25]`.

> XCom hanya untuk data **kecil** (angka, list pendek, path file). Untuk DataFrame/data besar, simpan ke file atau database, lalu kirim path-nya saja.

## 5. ETL: DAG menjalankan script terpisah

Pola yang sama dengan tugas Milestone 3: logika ada di file `extract.py` / `transform.py` / `load.py`, sementara DAG hanya **memanggil** script-script itu secara berurutan.

```
extract_prices  >>  transform_with_spark  >>  load_to_postgres
 (API CoinGecko)     (PySpark + kurs IDR)       (PostgreSQL dwh)
```

**Buka:** `workspace/dags/demo_03_crypto_etl.py`

```python
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

SCRIPTS = "/workspace/scripts/crypto"

default_args = {
    "owner": "instruktur",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="demo_03_crypto_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["demo", "etl", "spark"],
) as dag:

    extract_prices = BashOperator(
        task_id="extract_prices",
        bash_command=f"python {SCRIPTS}/extract.py",
    )

    transform_with_spark = BashOperator(
        task_id="transform_with_spark",
        bash_command=f"python {SCRIPTS}/transform.py",
    )

    load_to_postgres = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"python {SCRIPTS}/load.py",
    )

    extract_prices >> transform_with_spark >> load_to_postgres
```

- `SCRIPTS = "/workspace/scripts/crypto"` → lokasi script di dalam container. Disimpan di variabel supaya tidak ditulis berulang.
- `default_args` → setting yang berlaku untuk **semua task** di DAG:
  - `owner` → pemilik (tampil di UI).
  - `retries: 2` → kalau gagal (misal API sedang down), task diulang sampai 2 kali.
  - `retry_delay` → jeda 1 menit sebelum mengulang.
- `schedule="0 * * * *"` → setiap jam.
- `max_active_runs=1` → maksimal 1 run berjalan bersamaan (Spark cukup berat untuk laptop).
- `bash_command=f"python {SCRIPTS}/extract.py"` → task = menjalankan script Python lewat terminal. Kalau script error (exit code ≠ 0), task jadi **failed**.
- Di slide lama ditulis `sudo -u airflow python /opt/airflow/scripts/...`. Di lab ini cukup `python /workspace/scripts/...`.

**Buka:** `workspace/scripts/crypto/extract.py`

```python
import os

import pandas as pd
import requests

URL = "https://api.coingecko.com/api/v3/simple/price"
COINS = ["bitcoin", "ethereum", "ripple", "solana", "dogecoin"]
OUTPUT = "/workspace/data/crypto/extract_result.csv"


def extract_prices(coins):
    response = requests.get(URL, params={"ids": ",".join(coins), "vs_currencies": "usd"}, timeout=30)
    response.raise_for_status()
    data = response.json()
    return pd.DataFrame({"coin_name": list(data), "coin_price": [v["usd"] for v in data.values()]})


if __name__ == "__main__":
    df = extract_prices(COINS)
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    df.to_csv(OUTPUT, index=False)
    print(df)
    print(f"Tersimpan: {OUTPUT}")
```

- `URL`, `COINS`, `OUTPUT` → konstanta (huruf besar) di atas supaya mudah diubah.
- `requests.get(URL, params=..., timeout=30)` → memanggil API. `params` otomatis jadi `?ids=bitcoin,ethereum,...&vs_currencies=usd`. `timeout=30` → jangan menunggu lebih dari 30 detik.
- `",".join(coins)` → list jadi teks `bitcoin,ethereum,...`.
- `response.raise_for_status()` → kalau API membalas error (misal 429 Too Many Requests), script **sengaja dibuat error**, sehingga task failed lalu Airflow melakukan retry.
- `response.json()` → isinya `{"bitcoin": {"usd": 81049}, "ethereum": {"usd": 2349.38}, ...}`.
- `list(data)` → ambil key-nya: nama coin. `[v["usd"] for v in data.values()]` → ambil harganya.
- `if __name__ == "__main__":` → blok ini hanya jalan kalau file dijalankan langsung (`python extract.py`), tidak jalan kalau di-`import`.
- `os.makedirs(..., exist_ok=True)` → buat folder output kalau belum ada.
- `df.to_csv(OUTPUT, index=False)` → hasil extract disimpan ke file, supaya bisa dibaca task berikutnya. **Antar task, data dioper lewat file/database.**

**Buka:** `workspace/scripts/crypto/transform.py`

```python
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

INPUT = "/workspace/data/crypto/extract_result.csv"
OUTPUT = "/workspace/data/crypto/transform_result.csv"


def get_usd_idr_rate():
    response = requests.get("https://api.frankfurter.dev/v1/latest", params={"base": "USD", "symbols": "IDR"}, timeout=30)
    response.raise_for_status()
    return response.json()["rates"]["IDR"]


def transform(df, rate):
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

    result.toPandas().to_csv(OUTPUT, index=False)
    print(f"Tersimpan: {OUTPUT}")
    spark.stop()
```

- `get_usd_idr_rate()` → ambil kurs 1 USD dalam IDR dari API Frankfurter. Responsnya `{"base":"USD","rates":{"IDR":18143}}`, jadi diambil dengan `["rates"]["IDR"]`.
- `F.initcap("coin_name")` → `bitcoin` jadi `Bitcoin`.
- `F.lit(rate)` → **lit** = *literal*: angka Python biasa dijadikan kolom (nilainya sama di semua baris).
- `F.round(..., 2)` → harga IDR dibulatkan 2 desimal.
- `F.current_timestamp()` → waktu proses.
- `spark.read.csv(INPUT, ...)` → membaca hasil task extract.
- `result.toPandas().to_csv(OUTPUT, ...)` → data kecil, jadi disimpan sebagai **1 file CSV** lewat pandas (kalau `df.write.csv`, hasilnya folder berisi file `part-*`).
- `spark.stop()` → wajib, supaya memori dilepas setelah task selesai.

**Buka:** `workspace/scripts/crypto/load.py`

```python
import os

import pandas as pd
from sqlalchemy import create_engine

INPUT = "/workspace/data/crypto/transform_result.csv"


def get_engine():
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "postgres")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:5432/dwh")


if __name__ == "__main__":
    df = pd.read_csv(INPUT, parse_dates=["processed_at"])
    df["loaded_at"] = pd.Timestamp.now(tz="Asia/Jakarta").tz_localize(None)
    df.to_sql("crypto_price", con=get_engine(), if_exists="append", index=False)
    print(f"{len(df)} baris ditambahkan ke dwh.crypto_price")
```

- `os.getenv(...)` → kredensial diambil dari environment container (diatur di `docker-compose.yml` / `.env`). **Jangan hardcode password** di script, apalagi kalau di-push ke GitHub publik.
- `create_engine("postgresql+psycopg2://user:pass@host:5432/dwh")` → koneksi SQLAlchemy. Host `postgres` = nama service di docker-compose.
- `parse_dates=["processed_at"]` → kolom teks dibaca sebagai tanggal.
- `pd.Timestamp.now(tz="Asia/Jakarta").tz_localize(None)` → waktu sekarang (WIB) tanpa info zona.
- `df.to_sql("crypto_price", ..., if_exists="append")` → tabel dibuat otomatis kalau belum ada; setiap run **menambah** baris (riwayat harga).

**Jalankan:** nyalakan toggle `demo_03_crypto_etl` → **▶ Trigger** → tunggu 3 kotak hijau (±30 detik, karena Spark perlu waktu start).

Di log `transform_with_spark` akan terlihat:

```
Kurs USD -> IDR: 18143
+---------+----------+------------+--------------+-----------------------+
|coin_name|coin_price|usd_idr_rate|coin_price_idr|processed_at           |
...
```

**Cek hasil** (Terminal):

```bash
docker compose exec postgres psql -U postgres -d dwh -c "SELECT coin_name, coin_price, coin_price_idr, loaded_at FROM crypto_price ORDER BY loaded_at DESC LIMIT 5;"
```

atau dari DBeaver/pgAdmin: `localhost:5433`, database `dwh`, tabel `crypto_price`.

> Coba **sengaja error**: ubah `URL` di `extract.py` jadi alamat salah, Trigger lagi, lalu lihat task berwarna kuning (*up for retry*) sebelum akhirnya merah. Kembalikan URL-nya setelah selesai.

## 6. ELT: load dulu, transform di database

| | ETL (langkah 5) | ELT (langkah 6) |
| --- | --- | --- |
| Urutan | Extract → **Transform** → Load | Extract → **Load** → Transform |
| Transform di | Python / Spark | **Database (SQL)** |
| Data mentah | tidak disimpan | disimpan di schema `raw` |
| Cocok untuk | data perlu dibersihkan sebelum masuk | data warehouse modern (Postgres, BigQuery, Snowflake) |

```
extract_load_raw  >>  transform_staging  >>  transform_mart  >>  quality_check
   CSV → raw.*        raw → staging.*       staging → mart.*     cek hasil
```

**Buka:** `workspace/dags/demo_04_sales_elt.py`

Bagian 1: fungsi bantu

```python
import os
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task

DATA_DIR = Path("/workspace/data")
SQL_DIR = Path("/workspace/scripts/elt/sql")


def get_engine():
    from sqlalchemy import create_engine

    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "postgres")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:5432/dwh")


def run_sql_file(filename):
    sql = (SQL_DIR / filename).read_text()
    with get_engine().begin() as conn:
        conn.exec_driver_sql(sql)
    print(f"Selesai menjalankan {filename}")
```

- `from airflow.sdk import dag, task` → **TaskFlow API**: cara menulis DAG dengan decorator `@dag` dan `@task` (lebih ringkas dari `PythonOperator`).
- `Path("/workspace/data")` → path sebagai objek; bisa digabung dengan `/` → `SQL_DIR / "01_staging.sql"`.
- `from sqlalchemy import create_engine` ditaruh **di dalam fungsi** → file DAG dibaca ulang oleh `dag-processor` tiap ±30 detik. Import library berat di dalam fungsi membuat pembacaan DAG tetap cepat.
- `.read_text()` → isi file SQL sebagai string.
- `get_engine().begin()` → buka transaksi: kalau satu statement gagal, semua dibatalkan (*rollback*).
- `conn.exec_driver_sql(sql)` → menjalankan semua statement SQL di file sekaligus.

Bagian 2: DAG & task

```python
@dag(
    dag_id="demo_04_sales_elt",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["demo", "elt", "sql"],
)
def sales_elt():

    @task
    def extract_load_raw():
        import pandas as pd

        engine = get_engine()
        with engine.begin() as conn:
            conn.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS raw")

        counts = {}
        for table, file in [("sales", "demo_sales.csv"), ("customers", "demo_customers.csv")]:
            df = pd.read_csv(DATA_DIR / file, dtype=str)
            df["_loaded_at"] = pd.Timestamp.now()
            df.to_sql(table, engine, schema="raw", if_exists="replace", index=False)
            counts[table] = len(df)
            print(f"raw.{table}: {len(df)} baris")
        return counts

    @task
    def transform_staging():
        run_sql_file("01_staging.sql")

    @task
    def transform_mart():
        run_sql_file("02_mart.sql")

    @task
    def quality_check(raw_counts):
        checks = {
            "stg_sales tidak kosong": "SELECT COUNT(*) > 0 FROM staging.stg_sales",
            "order_id unik": "SELECT COUNT(*) = COUNT(DISTINCT order_id) FROM staging.stg_sales",
            "mart tidak kosong": "SELECT COUNT(*) > 0 FROM mart.sales_by_city_month",
        }
        with get_engine().connect() as conn:
            for name, sql in checks.items():
                ok = conn.exec_driver_sql(sql).scalar()
                print(f"[{'OK' if ok else 'GAGAL'}] {name}")
                if not ok:
                    raise ValueError(f"Quality check gagal: {name}")
            staged = conn.exec_driver_sql("SELECT COUNT(*) FROM staging.stg_sales").scalar()
        print(f"raw.sales {raw_counts['sales']} baris -> staging.stg_sales {staged} baris (setelah cleaning)")

    raw_counts = extract_load_raw()
    raw_counts >> transform_staging() >> transform_mart() >> quality_check(raw_counts)


sales_elt()
```

- `@dag(...)` di atas `def sales_elt():` → fungsi ini menjadi DAG. Parameternya sama dengan `with DAG(...)`.
- `@task` di atas fungsi → fungsi ini menjadi 1 task; `task_id` = nama fungsinya.
- `CREATE SCHEMA IF NOT EXISTS raw` → schema = "folder" tabel di database.
- `pd.read_csv(..., dtype=str)` → semua kolom dibaca sebagai **teks**: data dimuat **apa adanya**, tanpa dibersihkan. Inilah **E + L**.
- `df["_loaded_at"]` → kolom metadata waktu load (awalan `_` = kolom teknis).
- `to_sql(..., schema="raw", if_exists="replace")` → tabel `raw.sales` dibuat ulang setiap run.
- `return counts` → otomatis jadi XCom.
- `quality_check(raw_counts)` → hasil return task lain **langsung dipakai sebagai argumen**; Airflow otomatis mengambil XCom-nya.
- `conn.exec_driver_sql(sql).scalar()` → ambil 1 nilai hasil query (`true`/`false`).
- `raise ValueError(...)` → kalau data tidak lolos, task **sengaja digagalkan** supaya ketahuan.
- `raw_counts >> transform_staging() >> ...` → urutan task. Memanggil fungsi `@task` = membuat task di DAG (bukan langsung menjalankannya).
- `sales_elt()` di baris terakhir → **wajib**; tanpa ini DAG tidak terdaftar.

**Buka:** `workspace/scripts/elt/sql/01_staging.sql` (potongan)

```sql
CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.stg_sales;
CREATE TABLE staging.stg_sales AS
SELECT DISTINCT ON ("OrderID")
       "OrderID"                                    AS order_id,
       "OrderDate"::date                            AS order_date,
       "CustomerID"                                 AS customer_id,
       INITCAP(TRIM("Product"))                     AS product,
       "Category"                                   AS category,
       "Quantity"::int                              AS quantity,
       "UnitPrice"::bigint                          AS unit_price,
       "Quantity"::int * "UnitPrice"::bigint        AS total_price,
       COALESCE("PaymentMethod", 'Unknown')         AS payment_method
FROM raw.sales
WHERE "Quantity" IS NOT NULL
ORDER BY "OrderID";
```

- Inilah **T** pada ELT: transformasi ditulis dalam SQL dan dijalankan **oleh database**.
- `DROP TABLE IF EXISTS` + `CREATE TABLE ... AS SELECT` → tabel dibuat ulang setiap run (*idempotent*: dijalankan berkali-kali hasilnya tetap sama).
- `DISTINCT ON ("OrderID")` (khusus PostgreSQL) → ambil 1 baris per OrderID = hapus duplikat.
- `"OrderID"` pakai kutip ganda → nama kolom dari CSV ada huruf besarnya.
- `::date`, `::int`, `::bigint` → ubah tipe teks ke tipe sebenarnya (*cast*).
- `AS order_id` → ganti nama kolom ke `snake_case`.
- `INITCAP(TRIM(...))` → rapikan teks (`LAPTOP` → `Laptop`).
- `COALESCE(x, 'Unknown')` → kalau `x` null, pakai `'Unknown'`.
- `WHERE "Quantity" IS NOT NULL` → buang baris tanpa quantity.

**Buka:** `workspace/scripts/elt/sql/02_mart.sql` (potongan)

```sql
CREATE TABLE mart.sales_by_city_month AS
SELECT c.city,
       TO_CHAR(s.order_date, 'YYYY-MM')   AS order_month,
       COUNT(*)                           AS total_orders,
       SUM(s.quantity)                    AS total_qty,
       SUM(s.total_price)                 AS revenue
FROM staging.stg_sales s
JOIN staging.stg_customers c USING (customer_id)
GROUP BY c.city, TO_CHAR(s.order_date, 'YYYY-MM')
ORDER BY order_month, revenue DESC;
```

- **Mart** = tabel ringkasan siap pakai untuk dashboard/analisis.
- `JOIN ... USING (customer_id)` → join dengan kolom yang namanya sama di kedua tabel.
- `TO_CHAR(date, 'YYYY-MM')` → tanggal jadi teks bulan.

**Jalankan:** nyalakan toggle `demo_04_sales_elt` → **▶ Trigger**. Di log `quality_check`:

```
[OK] stg_sales tidak kosong
[OK] order_id unik
[OK] mart tidak kosong
raw.sales 301 baris -> staging.stg_sales 292 baris (setelah cleaning)
```

**Cek hasil** (Terminal):

```bash
docker compose exec postgres psql -U postgres -d dwh -c "SELECT * FROM mart.top_products WHERE rank_in_category = 1;"
```

```
  category   | product  |  revenue   | rank_in_category
-------------+----------+------------+------------------
 Electronics | Laptop   | 1216000000 |                1
 Fashion     | Backpack |   28700000 |                1
 Food        | Coffee   |    9165000 |                1
 Stationery  | Notebook |    2600000 |                1
```

Di DBeaver/pgAdmin terlihat 3 schema: `raw` (mentah), `staging` (bersih), `mart` (ringkasan).

## 7. Contoh slide versi Airflow 3

Kode di slide (`AirflowCSV.py`, `AirflowClean.py`) memakai Airflow 2 dan **akan error** di lab ini:

| Airflow 2 (slide) | Airflow 3 (lab ini) |
| --- | --- |
| `from airflow import DAG` | `from airflow.sdk import DAG` |
| `from airflow.operators.bash_operator import BashOperator` | `from airflow.providers.standard.operators.bash import BashOperator` |
| `from airflow.operators.python_operator import PythonOperator` | `from airflow.providers.standard.operators.python import PythonOperator` |
| `schedule_interval=timedelta(minutes=5)` | `schedule=timedelta(minutes=5)` |
| `/opt/airflow/dags/data.csv` | `/workspace/data/data.csv` |
| `sudo -u airflow python script.py` | `python /workspace/scripts/script.py` |

**Buka:** `workspace/dags/demo_05_csv_clean_slide.py` (potongan)

```python
def csvToJson():
    df = pd.read_csv(f"{DATA}/demo_customers.csv")
    for i, r in df.head(5).iterrows():
        print(r["CustomerName"])
    df.to_json(f"{OUT}/fromAirflow.json", orient="records")


def cleanSales():
    df = pd.read_csv(f"{DATA}/demo_sales.csv")
    df.drop(columns=["PaymentMethod"], inplace=True)
    df.columns = [x.lower() for x in df.columns]
    df["orderdate"] = pd.to_datetime(df["orderdate"], format="%Y-%m-%d")
    df.dropna(inplace=True)
    df.drop_duplicates(subset="orderid", inplace=True)
    df.to_csv(f"{OUT}/cleansales.csv", index=False)


def filterData():
    df = pd.read_csv(f"{OUT}/cleansales.csv")
    fromd, tod = "2024-05-23", "2024-06-03"
    tofrom = df[(df["orderdate"] > fromd) & (df["orderdate"] < tod)]
    tofrom.to_csv(f"{OUT}/may23-june3.csv", index=False)


default_args = {
    "owner": "raka",
    "start_date": dt.datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

with DAG(
    "demo_05_csv_clean_slide",
    default_args=default_args,
    schedule=timedelta(minutes=60),
    catchup=False,
) as dag:

    print_starting = BashOperator(
        task_id="starting",
        bash_command=f'mkdir -p {OUT} && echo "I am reading the CSV now....."',
    )

    csvJson = PythonOperator(task_id="convertCSVtoJson", python_callable=csvToJson)

    cleanData = PythonOperator(task_id="clean", python_callable=cleanSales)

    selectData = PythonOperator(task_id="filter", python_callable=filterData)

    copyFile = BashOperator(
        task_id="copy",
        bash_command=f"cp {OUT}/may23-june3.csv {OUT}/may23-june3_copy.csv && ls -l {OUT}",
    )

    print_starting >> csvJson
    print_starting >> cleanData >> selectData >> copyFile
```

- `csvToJson` (= `H8CSVDAG` di slide) → baca CSV, print 5 nama, simpan ke JSON. `orient="records"` → format `[{...}, {...}]`.
- `cleanSales` (= `cleanScooter` di slide) → drop kolom, nama kolom jadi huruf kecil, ubah ke tipe tanggal, buang baris kosong & duplikat.
- `filterData` → ambil transaksi antara 23 Mei – 3 Juni 2024. `&` = AND untuk kondisi pandas (tiap kondisi wajib dalam kurung).
- `"start_date"` boleh ditaruh di `default_args` seperti di slide.
- `with DAG("demo_05_csv_clean_slide", ...)` → argumen pertama = `dag_id`.
- `mkdir -p {OUT}` → buat folder output dulu sebelum task lain menulis ke sana.
- Dua baris dependency terakhir → `print_starting` bercabang ke 2 alur: `csvJson`, dan `clean → filter → copy`.

**Jalankan:** Trigger, lalu **cek di file explorer:** `workspace/data/output/` berisi `fromAirflow.json`, `cleansales.csv`, `may23-june3.csv`, `may23-june3_copy.csv`.

## 8. Buat DAG sendiri

**Buat file:** `workspace/dags/latihan_dag_saya.py`

```python
from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def hitung_baris():
    import pandas as pd

    df = pd.read_csv("/workspace/data/demo_sales.csv")
    print(f"Jumlah transaksi: {len(df)}")
    return len(df)


with DAG(
    dag_id="latihan_dag_saya",
    start_date=datetime(2025, 1, 1),
    schedule="0 7 * * 1-5",
    catchup=False,
    tags=["latihan"],
) as dag:

    mulai = BashOperator(task_id="mulai", bash_command="echo 'Mulai pipeline'")
    hitung = PythonOperator(task_id="hitung_baris", python_callable=hitung_baris)
    selesai = BashOperator(task_id="selesai", bash_command="echo 'Pipeline selesai'")

    mulai >> hitung >> selesai
```

- `schedule="0 7 * * 1-5"` → Senin–Jumat jam 07:00 WIB.
- `import pandas as pd` di dalam fungsi → supaya pembacaan file DAG tetap cepat.

**Langkah:**

1. Save file → tunggu ±30 detik → refresh UI, DAG `latihan_dag_saya` muncul.
2. Kalau tidak muncul, cek error import:

    ```bash
    docker compose exec lab airflow dags list-import-errors
    ```

    `No data found` = tidak ada error.
3. Test tanpa UI:

    ```bash
    docker compose exec lab airflow dags test latihan_dag_saya
    ```

4. Trigger di UI → cek log `hitung_baris` → `Jumlah transaksi: 301`.

**Tantangan:**

- Tambahkan task paralel (fan-out) setelah `mulai`.
- Ubah schedule menjadi setiap Sabtu jam 09:10–09:30 per 10 menit.
- Tambah `default_args` dengan `retries: 3`.

---

## Ringkasan

| Perintah / Konsep | Fungsi |
| --- | --- |
| `with DAG(...)` / `@dag` | Membuat DAG |
| `BashOperator(bash_command=...)` | Task menjalankan perintah terminal / script |
| `PythonOperator(python_callable=...)` | Task menjalankan fungsi Python |
| `@task` | Task gaya TaskFlow API |
| `a >> b`, `a >> [b, c] >> d` | Urutan & paralel |
| `schedule`, `start_date`, `catchup` | Penjadwalan |
| `default_args` (`retries`, `retry_delay`, `owner`) | Setting semua task |
| `return` / `ti.xcom_pull()` | Kirim data kecil antar task (XCom) |
| `airflow dags test <dag_id>` | Jalankan 1 run di terminal |
| `airflow tasks test <dag_id> <task_id>` | Jalankan 1 task di terminal |
| `airflow dags list-import-errors` | Cek file DAG yang error |

▎ ***DAG** = resep, **task** = langkah memasak, **operator** = jenis alat (Bash/Python), **scheduler** = alarm yang menyalakan resep sesuai jadwal, **XCom** = catatan kecil antar langkah, **ETL** = masak dulu baru disajikan, **ELT** = bahan masuk kulkas dulu, dimasak di dapur database.*
