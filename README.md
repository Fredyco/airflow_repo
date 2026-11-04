# DE Lab — PySpark & Airflow

Python 3.13 · PySpark 4.2 · Airflow 3.3 · Great Expectations 1.24.1 · pymongo · JupyterLab

## Persiapan (sekali saja)

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) dan buka sampai **Engine running**.
2. `git clone https://github.com/Fredyco/airflow_repo.git` (atau **Code → Download ZIP**).
3. Salin `.env.example` menjadi `.env`, lalu isi `PG_PASSWORD` dengan password pgAdmin kamu.
4. Di pgAdmin, jalankan `workspace/sql/01_create_database.sql` lalu `02_create_tables.sql` (di database `dwh`).

## Menjalankan

```bash
docker compose up -d
```

- Jupyter: http://localhost:8888
- Airflow: http://localhost:8080

| Perintah | Fungsi |
| --- | --- |
| `docker compose up -d` | Menyalakan (jalankan lagi setelah mengubah `.env`) |
| `docker compose stop` / `start` | Mematikan / menyalakan lagi |
| `docker compose down` | Menghapus container (file di `workspace/` tetap aman) |
| `docker compose logs lab` | Melihat log jika error |

## Isi

```
workspace/
├── notebooks/
│   ├── 01_pandas_vs_spark.ipynb   # perbandingan pandas & PySpark
│   └── 02_etl_superstore.ipynb    # ETL Superstore → star schema
├── sql/                           # buat database & tabel (jalankan di pgAdmin)
├── data/                          # file CSV
├── dags/                          # DAG Airflow
│   ├── demo_01_hello.py
│   ├── demo_02_crypto_etl.py      # ETL: API → Spark → PostgreSQL
│   ├── demo_03_sales_elt.py       # ELT: CSV → raw_sales → SQL → clean_sales
│   └── demo_04_superstore_etl.py  # notebook 02 dijadikan pipeline Spark
└── scripts/                       # script yang dipanggil DAG (crypto/, elt/, superstore/)
docker-simple/                     # Dockerfile & docker-compose sederhana, hanya untuk dibaca
```

Semua DAG demo memakai pola yang sama: setiap task = `BashOperator` yang menjalankan `python scripts/.../x.py`.
Demo 04 butuh tabel dari `sql/01_create_database.sql` & `sql/02_create_tables.sql`; setiap run mengosongkan tabel lalu mengisi ulang.

**docker-simple/** berisi Dockerfile & docker-compose versi minimal dengan komentar per baris, **hanya untuk belajar cara membacanya** (tidak untuk dijalankan).
Untuk dipakai sehari-hari gunakan `docker-compose.yml` di root.

Dari dalam container, PostgreSQL di laptop diakses dengan host `host.docker.internal` (sudah diatur di `.env`).

**Tanpa Docker?** Materi PySpark versi conda + cheat sheet pandas ↔ Spark ada di repo [Fredyco/pyspark](https://github.com/Fredyco/pyspark).
