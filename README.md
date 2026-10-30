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
└── scripts/                       # script yang dipanggil DAG
```

Dari dalam container, PostgreSQL di laptop diakses dengan host `host.docker.internal` (sudah diatur di `.env`).

**Tanpa Docker?** Materi PySpark versi conda + cheat sheet pandas ↔ Spark ada di repo [Fredyco/pyspark](https://github.com/Fredyco/pyspark).
