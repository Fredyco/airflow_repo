# PySpark tanpa Docker (conda)

Materi PySpark yang sama dengan versi Docker, tapi dijalankan langsung dengan conda. Cocok kalau laptop berat menjalankan Docker.

| | Docker (`workspace/`) | Conda (folder ini) |
| --- | --- | --- |
| Isi | PySpark + Airflow + Jupyter | PySpark + Jupyter saja |
| Ukuran install | ±1,5 GB image + Docker Desktop | ±1 GB di env conda |
| Waktu install | 5–15 menit | ±3 menit |
| Java | sudah di dalam image | dipasang lewat conda |

## Peta folder

```
pyspark-conda/
├── requirements.txt              ← langkah 1
├── .env.example → .env           ← langkah 2
├── sql/
│   ├── 01_create_database.sql    ← langkah 3
│   ├── 02_create_tables.sql      ← langkah 3
│   └── 03_check.sql              ← langkah 5
├── data/                         ← 3 file CSV Superstore
└── notebooks/
    ├── 01_pandas_vs_spark.ipynb  ← langkah 4
    └── 02_etl_superstore.ipynb   ← langkah 4
```

## 1. Buat env & install

**Terminal** (di folder `pyspark-conda`):

```bash
conda create -n pyspark313 -c conda-forge python=3.13 openjdk=21 -y
conda activate pyspark313
pip install -r requirements.txt

java -version
```

- `openjdk=21` → Java, wajib untuk Spark. Dipasang di dalam env, jadi tidak perlu install Java manual.
- `java -version` berhasil kalau muncul `openjdk version "21..."`.

> Sudah punya env Python 3.13 (misalnya `dbt313`)? Boleh dipakai juga:
> `conda activate dbt313`, lalu `conda install -c conda-forge openjdk=21 -y` dan `pip install -r requirements.txt`.

## 2. Isi `.env`

```bash
cp .env.example .env              # Mac / Linux
Copy-Item .env.example .env       # Windows (PowerShell)
```

Buka `.env`, isi `PG_PASSWORD` dengan password pgAdmin. `PG_HOST=localhost` karena notebook jalan langsung di laptop.

## 3. Buat database & tabel (pgAdmin)

1. Klik kanan server → *Query Tool* (database `postgres`) → buka `sql/01_create_database.sql` → jalankan.
2. Klik kanan *Databases* → *Refresh* → pilih `dwh` → *Query Tool* → buka `sql/02_create_tables.sql` → jalankan.

## 4. Jalankan notebook

Pilih salah satu cara.

**A. VS Code (disarankan)**

1. Install extension **Python** dan **Jupyter** di VS Code.
2. *File → Open Folder* → pilih folder `pyspark-conda`.
3. Buka `notebooks/01_pandas_vs_spark.ipynb`.
4. Klik **Select Kernel** (kanan atas) → *Python Environments* → pilih **`pyspark313`**.
   Env tidak muncul? `Ctrl/Cmd + Shift + P` → *Python: Select Interpreter* → pilih `pyspark313`, lalu ulangi.
5. Jalankan cell dengan `Shift + Enter`.

**B. JupyterLab di browser**

**Terminal** (di folder `pyspark-conda`, env sudah aktif):

```bash
jupyter lab
```

Masuk folder `notebooks`. Tekan `Ctrl + C` di terminal untuk mematikan Jupyter.

Jalankan berurutan:

1. `01_pandas_vs_spark.ipynb` → syntax pandas vs Spark.
2. `02_etl_superstore.ipynb` → ETL ke PostgreSQL. Saat pertama jalan, driver PostgreSQL di-download otomatis (butuh internet).

## 5. Cek hasil

Di pgAdmin, database `dwh` → *Query Tool* → buka `sql/03_check.sql` → jalankan.
Untuk mengulang dari awal: jalankan lagi `02_create_tables.sql` (tabel dikosongkan), lalu jalankan ulang notebook 02.

## Kalau ada masalah

| Masalah | Solusi |
| --- | --- |
| `JAVA_GATEWAY_EXITED` / Java tidak ditemukan | Pastikan kernel/env yang dipilih `pyspark313` dan Java sudah terpasang di env itu: `conda activate pyspark313` → `java -version`. Kalau tidak ada: `conda install -c conda-forge openjdk=21 -y`, lalu restart kernel |
| `jupyter: command not found` | Env belum aktif. `conda activate pyspark313` dulu |
| VS Code: kernel `pyspark313` tidak ada | Pastikan extension Python & Jupyter terpasang, lalu *Python: Select Interpreter* → `pyspark313` |
| `Belum diisi di file .env` | Buat `.env` dari `.env.example`, lalu di Jupyter: *Kernel → Restart Kernel* |
| `Password salah` | Samakan `PG_PASSWORD` di `.env` dengan password pgAdmin, lalu restart kernel |
| Gagal download driver PostgreSQL | Butuh internet saat pertama kali menjalankan notebook 02 |
| Windows: pesan tentang `winutils.exe` / `HADOOP_HOME` | Peringatan ini boleh diabaikan; notebook ini tidak menulis file ke disk |

## Ringkasan perintah

| Perintah | Fungsi |
| --- | --- |
| `conda activate pyspark313` | Aktifkan env (setiap buka terminal baru) |
| `jupyter lab` | Buka Jupyter |
| `java -version` | Cek Java |
| `conda deactivate` | Keluar dari env |
