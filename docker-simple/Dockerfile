# Dockerfile = RESEP untuk membuat image. Dibaca dari atas ke bawah.

# 1. Mulai dari image yang sudah ada: Linux + Python 3.13
FROM python:3.13-slim

# 2. Install Java (Spark butuh Java)
RUN apt-get update && apt-get install -y openjdk-21-jre-headless

# 3. Install library Python
RUN pip install apache-airflow pyspark pandas psycopg2-binary

# 4. Setting: lokasi folder DAG
ENV AIRFLOW__CORE__DAGS_FOLDER=/workspace/dags

# 5. Folder kerja di dalam container
WORKDIR /workspace

# 6. Perintah yang dijalankan saat container nyala
CMD ["airflow", "standalone"]
