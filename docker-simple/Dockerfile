# Versi sederhana untuk penjelasan: Python + Java (Spark) + Airflow + Jupyter
FROM python:3.13-slim

# 1. Java dibutuhkan Spark
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
 && rm -rf /var/lib/apt/lists/*

# 2. Library Python (constraints = daftar versi resmi Airflow supaya tidak bentrok)
RUN pip install --no-cache-dir "apache-airflow==3.3.2" apache-airflow-providers-standard \
      pyspark==4.2.1 pandas psycopg2-binary sqlalchemy jupyterlab \
      --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.13.txt

# 3. Driver PostgreSQL untuk Spark (df.write.jdbc)
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar \
    /usr/local/lib/python3.13/site-packages/pyspark/jars/

# 4. Setting Airflow
ENV AIRFLOW_HOME=/opt/airflow \
    AIRFLOW__CORE__DAGS_FOLDER=/workspace/dags \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True

WORKDIR /workspace
EXPOSE 8080 8888
