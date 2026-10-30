# DE Lab: Python 3.13 + Java (untuk Spark) + library di requirements.txt
FROM python:3.13-slim

# 1. Java untuk Spark, tini untuk menjalankan Jupyter & Airflow bersamaan, user "student"
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-21-jre-headless tini procps \
 && rm -rf /var/lib/apt/lists/* \
 && useradd -m -u 1000 student \
 && mkdir -p /opt/airflow/logs /workspace \
 && chown -R student /opt/airflow /workspace

# 2. Library Python. Versi mengikuti daftar resmi Airflow (constraints) supaya tidak bentrok
ARG AIRFLOW_VERSION=3.3.2
COPY --from=ghcr.io/astral-sh/uv:0.13.6 /uv /usr/local/bin/uv
ADD https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.13.txt /tmp/constraints.txt
COPY requirements.txt /tmp/requirements.txt
RUN sed -i '/^pandas==/d' /tmp/constraints.txt \
 && uv venv --seed /opt/venv \
 && VIRTUAL_ENV=/opt/venv UV_NO_CACHE=1 UV_COMPILE_BYTECODE=1 uv pip install \
      "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-standard \
      -r /tmp/requirements.txt --constraint /tmp/constraints.txt \
 && find /opt/venv -type d -name tests -not -path "*/great_expectations/*" -prune -exec rm -rf {} + \
 && chown -R student /opt/venv

# 3. Driver PostgreSQL untuk Spark (df.write.jdbc)
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar \
    /opt/venv/lib/python3.13/site-packages/pyspark/jars/

# 4. Setting Spark & script start. sed: buang line ending Windows (CRLF) kalau ada
COPY spark-defaults.conf /opt/spark-conf/spark-defaults.conf
COPY --chmod=755 entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh /opt/spark-conf/spark-defaults.conf

# 5. Setting lingkungan
ENV PATH=/opt/venv/bin:$PATH \
    TZ=Asia/Jakarta \
    SPARK_CONF_DIR=/opt/spark-conf \
    AIRFLOW_HOME=/opt/airflow \
    AIRFLOW__CORE__DAGS_FOLDER=/workspace/dags \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Jakarta \
    AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True \
    AIRFLOW__CORE__PARALLELISM=2 \
    AIRFLOW__API__WORKERS=1 \
    AIRFLOW__API_AUTH__JWT_SECRET=de-lab \
    AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=30

USER student
WORKDIR /workspace
EXPOSE 8888 8080 4040
ENTRYPOINT ["tini", "--", "/entrypoint.sh"]
