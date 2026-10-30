# syntax=docker/dockerfile:1
# Image all-in-one untuk tugas GC6 & M3: Python 3.13 + PySpark + Airflow + Great Expectations + pymongo + Jupyter
ARG PYTHON_VERSION=3.13
ARG AIRFLOW_VERSION=3.3.2
ARG PG_JDBC_VERSION=42.7.14

# ---------- Stage 1: build virtualenv ----------
FROM python:${PYTHON_VERSION}-slim AS builder
ARG PYTHON_VERSION
ARG AIRFLOW_VERSION
ARG PG_JDBC_VERSION

ENV UV_NO_CACHE=1 UV_COMPILE_BYTECODE=1 VIRTUAL_ENV=/opt/venv PATH=/opt/venv/bin:$PATH
COPY --from=ghcr.io/astral-sh/uv:0.13.6 /uv /usr/local/bin/uv
RUN uv venv --seed /opt/venv

ADD --chmod=644 https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt /tmp/constraints.txt
COPY requirements.txt /tmp/requirements.txt
# constraints Airflow mem-pin pandas 3; lepas pin itu agar bisa pakai pandas 2.x (kompatibel PySpark)
RUN sed -i '/^pandas==/d' /tmp/constraints.txt
# Satu kali resolve: Airflow + paket tugas, mengikuti constraints resmi Airflow
RUN uv pip install \
        "apache-airflow==${AIRFLOW_VERSION}" \
        apache-airflow-providers-standard \
        -r /tmp/requirements.txt \
        --constraint /tmp/constraints.txt

# JDBC driver PostgreSQL supaya PySpark bisa write.jdbc tanpa download saat runtime
ADD --chmod=644 https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar /tmp/
RUN SPARK_HOME=$(python -c "import pyspark,os;print(os.path.dirname(pyspark.__file__))") \
    && cp /tmp/postgresql-${PG_JDBC_VERSION}.jar "$SPARK_HOME/jars/" \
    # pangkas file yang tidak dipakai agar image lebih ringan
    && rm -rf "$SPARK_HOME/examples" "$SPARK_HOME/data" "$SPARK_HOME/licenses" \
    && find /opt/venv/lib -depth -type d -name 'tests' \
         -not -path '*/pyspark/*' -not -path '*/great_expectations/*' -not -path '*/airflow/*' -exec rm -rf {} +

# ---------- Stage 2: runtime ----------
FROM python:${PYTHON_VERSION}-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless tini procps \
    && rm -rf /var/lib/apt/lists/* \
    # path JAVA_HOME yang sama untuk arm64 maupun amd64
    && ln -s /usr/lib/jvm/java-21-openjdk-* /usr/lib/jvm/default-java \
    && useradd -m -u 1000 -s /bin/bash student \
    && mkdir -p /opt/airflow/logs /workspace /opt/spark-conf \
    && chown -R student:student /opt/airflow /workspace

COPY --from=builder --chown=student:student /opt/venv /opt/venv
COPY spark-defaults.conf /opt/spark-conf/spark-defaults.conf
COPY --chmod=755 entrypoint.sh /entrypoint.sh
# Buang CR (line ending Windows) jika file di-clone/di-edit di Windows
RUN sed -i 's/\r$//' /entrypoint.sh /opt/spark-conf/spark-defaults.conf

ENV PATH=/opt/venv/bin:$PATH \
    VIRTUAL_ENV=/opt/venv \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Jakarta \
    JAVA_HOME=/usr/lib/jvm/default-java \
    SPARK_CONF_DIR=/opt/spark-conf \
    PYSPARK_PYTHON=python \
    AIRFLOW_HOME=/opt/airflow \
    AIRFLOW__CORE__DAGS_FOLDER=/workspace/dags \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__CORE__PARALLELISM=2 \
    AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Jakarta \
    AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True \
    AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://localhost:8080/execution/ \
    AIRFLOW__API__WORKERS=1 \
    AIRFLOW__API_AUTH__JWT_SECRET=local-student-lab-secret \
    AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE=True \
    AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=30 \
    AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE=Asia/Jakarta

USER student
WORKDIR /workspace
EXPOSE 8888 8080 4040
ENTRYPOINT ["tini", "--", "/entrypoint.sh"]
