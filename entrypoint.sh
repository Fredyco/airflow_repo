#!/usr/bin/env bash
# Menjalankan Jupyter dan/atau Airflow di dalam satu container.
#   SERVICES=all      -> Jupyter + Airflow (default)
#   SERVICES=jupyter  -> hanya Jupyter (paling hemat RAM, cukup untuk GC6)
#   SERVICES=airflow  -> hanya Airflow
# Argumen lain (mis. `docker compose run lab python x.py`) dijalankan langsung.
set -euo pipefail

if [[ $# -gt 0 ]]; then
  exec "$@"
fi

SERVICES="${SERVICES:-all}"
mkdir -p /workspace/dags /workspace/notebooks /workspace/data

start_airflow() {
  echo "[lab] Menunggu database Airflow siap..."
  for i in $(seq 1 30); do
    airflow db check >/dev/null 2>&1 && break
    if [[ $i -eq 30 ]]; then echo "[lab] Database Airflow tidak bisa diakses:"; airflow db check; exit 1; fi
    sleep 2
  done
  airflow db migrate >/dev/null
  echo "[lab] Menjalankan Airflow (scheduler, dag-processor, api-server) di port 8080"
  mkdir -p "$AIRFLOW_HOME/logs"
  airflow scheduler     >>"$AIRFLOW_HOME/logs/scheduler.out" 2>&1 &
  airflow dag-processor >>"$AIRFLOW_HOME/logs/dag-processor.out" 2>&1 &
  airflow api-server --port 8080 >>"$AIRFLOW_HOME/logs/api-server.out" 2>&1 &
}

start_jupyter() {
  echo "[lab] Menjalankan JupyterLab di port 8888"
  jupyter lab --ip=0.0.0.0 --port=8888 --no-browser \
    --ServerApp.root_dir=/workspace \
    --IdentityProvider.token='' --ServerApp.password='' \
    --ServerApp.disable_check_xsrf=True &
}

case "$SERVICES" in
  all)     start_airflow; start_jupyter ;;
  jupyter) start_jupyter ;;
  airflow) start_airflow ;;
  *) echo "SERVICES tidak dikenal: $SERVICES (pilih all|jupyter|airflow)"; exit 1 ;;
esac

echo "[lab] Siap. Jupyter: http://localhost:8888  Airflow: http://localhost:8080"
# Container berhenti jika salah satu proses utama mati
wait -n
