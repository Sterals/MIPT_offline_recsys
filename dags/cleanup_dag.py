from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

DATA_DIR = "/opt/airflow/data"
RETENTION_DAYS = 7

default_args = {
    "owner": "recsys",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cleanup_daily",
    default_args=default_args,
    description="Wait for popular_daily, then remove stale CSVs",
    schedule_interval="0 6 * * *",   # тот же schedule, что и у popular_daily, иначе execution_delta
    start_date=datetime(2021, 6, 1),
    end_date=datetime(2021, 6, 30),
    catchup=True,
    max_active_runs=1,
    tags=["recsys", "cleanup"],
) as dag:


    wait_for_popular = ExternalTaskSensor(
        task_id="wait_for_popular_daily",
        external_dag_id="popular_daily",
        external_task_id=None,         # None = ждём завершения всего DAG
        mode="poke",                   # poke = проверяет с интервалом, держа worker
        poke_interval=30,              # проверять каждые 30 секунд
        timeout=3600,                  # максимум ждать 1 час
        allowed_states=["success"],    # считаем завершённым только при success
        failed_states=["failed"],      # если popular_daily упал — сразу fail
    )


    cleanup = BashOperator(
        task_id="remove_stale_csvs",
        bash_command="""
            set -euo pipefail

            CUTOFF_DATE="{{ macros.ds_add(ds, -""" + str(RETENTION_DAYS) + """) }}"
            DATA_DIR=""" + DATA_DIR + """

            echo "ds={{ ds }}, cutoff=${CUTOFF_DATE}"
            echo "Scanning ${DATA_DIR} for stale CSVs..."

            deleted=0
            for f in "${DATA_DIR}"/top_popular_*.csv; do
                [ -f "$f" ] || continue

                # Извлекаем дату из имени: top_popular_2021-06-01.csv
                # или top_popular_age_18_2021-06-01.csv
                file_date=$(echo "$f" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}')

                if [ -z "$file_date" ]; then
                    echo "SKIP (no date): $f"
                    continue
                fi

                if [ "$file_date" "<" "$CUTOFF_DATE" ]; then
                    echo "DELETE: $f (date=${file_date} < cutoff=${CUTOFF_DATE})"
                    rm "$f"
                    deleted=$((deleted + 1))
                fi
            done

            echo "Done. Deleted ${deleted} file(s)."
        """,
    )

    wait_for_popular >> cleanup
