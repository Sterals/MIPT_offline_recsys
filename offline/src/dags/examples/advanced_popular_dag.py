"""
ДЕМОНСТРАЦИОННЫЙ DAG: продвинутые фичи Airflow
================================================
Этот DAG НЕ предназначен для запуска — он показывает студентам
дополнительные возможности Airflow на примере RecSys-пайплайна.

Фичи:
    1. FileSensor — ждём появления файла с данными
    2. TaskGroup — группируем связанные таски
    3. BranchPythonOperator — условная логика (деплоить или нет)
    4. Trigger Rules — таск, который выполняется в любом случае
    5. Callbacks — уведомления при падении
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import redis
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

DATA_DIR = Path("/opt/airflow/data")
TOP_K = 10


# ===========================================================================
# Callbacks — вызываются при падении таска
# ===========================================================================
def on_failure_callback(context):
    """
    Вызывается автоматически при падении любого таска (если указан в default_args).
    В реальном проекте здесь можно отправить алерт в Slack, Telegram, PagerDuty.
    """
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    ds = context["ds"]
    print(f"ALERT: Task '{task_id}' in DAG '{dag_id}' failed for ds={ds}")
    # В реальности: requests.post("https://slack.com/api/chat.postMessage", ...)


# ===========================================================================
# Task functions
# ===========================================================================
def build_popular(**context):
    ds = context["ds"]
    end_date = datetime.strptime(ds, "%Y-%m-%d").date()
    start_date = end_date - timedelta(days=14)

    interactions = pd.read_csv(DATA_DIR / "interactions.csv", parse_dates=["last_watch_dt"])
    mask = (interactions["last_watch_dt"].dt.date > start_date) & (
        interactions["last_watch_dt"].dt.date <= end_date
    )
    window = interactions.loc[mask]

    top = (
        window.groupby("item_id")
        .agg(n_users=("user_id", "nunique"), total_watches=("user_id", "count"))
        .sort_values("n_users", ascending=False)
        .head(TOP_K)
        .reset_index()
    )
    top["rank"] = range(1, len(top) + 1)

    output_path = DATA_DIR / f"top_popular_{ds}.csv"
    top.to_csv(output_path, index=False)
    return str(output_path)


def validate(**context):
    csv_path = context["ti"].xcom_pull(task_ids="compute.build_popular")
    df = pd.read_csv(csv_path)
    assert len(df) == TOP_K, f"Expected {TOP_K} rows, got {len(df)}"
    assert df["item_id"].is_unique, "Duplicate item_id"
    return csv_path


# ===========================================================================
# Branching: решаем, деплоить ли новые рекомендации
# ===========================================================================
def should_deploy(**context):
    """
    BranchPythonOperator вызывает эту функцию.
    Она должна вернуть task_id следующего таска.

    Логика: если top-10 изменился по сравнению с прошлым запуском — деплоим.
    Если нет — пропускаем загрузку в Redis.
    """
    csv_path = context["ti"].xcom_pull(task_ids="quality.validate")
    new_items = set(pd.read_csv(csv_path)["item_id"].tolist())

    r = redis.Redis(
        host=Variable.get("REDIS_HOST", default_var="redis"),
        port=int(Variable.get("REDIS_PORT", default_var="6379")),
        password=Variable.get("REDIS_PASSWORD", default_var="recsys_redis_pass"),
        decode_responses=True,
    )

    raw = r.get("popular")
    if raw is None:
        print("[should_deploy] No existing data in Redis → DEPLOY")
        return "deploy.load_to_redis"

    old_items = set(json.loads(raw))

    if new_items != old_items:
        diff = new_items.symmetric_difference(old_items)
        print(f"[should_deploy] {len(diff)} items changed → DEPLOY")
        return "deploy.load_to_redis"
    else:
        print("[should_deploy] No changes → SKIP")
        return "deploy.skip_deploy"


def load_to_redis(**context):
    csv_path = context["ti"].xcom_pull(task_ids="quality.validate")
    df = pd.read_csv(csv_path)
    item_ids = df["item_id"].astype(str).tolist()

    r = redis.Redis(
        host=Variable.get("REDIS_HOST", default_var="redis"),
        port=int(Variable.get("REDIS_PORT", default_var="6379")),
        password=Variable.get("REDIS_PASSWORD", default_var="recsys_redis_pass"),
        decode_responses=True,
    )
    r.set("popular", json.dumps(item_ids))
    print(f"[load_to_redis] Updated {len(item_ids)} items")


def log_completion(**context):
    ds = context["ds"]
    print(f"[log_completion] DAG run for ds={ds} finished")


# ===========================================================================
# DAG definition
# ===========================================================================
default_args = {
    "owner": "recsys",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_failure_callback,  # Callback при падении
}

with DAG(
    dag_id="advanced_popular_daily",
    default_args=default_args,
    description="Popular items with sensor, branching, and task groups",
    schedule_interval="0 6 * * *",
    start_date=datetime(2021, 6, 1),
    end_date=datetime(2021, 6, 30),
    catchup=True,
    max_active_runs=1,
    tags=["recsys", "popular", "advanced"],
) as dag:

    # -----------------------------------------------------------------------
    # 1. FileSensor — ждём появления файла с данными
    # -----------------------------------------------------------------------
    # В реальности данные могут приходить из ETL-пайплайна с задержкой.
    # Sensor будет проверять наличие файла каждые 60 секунд,
    # максимум 2 часа (timeout). Если файла нет — таск упадёт.
    wait_for_data = FileSensor(
        task_id="wait_for_data",
        filepath="/opt/airflow/data/interactions.csv",
        poke_interval=60,          # проверять каждые 60 сек
        timeout=7200,              # максимум ждать 2 часа
        mode="poke",               # poke = держит worker, reschedule = освобождает
        soft_fail=False,           # при timeout → FAILED (не SKIPPED)
    )

    # -----------------------------------------------------------------------
    # 2. TaskGroup "compute" — группа вычислительных тасков
    # -----------------------------------------------------------------------
    # TaskGroup визуально группирует связанные таски в UI.
    # Это удобно когда в DAG много тасков.
    with TaskGroup("compute", tooltip="Build popular items") as compute_group:
        build = PythonOperator(
            task_id="build_popular",
            python_callable=build_popular,
        )

    # -----------------------------------------------------------------------
    # 3. TaskGroup "quality" — валидация
    # -----------------------------------------------------------------------
    with TaskGroup("quality", tooltip="Validate results") as quality_group:
        check = PythonOperator(
            task_id="validate",
            python_callable=validate,
        )

    # -----------------------------------------------------------------------
    # 4. BranchPythonOperator — условная логика
    # -----------------------------------------------------------------------
    branch = BranchPythonOperator(
        task_id="should_deploy",
        python_callable=should_deploy,
    )

    # -----------------------------------------------------------------------
    # 5. TaskGroup "deploy" — загрузка или пропуск
    # -----------------------------------------------------------------------
    with TaskGroup("deploy", tooltip="Deploy to Redis or skip") as deploy_group:
        load = PythonOperator(
            task_id="load_to_redis",
            python_callable=load_to_redis,
        )

        skip = EmptyOperator(
            task_id="skip_deploy",
        )

    # -----------------------------------------------------------------------
    # 6. Финальный таск с trigger_rule
    # -----------------------------------------------------------------------
    # trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS означает:
    # "выполнить, если хотя бы один upstream успешен и ни один не упал"
    # Это нужно, т.к. после branching один из путей будет SKIPPED.
    done = PythonOperator(
        task_id="log_completion",
        python_callable=log_completion,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # -----------------------------------------------------------------------
    # Зависимости
    # -----------------------------------------------------------------------
    wait_for_data >> compute_group >> quality_group >> branch >> deploy_group >> done
