"""
DAG: popular_daily
==================
Считает top-popular items за скользящее окно 14 дней и загружает в Redis.
Запускается ежедневно в 06:00, прогоняет catchup за июнь 2021.

Пайплайн:
    build_popular  →  validate  →  load_to_redis

Концепции Airflow, которые здесь демонстрируются:
    - DAG и schedule_interval (cron)
    - PythonOperator
    - context["ds"] — логическая дата запуска
    - XCom — передача данных между тасками
    - Variable — конфигурация вне кода
    - catchup — прогон за исторические даты
    - default_args (retries, retry_delay)
    - Зависимости через оператор >>
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import redis
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

DATA_DIR = Path("/opt/airflow/data")
TOP_K = 10


# ---------------------------------------------------------------------------
# Task 1: Считаем top-popular
# ---------------------------------------------------------------------------
def build_popular(**context):
    """
    Берём interactions за последние 14 дней относительно ds (логической даты),
    считаем top-K по числу уникальных пользователей, сохраняем CSV.

    Возвращаемое значение автоматически попадает в XCom
    и может быть прочитано downstream-тасками через xcom_pull.
    """
    ds = context["ds"]  # логическая дата запуска, формат YYYY-MM-DD
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
    print(f"[build_popular] ds={ds}, saved {len(top)} items → {output_path}")

    # xcom_push — сохраняем значение с произвольным ключом
    context["ti"].xcom_push(key="updated_at", value=ds)

    # Всё, что возвращает callable, Airflow кладёт в XCom с ключом "return_value"
    return str(output_path)


# ---------------------------------------------------------------------------
# Task 2: Валидация результатов
# ---------------------------------------------------------------------------
def validate(**context):
    """
    Проверяем, что CSV корректен:
    - файл существует и не пустой
    - количество строк = TOP_K
    - нет дубликатов item_id

    Если валидация не прошла — бросаем исключение,
    и Airflow пометит таск как FAILED (downstream-таски не запустятся).
    """
    csv_path = context["ti"].xcom_pull(task_ids="build_popular")
    path = Path(csv_path)

    assert path.exists(), f"Output file not found: {csv_path}"

    df = pd.read_csv(csv_path)
    assert len(df) > 0, "Output CSV is empty"
    assert len(df) == TOP_K, f"Expected {TOP_K} rows, got {len(df)}"
    assert df["item_id"].is_unique, "Duplicate item_id found"

    print(f"[validate] OK — {len(df)} unique items, file: {csv_path}")
    return csv_path


# ---------------------------------------------------------------------------
# Task 3: Загрузка в Redis
# ---------------------------------------------------------------------------
def load_to_redis(**context):
    """
    Читаем CSV (путь получаем через XCom из validate),
    загружаем список item_id в Redis как JSON-массив.

    Конфигурация Redis берётся из Airflow Variables —
    это позволяет менять хост/порт/пароль через UI без изменения кода.
    """
    csv_path = context["ti"].xcom_pull(task_ids="validate")

    redis_host = Variable.get("REDIS_HOST", default_var="redis")
    redis_port = int(Variable.get("REDIS_PORT", default_var="6379"))
    redis_password = Variable.get("REDIS_PASSWORD", default_var="recsys_redis_pass")

    df = pd.read_csv(csv_path)
    item_ids = df["item_id"].astype(str).tolist()

    r = redis.Redis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
    r.set("popular", json.dumps(item_ids))
    print(f"[load_to_redis] Loaded {len(item_ids)} items into Redis key 'popular'")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
default_args = {
    "owner": "recsys",
    "retries": 1,                        # при падении таска — одна повторная попытка
    "retry_delay": timedelta(minutes=5),  # через 5 минут после фейла
}

with DAG(
    dag_id="popular_daily",
    default_args=default_args,
    description="Build top popular and load to Redis",
    schedule_interval="0 6 * * *",   # каждый день в 06:00 UTC
    start_date=datetime(2021, 6, 1), # первая логическая дата
    end_date=datetime(2021, 6, 30),  # последняя логическая дата
    catchup=True,                    # прогнать все пропущенные даты
    max_active_runs=1,               # не более 1 запуска одновременно
    tags=["recsys", "popular"],
) as dag:

    build = PythonOperator(
        task_id="build_popular",
        python_callable=build_popular,
    )

    check = PythonOperator(
        task_id="validate",
        python_callable=validate,
    )

    load = PythonOperator(
        task_id="load_to_redis",
        python_callable=load_to_redis,
    )

    # Задаём порядок выполнения: build → validate → load
    build >> check >> load
