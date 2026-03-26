"""
DAG: popular_age_daily
=======================
Считает top-popular items по возрастным группам и загружает в Redis.
Вычисление для каждой возрастной группы выполняется параллельно
через Dynamic Task Mapping.

Пайплайн:
    prepare_data → build_age_group ×6 (параллельно) → validate → load_to_redis
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
AGE_GROUPS = [0, 6, 12, 16, 18, 21]
TOP_K = 10


def _age_to_kwargs(age):
    """Преобразует возрастную группу в kwargs для build_age_group."""
    return {"age": age}


def prepare_data(**context):
    """
    Читаем данные один раз, считаем агрегаты и сохраняем промежуточный CSV.
    Это позволяет не читать interactions.csv в каждом параллельном таске.
    """
    ds = context["ds"]
    end_date = datetime.strptime(ds, "%Y-%m-%d").date()
    start_date = end_date - timedelta(days=14)

    interactions = pd.read_csv(DATA_DIR / "interactions.csv", parse_dates=["last_watch_dt"])
    items = pd.read_csv(DATA_DIR / "items.csv", usecols=["item_id", "age_rating"])

    mask = (interactions["last_watch_dt"].dt.date > start_date) & (
        interactions["last_watch_dt"].dt.date <= end_date
    )
    window = interactions.loc[mask]

    counts = (
        window.groupby("item_id")
        .agg(n_users=("user_id", "nunique"), total_watches=("user_id", "count"))
        .reset_index()
    )
    counts = counts.merge(items, on="item_id", how="left")

    prepared_path = DATA_DIR / f"_counts_{ds}.csv"
    counts.to_csv(prepared_path, index=False)
    print(f"[prepare_data] ds={ds}, {len(counts)} items → {prepared_path}")

    return str(prepared_path)


def build_age_group(age, **context):
    """
    Считает top-K для одной возрастной группы.
    Каждый mapped-таск получает свой age через expand.
    """
    ds = context["ds"]
    prepared_path = context["ti"].xcom_pull(task_ids="prepare_data")

    counts = pd.read_csv(prepared_path)
    age_items = counts[counts["age_rating"].fillna(0) <= age]

    top = (
        age_items.sort_values("n_users", ascending=False)
        .head(TOP_K)
        .reset_index(drop=True)
    )
    top["rank"] = range(1, len(top) + 1)

    output_path = DATA_DIR / f"top_popular_age_{age}_{ds}.csv"
    top[["item_id", "n_users", "total_watches", "rank"]].to_csv(output_path, index=False)
    print(f"[build_age_group] age={age}, {len(top)} items → {output_path}")

    return str(output_path)


def validate(csv_paths, **context):
    """Проверяем что все файлы на месте и не пустые."""
    assert len(csv_paths) == len(AGE_GROUPS), (
        f"Expected {len(AGE_GROUPS)} files, got {len(csv_paths)}"
    )
    for csv_path in csv_paths:
        path = Path(csv_path)
        assert path.exists(), f"File not found: {csv_path}"
        df = pd.read_csv(csv_path)
        assert len(df) > 0, f"Empty CSV: {csv_path}"
        print(f"[validate] OK — {len(df)} items in {path.name}")

    return csv_paths


def load_to_redis(csv_paths, **context):
    """Загружаем каждую возрастную группу в Redis."""
    redis_host = Variable.get("REDIS_HOST", default_var="redis")
    redis_port = int(Variable.get("REDIS_PORT", default_var="6379"))
    redis_password = Variable.get("REDIS_PASSWORD", default_var="recsys_redis_pass")

    r = redis.Redis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)

    for csv_path in csv_paths:
        df = pd.read_csv(csv_path)
        item_ids = df["item_id"].astype(str).tolist()

        # top_popular_age_{age}_{ds}.csv → age
        age = Path(csv_path).stem.split("_")[3]
        key = f"popular_age:{age}"

        r.set(key, json.dumps(item_ids))
        print(f"[load_to_redis] {len(item_ids)} items → Redis key '{key}'")

    # Убираем промежуточный файл
    prepared_path = context["ti"].xcom_pull(task_ids="prepare_data")
    Path(prepared_path).unlink(missing_ok=True)


default_args = {
    "owner": "recsys",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="popular_age_daily",
    default_args=default_args,
    description="Build top popular per age group (parallel) and load to Redis",
    schedule_interval="0 7 * * *",
    start_date=datetime(2021, 6, 1),
    end_date=datetime(2021, 6, 30),
    catchup=True,
    max_active_runs=1,
    tags=["recsys", "popular_age"],
) as dag:

    prepare = PythonOperator(
        task_id="prepare_data",
        python_callable=prepare_data,
    )

    # Dynamic Task Mapping: 6 параллельных тасков — по одному на возрастную группу
    build = PythonOperator.partial(
        task_id="build_age_group",
        python_callable=build_age_group,
    ).expand(
        op_kwargs=[_age_to_kwargs(age) for age in AGE_GROUPS],
    )

    check = PythonOperator(
        task_id="validate",
        python_callable=validate,
        op_kwargs={"csv_paths": build.output},
    )

    load = PythonOperator(
        task_id="load_to_redis",
        python_callable=load_to_redis,
        op_kwargs={"csv_paths": build.output},
    )

    prepare >> build >> check >> load
