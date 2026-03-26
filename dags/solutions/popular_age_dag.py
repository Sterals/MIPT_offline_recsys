"""
ЭТАЛОННОЕ РЕШЕНИЕ: DAG popular_age_daily
=========================================
Считает top-popular items по возрастным группам и загружает в Redis.

Пайплайн:
    build_popular_age  →  validate  →  load_to_redis
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


def build_popular_age(**context):
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

    output_paths = []
    for age in AGE_GROUPS:
        age_items = counts[counts["age_rating"].fillna(0) <= age]
        top = (
            age_items.sort_values("n_users", ascending=False)
            .head(TOP_K)
            .reset_index(drop=True)
        )
        top["rank"] = range(1, len(top) + 1)

        output_path = DATA_DIR / f"top_popular_age_{age}_{ds}.csv"
        top[["item_id", "n_users", "total_watches", "rank"]].to_csv(output_path, index=False)
        output_paths.append(str(output_path))
        print(f"[build_popular_age] age={age}, saved {len(top)} items → {output_path}")

    return output_paths


def validate(**context):
    csv_paths = context["ti"].xcom_pull(task_ids="build_popular_age")

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


def load_to_redis(**context):
    csv_paths = context["ti"].xcom_pull(task_ids="validate")

    redis_host = Variable.get("REDIS_HOST", default_var="redis")
    redis_port = int(Variable.get("REDIS_PORT", default_var="6379"))
    redis_password = Variable.get("REDIS_PASSWORD", default_var="recsys_redis_pass")

    r = redis.Redis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)

    for csv_path in csv_paths:
        df = pd.read_csv(csv_path)
        item_ids = df["item_id"].astype(str).tolist()

        # Извлекаем возрастную группу из имени файла: top_popular_age_{age}_{ds}.csv
        filename = Path(csv_path).stem  # top_popular_age_18_2021-06-15
        age = filename.split("_")[3]    # "18"
        key = f"popular_age:{age}"

        r.set(key, json.dumps(item_ids))
        print(f"[load_to_redis] Loaded {len(item_ids)} items → Redis key '{key}'")


default_args = {
    "owner": "recsys",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="popular_age_daily",
    default_args=default_args,
    description="Build top popular per age group and load to Redis",
    schedule_interval="0 7 * * *",
    start_date=datetime(2021, 6, 1),
    end_date=datetime(2021, 6, 30),
    catchup=True,
    max_active_runs=1,
    tags=["recsys", "popular_age"],
) as dag:

    build = PythonOperator(
        task_id="build_popular_age",
        python_callable=build_popular_age,
    )

    check = PythonOperator(
        task_id="validate",
        python_callable=validate,
    )

    load = PythonOperator(
        task_id="load_to_redis",
        python_callable=load_to_redis,
    )

    build >> check >> load
