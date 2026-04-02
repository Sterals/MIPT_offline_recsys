import random
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


def _model_to_kwargs(model_name):
    """Преобразует имя модели в kwargs для train_model. Используется в .map()."""
    return {"model_name": model_name}


def get_models(**context):
    """
    Возвращает список моделей для обучения.

    В реальности список может приходить из Variable, конфига,
    базы данных или API. Здесь для простоты — дефолтный список.
    """
    default_models = ["popular", "popular_age", "als", "bpr"]
    models_raw = Variable.get("TRAINING_MODELS", default_var=None)

    if models_raw:
        models = [m.strip() for m in models_raw.split(",")]
    else:
        models = default_models

    print(f"[get_models] Will train: {models}")
    return models


def train_model(model_name, ds, **context):
    """
    Имитация обучения модели.

    Каждый mapped-таск получает один элемент из списка моделей.
    В реальности здесь был бы вызов training pipeline или DockerOperator.
    """
    print(f"[train] Starting training: model={model_name}, ds={ds}")

    # Имитируем обучение разной длительности
    duration = random.uniform(1, 5)
    time.sleep(duration)

    # Имитируем метрики
    metrics = {
        "model": model_name,
        "ds": ds,
        "precision_at_10": round(random.uniform(0.05, 0.35), 4),
        "recall_at_10": round(random.uniform(0.02, 0.20), 4),
        "training_time_sec": round(duration, 2),
    }

    print(f"[train] Done: {metrics}")
    return metrics


def report(results, **context):
    """
    Собирает результаты всех mapped-тасков.

    results — список значений, возвращённых каждым train_model.
    Airflow автоматически собирает XCom со всех mapped-тасков.
    """
    print("[report] === Training Results ===")
    print(f"[report] Trained {len(results)} models")

    best = max(results, key=lambda r: r["precision_at_10"])
    print(f"[report] Best model: {best['model']} (precision@10={best['precision_at_10']})")

    for r in sorted(results, key=lambda r: r["precision_at_10"], reverse=True):
        print(f"  {r['model']:>15}: p@10={r['precision_at_10']:.4f}, r@10={r['recall_at_10']:.4f}, time={r['training_time_sec']}s")

    context["ti"].xcom_push(key="best_model", value=best["model"])
    context["ti"].xcom_push(key="all_results", value=results)


default_args = {
    "owner": "recsys",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dynamic_training_demo",
    default_args=default_args,
    description="Dynamic task mapping: train N models in parallel",
    schedule_interval="0 8 * * *",
    start_date=datetime(2021, 6, 1),
    catchup=False,
    max_active_runs=1,
    tags=["demo", "dynamic_mapping"],
) as dag:

    # ------------------------------------------------------------------
    # 1. Получаем список моделей
    # ------------------------------------------------------------------
    get = PythonOperator(
        task_id="get_models",
        python_callable=get_models,
    )

    # ------------------------------------------------------------------
    # 2. Dynamic Task Mapping — для каждой модели создаётся свой таск
    # ------------------------------------------------------------------
    # .partial() — параметры, общие для всех тасков
    # .expand() — параметр, который маппится: один элемент = один таск
    #
    # Если get_models вернул ["popular", "popular_age", "als", "bpr"],
    # Airflow создаст 4 таска:
    #   train_model[0] (model_name="popular")
    #   train_model[1] (model_name="popular_age")
    #   train_model[2] (model_name="als")
    #   train_model[3] (model_name="bpr")
    train = PythonOperator.partial(
        task_id="train_model",
        python_callable=train_model,
    ).expand(
        op_kwargs=get.output.map(_model_to_kwargs),
    )

    # ------------------------------------------------------------------
    # 3. Сбор результатов — автоматически ждёт ВСЕ mapped-таски
    # ------------------------------------------------------------------
    # train.output — список XCom-значений от всех mapped-тасков
    summary = PythonOperator(
        task_id="report",
        python_callable=report,
        op_kwargs={"results": train.output},
    )

    get >> train >> summary
