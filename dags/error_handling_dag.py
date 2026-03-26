"""
DAG: error_handling_demo
=========================
Демонстрирует обработку ошибок через trigger_rule.

Пайплайн:
                        ┌─ on_success (trigger_rule=ALL_SUCCESS) ─┐
    start → risky_task ─┤                                         ├─ done
                        └─ on_failure (trigger_rule=ALL_FAILED)  ─┘

Концепции:
    - trigger_rule — запуск таска в зависимости от статуса upstream
    - ALL_SUCCESS — только если upstream успешен (по умолчанию)
    - ALL_FAILED — только если upstream упал
    - NONE_FAILED_MIN_ONE_SUCCESS — финальный таск отработает в любом случае
"""

import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


def risky_division(**context):
    """Делим на 0 или 1 случайным образом. С вероятностью 50% таск упадёт."""
    divisor = random.choice([0, 1])
    print(f"[risky_division] divisor = {divisor}")
    result = 42 / divisor  # ZeroDivisionError если divisor == 0
    print(f"[risky_division] result = {result}")


def handle_success(**context):
    """Вызывается только если risky_task завершился успешно."""
    print("[on_success] risky_task completed successfully!")
    context["ti"].xcom_push(key="status", value=True)


def handle_failure(**context):
    """Вызывается только если risky_task упал."""
    print("[on_failure] risky_task failed! Handling error...")
    context["ti"].xcom_push(key="status", value=False)


default_args = {
    "owner": "recsys",
    "retries": 0,  # без retry — чтобы падение было сразу видно
}

with DAG(
    dag_id="error_handling_demo",
    default_args=default_args,
    description="Demo: handle task failure with trigger_rule",
    schedule_interval="0 12 * * *",
    start_date=datetime(2021, 6, 1),
    catchup=False,
    tags=["demo", "error_handling"],
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    risky = PythonOperator(
        task_id="risky_task",
        python_callable=risky_division,
    )

    success = PythonOperator(
        task_id="on_success",
        python_callable=handle_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,  # default, но указываем явно для наглядности
    )

    failure = PythonOperator(
        task_id="on_failure",
        python_callable=handle_failure,
        trigger_rule=TriggerRule.ALL_FAILED,  # запустится ТОЛЬКО если risky_task упал
    )

    # Финальный таск — выполнится в любом случае (один путь success, другой skipped)
    done = EmptyOperator(
        task_id="done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, # ALL_DONE
    )

    start >> risky >> [success, failure] >> done
