"""
DAG: hello_docker
==================
Запускает Python-контейнер через DockerOperator и выполняет print("Hello, World!").

Демонстрирует:
    - DockerOperator — запуск задач в изолированном Docker-контейнере
    - Зачем это нужно: изоляция зависимостей, воспроизводимость, безопасность
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "recsys",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="hello_docker",
    default_args=default_args,
    description="Run print('Hello, World!') inside a Python Docker container",
    schedule_interval="0 10 * * *",
    start_date=datetime(2026, 3, 26),
    catchup=False,
    tags=["demo", "docker"],
) as dag:

    hello = DockerOperator(
        task_id="hello_world",
        image="python:3.11-slim",        # образ скачается автоматически при первом запуске
        command='python -c "print(\'Hello, World!\')"',
        docker_url="unix://var/run/docker.sock",  # подключение к Docker daemon хоста
        network_mode="bridge",
        auto_remove=False,               # удалить контейнер после завершения
        mount_tmp_dir=False,             # не монтировать tmp — не нужен для этого таска
    )
