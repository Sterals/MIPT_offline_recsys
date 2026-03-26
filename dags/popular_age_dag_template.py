"""
ЗАДАНИЕ: DAG popular_age_daily
===============================
Напишите DAG, который считает top-popular items ПО ВОЗРАСТНЫМ ГРУППАМ
и загружает результаты в Redis.

Пайплайн:
    build_popular_age  →  validate  →  load_to_redis

Требования:
    1. DAG должен запускаться ежедневно в 07:00
    2. catchup за июнь 2021 (как в popular_dag.py)
    3. Возрастные группы: [0, 6, 12, 16, 18, 21]
    4. Для каждой группы — top-10 items с age_rating <= порогу группы
    5. Результат каждой группы сохраняется в отдельный CSV
    6. Валидация: проверить что все 6 файлов существуют и не пустые
    7. Загрузка в Redis: ключ "popular_age:{age}" для каждой группы

Подсказки:
    - Смотрите popular_dag.py как образец структуры
    - Используйте XCom для передачи путей между тасками
    - items.csv содержит колонку age_rating
    - Для передачи нескольких значений через XCom можно вернуть dict или list
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
    """
    TODO: Реализуйте функцию.

    1. Получите ds из context
    2. Вычислите окно [end_date - 14 дней, end_date]
    3. Прочитайте interactions.csv и items.csv
    4. Отфильтруйте interactions по дате
    5. Для каждой возрастной группы из AGE_GROUPS:
       - Отфильтруйте items с age_rating <= порогу
       - Посчитайте top-K по n_users
       - Сохраните в CSV: top_popular_age_{age}_{ds}.csv
    6. Верните список путей к CSV через return (для XCom)
    """
    # ВАШ КОД ЗДЕСЬ
    pass


def validate(**context):
    """
    TODO: Реализуйте функцию.

    1. Получите список путей CSV из XCom (task_id="build_popular_age")
    2. Для каждого файла проверьте:
       - Файл существует
       - Файл не пустой (len > 0)
    3. При ошибке — бросьте исключение (assert или raise)
    4. Верните список путей дальше
    """
    # ВАШ КОД ЗДЕСЬ
    pass


def load_to_redis(**context):
    """
    TODO: Реализуйте функцию.

    1. Получите список путей CSV из XCom (task_id="validate")
    2. Подключитесь к Redis (используйте Variable.get для конфигурации)
    3. Для каждого файла:
       - Прочитайте CSV
       - Извлеките item_id как список строк
       - Сохраните в Redis с ключом "popular_age:{age}"

    Подсказка: возрастную группу можно извлечь из имени файла.
    """
    # ВАШ КОД ЗДЕСЬ
    pass


# ---------------------------------------------------------------------------
# TODO: Определите DAG
# ---------------------------------------------------------------------------
# Подсказка: используйте конструкцию with DAG(...) as dag:
#
# Параметры:
#   - dag_id: "popular_age_daily"
#   - schedule_interval: "0 7 * * *"
#   - start_date: 1 июня 2021
#   - end_date: 30 июня 2021
#   - catchup: True
#   - max_active_runs: 1
#   - tags: ["recsys", "popular_age"]
#
# Создайте 3 PythonOperator и задайте зависимости:
#   build >> check >> load

# ВАШ КОД ЗДЕСЬ
