# MIPT Offline & Online Recommender Systems

Учебный репозиторий к лекциям по офлайн и онлайн рекомендательным системам (МФТИ).

## Структура репозитория

```
.
├── data/                        # Данные (users, items, interactions) 
├── offline/                     # Офлайн-часть
│   └── src/
│       ├── dags/                # Учебные DAG-и для Apache Airflow
│       │   ├── hello_docker_dag.py          # Пример запуска Docker-оператора
│       │   ├── popular_dag.py               # DAG расчёта популярных рекомендаций
│       │   ├── popular_age_dag.py           # DAG популярных рекомендаций с учётом возраста
│       │   ├── popular_age_dag_template.py  # Шаблон для самостоятельной работы
│       │   ├── dynamic_training_dag.py      # Пример динамического построения DAG-а
│       │   ├── cleanup_dag.py               # DAG очистки данных
│       │   ├── error_handling_dag.py        # Пример обработки ошибок в DAG-ах
│       │   ├── examples/                    # Продвинутые примеры
│       │   └── solutions/                   # Решения к заданиям
│       ├── app/                 # Простейшая рекомендательная система
│       │   ├── main.py          # Сервис рекомендаций (FastAPI)
│       │   ├── splitter.py      # A/B-сплиттер
│       │   └── deploy/         # Docker-конфигурация для деплоя
│       ├── notebooks/           # Ноутбуки (EDA, работа с Redis, скачивание датасета)
│       ├── popular.py           # Скрипт расчёта популярных рекомендаций
│       ├── popular_age.py       # Скрипт расчёта рекомендаций по возрасту
│       └── load_popular*        # Загрузка рекомендаций в Redis
├── online/                      # Онлайн-часть
│   └── src/
│       ├── notebooks/           # Ноутбуки с примерами онлайн-рекомендаций
│       │   ├── train_als.py     # Обучение ALS-модели
│       │   ├── qdrant_ann.ipynb # Approximate Nearest Neighbors (Qdrant)
│       │   ├── train_reranker.ipynb  # Обучение ранжирующей модели
│       │   └── mlflow_demo.ipynb     # Демо MLflow
│       ├── app/                 # Микросервис онлайн-рекомендаций
│       │   ├── main.py          # Сервис (FastAPI)
│       │   └── deploy/         # Docker-конфигурация для деплоя
│       └── load_test/           # Нагрузочное тестирование (k6)
├── infra/                       # Инфраструктура (Docker Compose)
│   ├── docker-compose.airflow.yml   # Apache Airflow
│   ├── docker-compose.mlflow.yml    # MLflow Tracking Server
│   ├── docker-compose.qdrant.yml    # Qdrant (векторная БД)
│   ├── docker-compose.redis.yml     # Redis
│   ├── docker-compose.triton.yml    # NVIDIA Triton Inference Server
│   └── triton_models/               # Модели для Triton (CatBoost reranker)
└── pyproject.toml               # Зависимости проекта (Poetry)
```

## Компоненты инфраструктуры

- **Apache Airflow** -- оркестрация офлайн-пайплайнов
- **Redis** -- хранение предрассчитанных рекомендаций
- **Qdrant** -- векторный поиск ближайших соседей
- **MLflow** -- трекинг экспериментов и моделей
- **NVIDIA Triton** -- сервинг ML-моделей (CatBoost reranker)
