import json
import os
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import FastAPI, Query
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue, Range
import mlflow
import redis
import tritonclient.http as tritonhttpclient

app = FastAPI(title="Online Recs API")

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    password=os.getenv("REDIS_PASSWORD", "recsys_redis_pass"),
    decode_responses=True,
)

qdrant_client = QdrantClient(
    host=os.getenv("QDRANT_HOST", "localhost"),
    port=int(os.getenv("QDRANT_PORT", "6333")),
)

COLLECTION_NAME = os.getenv("QDRANT_COLLECTION", "items")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
TRITON_URL = os.getenv("TRITON_URL", "localhost:8010")
TRITON_MODEL_NAME = os.getenv("TRITON_MODEL_NAME", "catboost_reranker")

# Item эмбеддинги в памяти для brute-force подхода
ITEM_IDS: list[int] = []
ITEM_EMBEDDINGS: np.ndarray | None = None

# CatBoost reranker, загружается из MLflow
RERANKER_MODEL = None

FEATURE_COLS = [
    "als_score", "als_rank",
    "age", "income", "sex", "kids_flg",
    "content_type", "release_year", "age_rating", "for_kids",
    "n_genres", "n_countries", "n_actors",
]
CAT_FEATURES = ["age", "income", "sex", "content_type", "for_kids"]


@app.on_event("startup")
def startup():
    """Загружаем item эмбеддинги и reranker модель."""
    global ITEM_IDS, ITEM_EMBEDDINGS, RERANKER_MODEL

    # Item embeddings для brute-force
    raw = redis_client.get("als:item_ids")
    if not raw:
        print("WARNING: als:item_ids not found in Redis")
    else:
        ITEM_IDS = json.loads(raw)
        pipe = redis_client.pipeline()
        for item_id in ITEM_IDS:
            pipe.get(f"als:item:{item_id}")
        results = pipe.execute()

        embeddings, valid_ids = [], []
        for item_id, emb_raw in zip(ITEM_IDS, results):
            if emb_raw is not None:
                embeddings.append(json.loads(emb_raw))
                valid_ids.append(item_id)
        ITEM_IDS = valid_ids
        ITEM_EMBEDDINGS = np.array(embeddings, dtype=np.float32)
        print(f"Loaded {len(ITEM_IDS)} item embeddings, shape: {ITEM_EMBEDDINGS.shape}")

    # Reranker из MLflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    run_id = redis_client.get("reranker:mlflow_run_id")
    if run_id:
        model_uri = f"runs:/{run_id}/reranker_model"
        RERANKER_MODEL = mlflow.catboost.load_model(model_uri)
        print(f"Loaded reranker model from {model_uri}")
    else:
        print("WARNING: reranker:mlflow_run_id not found in Redis, /recs_reranked will be unavailable")


@app.get("/health")
def health():
    info = qdrant_client.get_collection(COLLECTION_NAME)
    return {
        "status": "ok",
        "items_in_memory": len(ITEM_IDS),
        "qdrant_points": info.points_count,
        "reranker_loaded": RERANKER_MODEL is not None,
    }


# ──────────────────────────────────────────────────────────────
# Brute-force: dot product по всем айтемам в numpy
# ──────────────────────────────────────────────────────────────

@app.get("/recs")
def get_recs(
    user_id: int = Query(..., description="User ID"),
    top_k: int = Query(10, description="Number of recommendations"),
):
    user_emb_raw = redis_client.get(f"als:user:{user_id}")
    if not user_emb_raw:
        return {"error": f"User {user_id} not found", "recommendations": []}

    if ITEM_EMBEDDINGS is None or len(ITEM_IDS) == 0:
        return {"error": "Item embeddings not loaded", "recommendations": []}

    user_emb = np.array(json.loads(user_emb_raw), dtype=np.float32)

    scores = ITEM_EMBEDDINGS @ user_emb
    top_indices = np.argsort(scores)[::-1][:top_k]

    recommendations = [
        {"item_id": ITEM_IDS[idx], "score": float(scores[idx])}
        for idx in top_indices
    ]

    return {"user_id": user_id, "recommendations": recommendations}


# ──────────────────────────────────────────────────────────────
# ANN: приближённый поиск через Qdrant
# ──────────────────────────────────────────────────────────────

@app.get("/recs_ann")
def get_recs_ann(
    user_id: int = Query(..., description="User ID"),
    top_k: int = Query(10, description="Number of recommendations"),
    content_type: Optional[str] = Query(None, description="Filter: film or series"),
    max_age_rating: Optional[int] = Query(None, description="Filter: max age rating (0,6,12,16,18)"),
):
    user_emb_raw = redis_client.get(f"als:user:{user_id}")
    if not user_emb_raw:
        return {"error": f"User {user_id} not found", "recommendations": []}

    user_emb = json.loads(user_emb_raw)

    conditions = []
    if content_type:
        conditions.append(FieldCondition(key="content_type", match=MatchValue(value=content_type)))
    if max_age_rating is not None:
        conditions.append(FieldCondition(key="age_rating", range=Range(lte=max_age_rating)))

    query_filter = Filter(must=conditions) if conditions else None

    results = qdrant_client.query_points(
        collection_name=COLLECTION_NAME,
        query=user_emb,
        query_filter=query_filter,
        limit=top_k,
    )

    recommendations = [
        {
            "item_id": point.id,
            "score": point.score,
            "title": point.payload.get("title", ""),
            "genres": point.payload.get("genres", ""),
        }
        for point in results.points
    ]

    return {"user_id": user_id, "recommendations": recommendations}


# ──────────────────────────────────────────────────────────────
# Similar: item-to-item через Qdrant
# ──────────────────────────────────────────────────────────────

@app.get("/similar")
def get_similar(
    item_id: int = Query(..., description="Item ID"),
    top_k: int = Query(10, description="Number of similar items"),
):
    results = qdrant_client.query_points(
        collection_name=COLLECTION_NAME,
        query=item_id,
        limit=top_k + 1,
    )

    similar = [
        {
            "item_id": point.id,
            "score": point.score,
            "title": point.payload.get("title", ""),
            "genres": point.payload.get("genres", ""),
        }
        for point in results.points
        if point.id != item_id
    ][:top_k]

    return {"item_id": item_id, "similar": similar}


# ──────────────────────────────────────────────────────────────
# Reranked: ANN-кандидаты + CatBoost reranker из MLflow
# ──────────────────────────────────────────────────────────────

DEFAULT_USER_FEATURES = {"age": "unknown", "income": "unknown", "sex": "unknown", "kids_flg": 0}
DEFAULT_ITEM_FEATURES = {
    "content_type": "unknown", "release_year": 0, "age_rating": 0,
    "for_kids": "unknown", "n_genres": 0, "n_countries": 0, "n_actors": 0,
}


@app.get("/recs_reranked")
def get_recs_reranked(
    user_id: int = Query(..., description="User ID"),
    top_k: int = Query(10, description="Number of recommendations to return"),
    n_candidates: int = Query(100, description="Number of ANN candidates to rerank"),
):
    if RERANKER_MODEL is None:
        return {"error": "Reranker model not loaded", "recommendations": []}

    # 1. Получаем эмбеддинг пользователя
    user_emb_raw = redis_client.get(f"als:user:{user_id}")
    if not user_emb_raw:
        return {"error": f"User {user_id} not found", "recommendations": []}
    user_emb = json.loads(user_emb_raw)

    # 2. ANN-кандидаты из Qdrant
    ann_results = qdrant_client.query_points(
        collection_name=COLLECTION_NAME,
        query=user_emb,
        limit=n_candidates,
    )

    if not ann_results.points:
        return {"user_id": user_id, "recommendations": []}

    # 3. Собираем фичи для реранкера
    user_feat_raw = redis_client.get(f"user_features:{user_id}")
    user_feat = json.loads(user_feat_raw) if user_feat_raw else DEFAULT_USER_FEATURES

    pipe = redis_client.pipeline()
    for point in ann_results.points:
        pipe.get(f"item_features:{point.id}")
    item_feats_raw = pipe.execute()

    rows = []
    for rank, (point, ifeat_raw) in enumerate(zip(ann_results.points, item_feats_raw)):
        item_feat = json.loads(ifeat_raw) if ifeat_raw else DEFAULT_ITEM_FEATURES
        rows.append({
            "item_id": point.id,
            "als_score": point.score,
            "als_rank": rank,
            "title": point.payload.get("title", ""),
            "genres": point.payload.get("genres", ""),
            **user_feat,
            **item_feat,
        })

    df = pd.DataFrame(rows)

    # 4. Предикт реранкера
    rerank_scores = RERANKER_MODEL.predict_proba(df[FEATURE_COLS])[:, 1]
    df["rerank_score"] = rerank_scores

    # 5. Сортируем по скору реранкера, берём top_k
    df = df.sort_values("rerank_score", ascending=False).head(top_k)

    recommendations = [
        {
            "item_id": int(row["item_id"]),
            "rerank_score": float(row["rerank_score"]),
            "als_score": float(row["als_score"]),
            "title": row["title"],
            "genres": row["genres"],
        }
        for _, row in df.iterrows()
    ]

    return {"user_id": user_id, "recommendations": recommendations}


# ──────────────────────────────────────────────────────────────
# Triton: ANN-кандидаты + CatBoost reranker через Triton
# ──────────────────────────────────────────────────────────────

@app.get("/recs_triton")
def get_recs_triton(
    user_id: int = Query(..., description="User ID"),
    top_k: int = Query(10, description="Number of recommendations to return"),
    n_candidates: int = Query(100, description="Number of ANN candidates to rerank"),
):
    # 1. Эмбеддинг пользователя
    user_emb_raw = redis_client.get(f"als:user:{user_id}")
    if not user_emb_raw:
        return {"error": f"User {user_id} not found", "recommendations": []}
    user_emb = json.loads(user_emb_raw)

    # 2. ANN-кандидаты из Qdrant
    ann_results = qdrant_client.query_points(
        collection_name=COLLECTION_NAME,
        query=user_emb,
        limit=n_candidates,
    )

    if not ann_results.points:
        return {"user_id": user_id, "recommendations": []}

    # 3. Собираем фичи (те же, что для /recs_reranked)
    user_feat_raw = redis_client.get(f"user_features:{user_id}")
    user_feat = json.loads(user_feat_raw) if user_feat_raw else DEFAULT_USER_FEATURES

    pipe = redis_client.pipeline()
    for point in ann_results.points:
        pipe.get(f"item_features:{point.id}")
    item_feats_raw = pipe.execute()

    feature_jsons = []
    point_meta = []
    for rank, (point, ifeat_raw) in enumerate(zip(ann_results.points, item_feats_raw)):
        item_feat = json.loads(ifeat_raw) if ifeat_raw else DEFAULT_ITEM_FEATURES
        row = {
            "als_score": point.score,
            "als_rank": rank,
            **user_feat,
            **item_feat,
        }
        feature_jsons.append(json.dumps(row))
        point_meta.append({
            "item_id": point.id,
            "als_score": point.score,
            "title": point.payload.get("title", ""),
            "genres": point.payload.get("genres", ""),
        })

    # 4. Вызов Triton
    triton_client = tritonhttpclient.InferenceServerClient(url=TRITON_URL)

    input_data = np.array(feature_jsons, dtype=object)
    triton_input = tritonhttpclient.InferInput("features", input_data.shape, "BYTES")
    triton_input.set_data_from_numpy(input_data)

    triton_output = tritonhttpclient.InferRequestedOutput("scores")

    response = triton_client.infer(
        model_name=TRITON_MODEL_NAME,
        inputs=[triton_input],
        outputs=[triton_output],
    )

    scores = response.as_numpy("scores")

    # 5. Реранжируем и берём top_k
    top_indices = np.argsort(scores)[::-1][:top_k]

    recommendations = [
        {
            **point_meta[idx],
            "rerank_score": float(scores[idx]),
        }
        for idx in top_indices
    ]

    return {"user_id": user_id, "recommendations": recommendations}
