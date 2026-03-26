import json
import os
from typing import Optional

from fastapi import FastAPI, Query
import redis

app = FastAPI(title="Offline Recs API")

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    password=os.getenv("REDIS_PASSWORD", "recsys_redis_pass"),
    decode_responses=True,
)

REDIS_KEY_MAP = {
    "popular": "popular",
    "popular_age": "popular_age:{age}",
}


@app.get("/recs")
def get_recs(
    model_name: str = Query("popular", description="Model name: popular or popular_age"),
    age_restriction: Optional[int] = Query(18, description="Age restriction (0, 6, 12, 16, 18, 21)"),
):
    if model_name == "popular_age":
        key = REDIS_KEY_MAP["popular_age"].format(age=age_restriction)
    else:
        key = REDIS_KEY_MAP["popular"]

    raw = redis_client.get(key)
    items = json.loads(raw) if raw else []
    return {"recommendations": items}
