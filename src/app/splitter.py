import csv
import hashlib
import os
from datetime import datetime

import yaml
from fastapi import FastAPI, Query
import httpx

app = FastAPI(title="A/B Splitter")

CONFIG_PATH = os.getenv("AB_CONFIG_PATH", "config_ab.yaml")
RECS_API_URL = os.getenv("RECS_API_URL", "http://recs-api:8000")
AB_LOG_PATH = os.getenv("AB_LOG_PATH", "ab_log.csv")


def load_config(path: str) -> list[dict]:
    with open(path) as f:
        cfg = yaml.safe_load(f)
    groups = cfg["groups"]
    total = sum(g["percent"] for g in groups)
    if total != 100:
        raise ValueError(f"Group percentages must sum to 100, got {total}")
    return groups


GROUPS = load_config(CONFIG_PATH)


def assign_group(device_id: str) -> dict:
    """Assign a device to an A/B group based on hash of device_id."""
    h = int(hashlib.md5(device_id.encode()).hexdigest(), 16)
    bucket = h % 100

    cumulative = 0
    for group in GROUPS:
        cumulative += group["percent"]
        if bucket < cumulative:
            return group

    return GROUPS[-1]


def log_assignment(device_id: str, group_name: str):
    file_exists = os.path.exists(AB_LOG_PATH)
    with open(AB_LOG_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "device_id", "group"])
        writer.writerow([datetime.now().isoformat(), device_id, group_name])


@app.get("/recommendations")
async def get_recommendations(
    device_id: str = Query(..., description="Device ID for A/B splitting"),
    age_restriction: int = Query(18, description="Age restriction"),
):
    group = assign_group(device_id)
    model_name = group["model_name"]

    log_assignment(device_id, group["name"])

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{RECS_API_URL}/recs",
            params={"model_name": model_name, "age_restriction": age_restriction},
        )
        recs = resp.json()

    return {
        "group": group["name"],
        "model_name": model_name,
        "recommendations": recs.get("recommendations", []),
    }
