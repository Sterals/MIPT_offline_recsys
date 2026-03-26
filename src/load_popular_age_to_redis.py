"""Load age-restricted top-popular CSVs into Redis."""

import argparse
import json
import os
from pathlib import Path

import pandas as pd
import redis

DATA_DIR = Path(__file__).resolve().parents[1] / "data"

AGE_GROUPS = [0, 6, 12, 16, 18, 21]


def main() -> None:
    parser = argparse.ArgumentParser(description="Load age-restricted popular items into Redis")
    parser.add_argument("date", type=str, help="Date used in CSV filenames (YYYY-MM-DD)")
    parser.add_argument("--input-dir", type=str, default=None, help="Directory with CSVs (default: data/)")
    parser.add_argument("--redis-host", type=str, default=os.getenv("REDIS_HOST", "localhost"))
    parser.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    parser.add_argument("--redis-password", type=str, default=os.getenv("REDIS_PASSWORD", "recsys_redis_pass"))
    args = parser.parse_args()

    input_dir = Path(args.input_dir) if args.input_dir else DATA_DIR

    r = redis.Redis(host=args.redis_host, port=args.redis_port, password=args.redis_password, decode_responses=True)

    for age in AGE_GROUPS:
        csv_path = input_dir / f"top_popular_age_{age}_{args.date}.csv"
        df = pd.read_csv(csv_path)
        item_ids = df["item_id"].astype(str).tolist()

        key = f"popular_age:{age}"
        r.set(key, json.dumps(item_ids))
        print(f"Loaded {len(item_ids)} items into Redis key '{key}'")


if __name__ == "__main__":
    main()
