import argparse
import json
import os

import pandas as pd
import redis


def main() -> None:
    parser = argparse.ArgumentParser(description="Load popular items CSV into Redis set")
    parser.add_argument("csv_path", type=str, help="Path to the top_popular CSV file")
    parser.add_argument("--redis-host", type=str, default=os.getenv("REDIS_HOST", "localhost"))
    parser.add_argument("--redis-port", type=int, default=int(os.getenv("REDIS_PORT", "6379")))
    parser.add_argument("--redis-password", type=str, default=os.getenv("REDIS_PASSWORD", "recsys_redis_pass"))
    parser.add_argument("--key", type=str, default="popular", help="Redis key name (default: popular)")
    args = parser.parse_args()

    df = pd.read_csv(args.csv_path)
    item_ids = df["item_id"].astype(str).tolist()

    r = redis.Redis(host=args.redis_host, port=args.redis_port, password=args.redis_password, decode_responses=True)

    r.set(args.key, json.dumps(item_ids))

    print(f"Loaded {len(item_ids)} items into Redis key '{args.key}'")


if __name__ == "__main__":
    main()
