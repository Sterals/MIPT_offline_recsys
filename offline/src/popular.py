import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def main() -> None:
    parser = argparse.ArgumentParser(description="Top popular items for a 2-week window")
    parser.add_argument("date", type=str, help="End date in YYYY-MM-DD format")
    parser.add_argument("--top-k", type=int, default=10, help="Number of top items (default: 10)")
    parser.add_argument("--output", type=str, default=None, help="Output CSV path")
    args = parser.parse_args()

    end_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    start_date = end_date - timedelta(days=14)

    interactions = pd.read_csv(DATA_DIR / "interactions.csv", parse_dates=["last_watch_dt"])

    mask = (interactions["last_watch_dt"].dt.date > start_date) & (
        interactions["last_watch_dt"].dt.date <= end_date
    )
    window = interactions.loc[mask]

    top = (
        window.groupby("item_id")
        .agg(n_users=("user_id", "nunique"), total_watches=("user_id", "count"))
        .sort_values("n_users", ascending=False)
        .head(args.top_k)
        .reset_index()
    )
    top["rank"] = range(1, len(top) + 1)

    output_path = Path(args.output) if args.output else DATA_DIR / f"top_popular_{args.date}.csv"
    top.to_csv(output_path, index=False)
    print(f"Saved {len(top)} items to {output_path}")


if __name__ == "__main__":
    main()
