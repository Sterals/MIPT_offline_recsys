import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data"

AGE_GROUPS = [0, 6, 12, 16, 18, 21]


def main() -> None:
    parser = argparse.ArgumentParser(description="Top popular items per age group for a 2-week window")
    parser.add_argument("date", type=str, help="End date in YYYY-MM-DD format")
    parser.add_argument("--top-k", type=int, default=10, help="Number of top items per age group (default: 10)")
    parser.add_argument("--output-dir", type=str, default=None, help="Output directory (default: data/)")
    args = parser.parse_args()

    end_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    start_date = end_date - timedelta(days=14)

    interactions = pd.read_csv(DATA_DIR / "interactions.csv", parse_dates=["last_watch_dt"])
    items = pd.read_csv(DATA_DIR / "items.csv", usecols=["item_id", "age_rating"])

    mask = (interactions["last_watch_dt"].dt.date > start_date) & (
        interactions["last_watch_dt"].dt.date <= end_date
    )
    window = interactions.loc[mask]

    counts = (
        window.groupby("item_id")
        .agg(n_users=("user_id", "nunique"), total_watches=("user_id", "count"))
        .reset_index()
    )
    counts = counts.merge(items, on="item_id", how="left")

    output_dir = Path(args.output_dir) if args.output_dir else DATA_DIR

    for age in AGE_GROUPS:
        age_items = counts[counts["age_rating"].fillna(0) <= age]
        top = (
            age_items.sort_values("n_users", ascending=False)
            .head(args.top_k)
            .reset_index(drop=True)
        )
        top["rank"] = range(1, len(top) + 1)

        output_path = output_dir / f"top_popular_age_{age}_{args.date}.csv"
        top[["item_id", "n_users", "total_watches", "rank"]].to_csv(output_path, index=False)
        print(f"Age {age:>2}: saved {len(top)} items to {output_path}")


if __name__ == "__main__":
    main()
