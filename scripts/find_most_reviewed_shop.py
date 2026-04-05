from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

import pandas as pd


def _read_csv_with_fallback(path: Path) -> pd.DataFrame:
    for enc in ("utf-8", "utf-8-sig", "gb18030"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("csv", b"", 0, 1, f"unable to decode file: {path}")


def _iter_rest_ids(ratings_csv: Path, chunk_size: int):
    for enc in ("utf-8", "utf-8-sig", "gb18030"):
        try:
            iterator = pd.read_csv(
                ratings_csv,
                encoding=enc,
                chunksize=chunk_size,
                usecols=["restId"],
            )
            break
        except UnicodeDecodeError:
            continue
    else:
        raise UnicodeDecodeError("csv", b"", 0, 1, f"unable to decode file: {ratings_csv}")

    for chunk in iterator:
        vals = pd.to_numeric(chunk["restId"], errors="coerce").dropna().astype(int).tolist()
        for rid in vals:
            yield rid


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find the merchant with the most reviews in ratings.csv"
    )
    parser.add_argument(
        "--ratings-csv",
        default="../ratings/ratings/ratings.csv",
        help="path to ratings.csv",
    )
    parser.add_argument(
        "--restaurants-csv",
        default="../ratings/ratings/restaurants.csv",
        help="path to restaurants.csv",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=200000,
        help="CSV chunk size when scanning ratings.csv",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=1,
        help="print top N merchants by review count",
    )
    parser.add_argument(
        "--include-empty-name",
        action="store_true",
        help="include shops whose name is empty in restaurants.csv",
    )
    args = parser.parse_args()

    ratings_csv = Path(args.ratings_csv)
    restaurants_csv = Path(args.restaurants_csv)
    if not ratings_csv.exists():
        raise FileNotFoundError(ratings_csv)
    if not restaurants_csv.exists():
        raise FileNotFoundError(restaurants_csv)

    counts = Counter(_iter_rest_ids(ratings_csv, chunk_size=max(1000, args.chunk_size)))
    if not counts:
        print("No reviews found in ratings.csv")
        return

    rest_df = _read_csv_with_fallback(restaurants_csv)
    if "restId" not in rest_df.columns or "name" not in rest_df.columns:
        raise ValueError("restaurants.csv must contain columns: restId,name")

    rest_df["restId"] = pd.to_numeric(rest_df["restId"], errors="coerce")
    rest_df = rest_df.dropna(subset=["restId"]).copy()
    rest_df["restId"] = rest_df["restId"].astype(int)
    rest_df["name"] = rest_df["name"].fillna("").astype(str).str.strip()

    rows = []
    for rest_id, review_count in counts.items():
        rows.append({"restId": int(rest_id), "review_count": int(review_count)})
    count_df = pd.DataFrame(rows)

    merged = count_df.merge(rest_df[["restId", "name"]], on="restId", how="left")
    merged["name"] = merged["name"].fillna("").astype(str).str.strip()
    if not args.include_empty_name:
        merged = merged[merged["name"] != ""]

    if merged.empty:
        print("No merchant with non-empty name found.")
        return

    merged = merged.sort_values(["review_count", "restId"], ascending=[False, True]).reset_index(drop=True)
    top_n = merged.head(max(1, args.top_n))

    print("Top merchants by review count:")
    for i, row in top_n.iterrows():
        print(
            f"{i + 1}. restId={int(row['restId'])}, "
            f"name={row['name']}, reviews={int(row['review_count'])}"
        )

    top = top_n.iloc[0]
    print(
        "\nMost reviewed merchant: "
        f"restId={int(top['restId'])}, name={top['name']}, reviews={int(top['review_count'])}"
    )


if __name__ == "__main__":
    main()
