from __future__ import annotations

import argparse
import hashlib
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app import create_app
from app.services.graph_service import GraphService
from app.services.import_service import ImportService
from app.services.nlp_service import NlpService


KEYWORD_TAGS: list[tuple[list[str], str]] = [
    (["便宜", "实惠", "划算", "性价比", "优惠"], "价格实惠"),
    (["贵", "涨价", "偏高", "不值"], "价格偏高"),
    (["干净", "卫生", "整洁"], "卫生干净"),
    (["脏", "不卫生", "异味"], "卫生一般"),
    (["排队", "等位", "等太久", "慢"], "等待时间长"),
    (["上菜快", "出餐快", "很快"], "出餐快"),
    (["分量足", "量大", "管饱"], "分量足"),
    (["分量少", "量少"], "分量偏少"),
    (["回头", "常来", "再来", "复购"], "复购意愿高"),
    (["不会再来", "踩雷"], "复购意愿低"),
    (["推荐", "值得", "好评"], "推荐度高"),
    (["不推荐", "避雷", "差评"], "推荐度低"),
]

TASTE_PATTERNS: list[tuple[str, str]] = [
    (r"(太?辣|麻辣|辣味足)", "辣度偏高"),
    (r"(太?咸|咸了)", "咸度偏高"),
    (r"(偏甜|太甜)", "甜度偏高"),
    (r"(太?油|油腻)", "油腻感偏高"),
]


@dataclass
class ShopMeta:
    rest_id: int
    name: str


def _read_csv_with_fallback(path: Path, usecols=None):
    for enc in ("utf-8", "utf-8-sig", "gb18030"):
        try:
            return pd.read_csv(path, encoding=enc, usecols=usecols)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("csv", b"", 0, 1, f"unable to decode file: {path}")


def _iter_csv_with_fallback(path: Path, chunk_size: int, usecols=None):
    for enc in ("utf-8", "utf-8-sig", "gb18030"):
        try:
            return pd.read_csv(path, encoding=enc, chunksize=chunk_size, usecols=usecols), enc
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("csv", b"", 0, 1, f"unable to decode file: {path}")


def _load_shop(restaurants_csv: Path, rest_id: int) -> ShopMeta:
    df = _read_csv_with_fallback(restaurants_csv, usecols=["restId", "name"])
    df["restId"] = pd.to_numeric(df["restId"], errors="coerce")
    df = df.dropna(subset=["restId"])
    df["restId"] = df["restId"].astype(int)
    df["name"] = df["name"].fillna("").astype(str).str.strip()

    row = df[df["restId"] == rest_id]
    if row.empty:
        raise ValueError(f"restId={rest_id} not found in restaurants.csv")

    name = row.iloc[0]["name"]
    if not name:
        raise ValueError(f"restId={rest_id} has empty name and will be skipped by project rules")
    return ShopMeta(rest_id=rest_id, name=name)


def _score_to_tag(value, pos_tag: str, neg_tag: str, neutral_tag: str) -> str | None:
    if pd.isna(value):
        return None
    score = float(value)
    if score >= 4:
        return pos_tag
    if score <= 2:
        return neg_tag
    return neutral_tag


def _tags_from_comment(comment: str) -> list[str]:
    text = comment.strip()
    if not text:
        return []
    out = []
    for words, tag in KEYWORD_TAGS:
        if any(w in text for w in words):
            out.append(tag)
    for pattern, tag in TASTE_PATTERNS:
        if re.search(pattern, text):
            out.append(tag)
    return out


def _build_tags(row: pd.Series, comment: str) -> list[str]:
    tags: list[str] = []
    flavor_tag = _score_to_tag(row.get("rating_flavor"), "口味很好", "口味一般", "口味正常")
    service_tag = _score_to_tag(row.get("rating_service"), "服务好", "服务一般", "服务正常")
    env_tag = _score_to_tag(row.get("rating_env"), "环境干净", "环境一般", "环境正常")
    for t in (flavor_tag, service_tag, env_tag):
        if t:
            tags.append(t)
    tags.extend(_tags_from_comment(comment))
    return list(dict.fromkeys(tags))[:8]


def _rating_value(row: pd.Series) -> float:
    if pd.notna(row.get("rating")):
        return float(row["rating"])
    sub_scores = [row.get("rating_env"), row.get("rating_flavor"), row.get("rating_service")]
    vals = [float(x) for x in sub_scores if pd.notna(x)]
    return round(sum(vals) / len(vals), 1) if vals else 3.0


def _parse_dt(ts_value) -> datetime | None:
    if pd.isna(ts_value):
        return None
    try:
        return pd.to_datetime(int(float(ts_value)), unit="ms").to_pydatetime()
    except Exception:
        try:
            return pd.to_datetime(ts_value).to_pydatetime()
        except Exception:
            return None


def _stable_review_id(
    rest_id: int,
    user_id: int,
    timestamp_raw,
    comment: str,
    rating: float,
) -> int:
    key = f"mt|{rest_id}|{user_id}|{timestamp_raw}|{comment}|{rating:.3f}"
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()
    n = int(digest[:12], 16) % 2_147_483_000 + 1
    return -n  # negative ID space avoids collision with existing positive IDs


def _iter_shop_rows(
    ratings_csv: Path,
    shop: ShopMeta,
    chunk_size: int,
    max_rows: int | None,
    since: datetime | None,
) -> Iterator[dict]:
    usecols = [
        "userId",
        "restId",
        "rating",
        "rating_env",
        "rating_flavor",
        "rating_service",
        "timestamp",
        "comment",
    ]
    iterator, enc = _iter_csv_with_fallback(ratings_csv, chunk_size=chunk_size, usecols=usecols)
    print(f"reading ratings.csv with encoding={enc}")

    emitted = 0
    for chunk in iterator:
        chunk["restId"] = pd.to_numeric(chunk["restId"], errors="coerce")
        sub = chunk[chunk["restId"] == shop.rest_id]
        if sub.empty:
            continue

        for _, row in sub.iterrows():
            if max_rows is not None and emitted >= max_rows:
                return
            if pd.isna(row.get("userId")):
                continue

            dt = _parse_dt(row.get("timestamp"))
            if dt is None:
                continue
            if since and dt <= since:
                continue

            comment = str(row.get("comment") or "").strip()
            tags = _build_tags(row, comment)
            rating = _rating_value(row)
            review_text = comment if comment else f"{shop.name}。{'、'.join(tags) if tags else '综合评价正常'}。"
            user = int(float(row["userId"]))

            yield {
                "id": _stable_review_id(shop.rest_id, user, row.get("timestamp"), comment, rating),
                "user_id": f"U{user}",
                "shop_id": f"R{shop.rest_id}",
                "dish": "N/A",
                "rating": rating,
                "rating_env": float(row["rating_env"]) if pd.notna(row.get("rating_env")) else None,
                "rating_flavor": float(row["rating_flavor"]) if pd.notna(row.get("rating_flavor")) else None,
                "rating_service": float(row["rating_service"]) if pd.notna(row.get("rating_service")) else None,
                "review_text": review_text,
                "review_time": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "tags": tags,
            }
            emitted += 1


def _parse_since(since_text: str | None) -> datetime | None:
    if not since_text:
        return None
    try:
        return pd.to_datetime(since_text).to_pydatetime()
    except Exception as exc:
        raise ValueError(f"invalid --since value: {since_text}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Incrementally import one merchant's reviews into MySQL + Neo4j workflow."
    )
    parser.add_argument("--rest-id", type=int, default=173066, help="target restId in ratings.csv")
    parser.add_argument("--ratings-csv", default="../ratings/ratings/ratings.csv", help="path to ratings.csv")
    parser.add_argument(
        "--restaurants-csv",
        default="../ratings/ratings/restaurants.csv",
        help="path to restaurants.csv",
    )
    parser.add_argument("--chunk-size", type=int, default=200000, help="CSV read chunk size")
    parser.add_argument("--max-rows", type=int, default=-1, help="max rows to import; -1 means all")
    parser.add_argument(
        "--since",
        type=str,
        default="",
        help="only import reviews after this datetime (e.g. 2011-01-01 or 2011-01-01 00:00:00)",
    )
    parser.add_argument("--dry-run", action="store_true", help="only count and preview, do not write DB")
    args = parser.parse_args()

    ratings_csv = Path(args.ratings_csv)
    restaurants_csv = Path(args.restaurants_csv)
    if not ratings_csv.exists():
        raise FileNotFoundError(ratings_csv)
    if not restaurants_csv.exists():
        raise FileNotFoundError(restaurants_csv)

    shop = _load_shop(restaurants_csv, args.rest_id)
    since = _parse_since(args.since)
    max_rows = None if args.max_rows is not None and args.max_rows < 0 else args.max_rows

    if args.dry_run:
        total = 0
        sample = []
        for row in _iter_shop_rows(ratings_csv, shop, args.chunk_size, max_rows, since):
            total += 1
            if len(sample) < 3:
                sample.append(row)
        print(f"dry-run only | shop=R{shop.rest_id} ({shop.name}) | rows={total}")
        for i, item in enumerate(sample, start=1):
            print(
                f"sample#{i}: id={item['id']} user={item['user_id']} "
                f"time={item['review_time']} tags={item['tags']}"
            )
        return

    app = create_app()
    with app.app_context():
        service = ImportService(nlp_service=NlpService(), graph_service=GraphService())
        rows = _iter_shop_rows(ratings_csv, shop, args.chunk_size, max_rows, since)
        report = service.import_reviews(rows)
        print(
            f"shop=R{shop.rest_id} ({shop.name}) "
            f"total={report.total} imported={report.imported} "
            f"updated={report.updated} failed={report.failed}"
        )
        if report.errors:
            print("sample errors:")
            for err in report.errors[:10]:
                print(f"- {err}")


if __name__ == "__main__":
    main()
