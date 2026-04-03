import argparse
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app import create_app
from app.services.graph_service import GraphService
from app.services.import_service import ImportService
from app.services.nlp_service import NlpService


def _read_csv_with_fallback(path: Path) -> pd.DataFrame:
    for enc in ("utf-8", "gb18030", "utf-8-sig"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("csv", b"", 0, 1, f"unable to decode file: {path}")


def _load_valid_restaurants(restaurants_csv: Path) -> dict[int, str]:
    df = _read_csv_with_fallback(restaurants_csv)
    if "restId" not in df.columns or "name" not in df.columns:
        raise ValueError("restaurants.csv must contain columns: restId,name")
    df["name"] = df["name"].fillna("").astype(str).str.strip()
    df = df[df["name"] != ""]
    return {int(row["restId"]): row["name"] for _, row in df.iterrows()}


KEYWORD_TAGS = [
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
    tags = []
    for words, tag in KEYWORD_TAGS:
        if any(w in text for w in words):
            tags.append(tag)
    # 强化“辣/咸/甜/油”等口味细分
    taste_map = [
        (r"(太?辣|麻辣|辣味足)", "辣度偏高"),
        (r"(太?咸|咸了)", "咸度偏高"),
        (r"(偏甜|太甜)", "甜度偏高"),
        (r"(太?油|油腻)", "油腻感偏高"),
    ]
    for pattern, tag in taste_map:
        if re.search(pattern, text):
            tags.append(tag)
    return tags


def _build_tags(row: pd.Series, comment: str) -> list[str]:
    tags: list[str] = []

    flavor_tag = _score_to_tag(row.get("rating_flavor"), "口味很好", "口味一般", "口味正常")
    service_tag = _score_to_tag(row.get("rating_service"), "服务好", "服务一般", "服务正常")
    env_tag = _score_to_tag(row.get("rating_env"), "环境干净", "环境一般", "环境正常")
    for t in (flavor_tag, service_tag, env_tag):
        if t:
            tags.append(t)

    tags.extend(_tags_from_comment(comment))
    # 去重并限制长度，避免标签太多干扰解释
    tags = list(dict.fromkeys(tags))
    return tags[:8]


def _rating_value(row: pd.Series) -> float:
    if pd.notna(row.get("rating")):
        return float(row["rating"])
    sub_scores = [row.get("rating_env"), row.get("rating_flavor"), row.get("rating_service")]
    vals = [float(x) for x in sub_scores if pd.notna(x)]
    if not vals:
        return 3.0
    return round(sum(vals) / len(vals), 1)


def _iter_rows_from_ratings(
    ratings_csv: Path,
    rest_name_map: dict[int, str],
    chunk_size: int,
    max_rows: int | None,
):
    imported = 0
    used_encoding = None
    for enc in ("utf-8", "gb18030", "utf-8-sig"):
        try:
            iterator = pd.read_csv(ratings_csv, chunksize=chunk_size, encoding=enc)
            used_encoding = enc
            break
        except UnicodeDecodeError:
            continue
    else:
        raise UnicodeDecodeError("csv", b"", 0, 1, f"unable to decode file: {ratings_csv}")

    print(f"reading ratings.csv with encoding={used_encoding}")
    for chunk in iterator:
        for _, row in chunk.iterrows():
            if max_rows is not None and imported >= max_rows:
                return

            if pd.isna(row.get("restId")) or pd.isna(row.get("userId")):
                continue
            rest_id = int(row["restId"])
            if rest_id not in rest_name_map:
                continue

            ts = row.get("timestamp")
            if pd.isna(ts):
                continue
            try:
                dt = pd.to_datetime(int(float(ts)), unit="ms")
            except Exception:
                continue

            shop_name = rest_name_map[rest_id]
            comment = str(row.get("comment") or "").strip()
            tags = _build_tags(row, comment)
            review_text = (
                comment
                if comment
                else f"{shop_name}：{'、'.join(tags) if tags else '综合评价正常'}。"
            )

            yield {
                "id": int(imported + 1),
                "user_id": f"U{int(row['userId'])}",
                "shop_id": f"R{rest_id}",
                "dish": "N/A",  # 真实数据集无菜品字段，统一占位避免误解
                "rating": _rating_value(row),
                "rating_env": float(row["rating_env"]) if pd.notna(row.get("rating_env")) else None,
                "rating_flavor": float(row["rating_flavor"]) if pd.notna(row.get("rating_flavor")) else None,
                "rating_service": float(row["rating_service"]) if pd.notna(row.get("rating_service")) else None,
                "review_text": review_text,
                "review_time": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "tags": tags,
            }
            imported += 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, help="JSON reviews file path")
    parser.add_argument("--ratings-csv", type=str, help="ratings.csv path")
    parser.add_argument("--restaurants-csv", type=str, help="restaurants.csv path")
    parser.add_argument("--chunk-size", type=int, default=20000)
    parser.add_argument(
        "--max-rows",
        type=int,
        default=5000,
        help="max rows imported from ratings.csv; use -1 for all",
    )
    args = parser.parse_args()

    use_json = bool(args.file)
    use_csv = bool(args.ratings_csv and args.restaurants_csv)
    if use_json == use_csv:
        raise ValueError("use either --file OR (--ratings-csv and --restaurants-csv)")

    app = create_app()
    with app.app_context():
        service = ImportService(NlpService(), GraphService())
        if use_json:
            file_path = Path(args.file)
            if not file_path.exists():
                raise FileNotFoundError(file_path)
            rows = service.load_json_file(str(file_path))
            report = service.import_reviews(rows)
        else:
            ratings_csv = Path(args.ratings_csv)
            restaurants_csv = Path(args.restaurants_csv)
            if not ratings_csv.exists():
                raise FileNotFoundError(ratings_csv)
            if not restaurants_csv.exists():
                raise FileNotFoundError(restaurants_csv)

            rest_name_map = _load_valid_restaurants(restaurants_csv)
            print(f"valid restaurants with non-empty name: {len(rest_name_map)}")
            max_rows = None if args.max_rows is not None and args.max_rows < 0 else args.max_rows
            rows = _iter_rows_from_ratings(
                ratings_csv=ratings_csv,
                rest_name_map=rest_name_map,
                chunk_size=args.chunk_size,
                max_rows=max_rows,
            )
            report = service.import_reviews(rows)

        print(
            f"total={report.total} imported={report.imported} "
            f"updated={report.updated} failed={report.failed}"
        )
        if report.errors:
            print("sample errors:")
            for err in report.errors[:5]:
                print(f"- {err}")


if __name__ == "__main__":
    main()
