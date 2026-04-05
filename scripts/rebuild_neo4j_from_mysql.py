from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pymysql
from neo4j import GraphDatabase

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


@dataclass(frozen=True)
class FactorMeta:
    name: str
    category: str
    polarity: float


FACTOR_MAP: dict[str, FactorMeta] = {
    "口味很好": FactorMeta("口味", "产品体验", 0.90),
    "口味正常": FactorMeta("口味", "产品体验", 0.10),
    "口味一般": FactorMeta("口味", "产品体验", -0.45),
    "辣度偏高": FactorMeta("口味", "产品体验", -0.20),
    "咸度偏高": FactorMeta("口味", "产品体验", -0.30),
    "甜度偏高": FactorMeta("口味", "产品体验", -0.20),
    "油腻感偏高": FactorMeta("口味", "产品体验", -0.35),
    "服务好": FactorMeta("服务", "服务效率", 0.80),
    "服务正常": FactorMeta("服务", "服务效率", 0.10),
    "服务一般": FactorMeta("服务", "服务效率", -0.40),
    "出餐快": FactorMeta("出餐效率", "服务效率", 0.65),
    "等待时间长": FactorMeta("等待时长", "服务效率", -0.65),
    "环境干净": FactorMeta("环境", "门店体验", 0.70),
    "环境正常": FactorMeta("环境", "门店体验", 0.10),
    "环境一般": FactorMeta("环境", "门店体验", -0.35),
    "卫生干净": FactorMeta("卫生", "门店体验", 0.75),
    "卫生一般": FactorMeta("卫生", "门店体验", -0.70),
    "价格实惠": FactorMeta("价格", "价值感知", 0.60),
    "价格偏高": FactorMeta("价格", "价值感知", -0.55),
    "分量足": FactorMeta("分量", "价值感知", 0.55),
    "分量偏少": FactorMeta("分量", "价值感知", -0.45),
    "复购意愿高": FactorMeta("复购意愿", "用户忠诚", 0.65),
    "复购意愿低": FactorMeta("复购意愿", "用户忠诚", -0.75),
    "推荐度高": FactorMeta("推荐度", "用户忠诚", 0.75),
    "推荐度低": FactorMeta("推荐度", "用户忠诚", -0.85),
}


INIT_CONSTRAINTS = [
    "CREATE CONSTRAINT shop_id IF NOT EXISTS FOR (s:Shop) REQUIRE s.id IS UNIQUE",
    "CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.id IS UNIQUE",
    "CREATE CONSTRAINT factor_name IF NOT EXISTS FOR (f:Factor) REQUIRE f.name IS UNIQUE",
]


UPSERT_BATCH_QUERY = """
UNWIND $rows AS row
MERGE (s:Shop {id: row.shop_id})
ON CREATE SET s.name = row.shop_id
MERGE (r:Review {id: row.review_id})
SET r.rating = row.rating,
    r.sentiment = row.sentiment,
    r.time = datetime(row.review_time),
    r.month = row.review_month
MERGE (s)-[:HAS_REVIEW]->(r)
WITH r, row
OPTIONAL MATCH (r)-[old:MENTIONS_FACTOR]->(:Factor)
DELETE old
WITH r, row
UNWIND row.factors AS f
MERGE (factor:Factor {name: f.factor})
ON CREATE SET factor.category = f.category
SET factor.category = coalesce(factor.category, f.category)
MERGE (r)-[m:MENTIONS_FACTOR {factor: f.factor}]->(factor)
SET m.tag = f.tag,
    m.polarity = f.polarity,
    m.effect = f.effect
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild Neo4j factor graph from MySQL reviews table."
    )
    parser.add_argument("--batch-size", type=int, default=500, help="Neo4j write batch size")
    parser.add_argument("--limit", type=int, default=-1, help="Max reviews to process; -1 means all")
    parser.add_argument("--start-id", type=int, default=0, help="Only process reviews.id >= start-id")
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear current Neo4j database before rebuilding (MATCH (n) DETACH DELETE n)",
    )
    parser.add_argument(
        "--skip-constraints",
        action="store_true",
        help="Skip creating Neo4j constraints",
    )
    return parser.parse_args()


def to_iso_string(value: Any) -> str:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.replace(" ", "T")
        dt = datetime.fromisoformat(text)
    else:
        raise ValueError(f"Unsupported review_time type: {type(value)}")
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def parse_factors(tags_text: str, sentiment: float) -> list[dict[str, Any]]:
    factors_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in str(tags_text or "").split(","):
        tag = raw.strip()
        if not tag:
            continue
        meta = FACTOR_MAP.get(tag, FactorMeta(tag, "其他体验", 0.0))
        key = (meta.name, meta.category)
        polarity = float(meta.polarity)
        effect = 0.6 * polarity + 0.4 * float(sentiment)
        candidate = {
            "factor": meta.name,
            "category": meta.category,
            "tag": tag,
            "polarity": polarity,
            "effect": float(effect),
        }
        if key not in factors_by_key:
            factors_by_key[key] = candidate
            continue
        old = factors_by_key[key]
        if abs(candidate["polarity"]) > abs(old["polarity"]):
            factors_by_key[key] = candidate
    return list(factors_by_key.values())


def iter_reviews(mysql_conn, start_id: int, limit: int):
    sql = """
    SELECT id, shop_id, rating, sentiment, review_time, tags
    FROM reviews
    WHERE id >= %s
    ORDER BY id ASC
    """
    with mysql_conn.cursor(pymysql.cursors.SSCursor) as cursor:
        cursor.execute(sql, (start_id,))
        count = 0
        for row in cursor:
            if limit >= 0 and count >= limit:
                break
            count += 1
            yield {
                "review_id": int(row[0]),
                "shop_id": str(row[1]),
                "rating": float(row[2] if row[2] is not None else 0.0),
                "sentiment": float(row[3] if row[3] is not None else 0.0),
                "review_time": to_iso_string(row[4]),
                "review_month": (
                    row[4].strftime("%Y-%m")
                    if isinstance(row[4], datetime)
                    else datetime.fromisoformat(str(row[4]).replace(" ", "T")).strftime("%Y-%m")
                ),
                "factors": parse_factors(row[5], float(row[3] if row[3] is not None else 0.0)),
            }


def main() -> None:
    if load_dotenv is not None:
        load_dotenv()

    args = parse_args()
    mysql_conn = pymysql.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DATABASE", "restaurant_analytics"),
        charset="utf8mb4",
        autocommit=True,
    )
    neo4j_driver = GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687"),
        auth=(os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "neo4j")),
    )

    try:
        with neo4j_driver.session() as session:
            if args.clear:
                session.run("MATCH (n) DETACH DELETE n")
                print("Neo4j cleared.")

            if not args.skip_constraints:
                for stmt in INIT_CONSTRAINTS:
                    session.run(stmt)
                print("Neo4j constraints ensured.")

            batch: list[dict[str, Any]] = []
            total = 0
            for payload in iter_reviews(mysql_conn, start_id=args.start_id, limit=args.limit):
                batch.append(payload)
                if len(batch) < args.batch_size:
                    continue
                session.run(UPSERT_BATCH_QUERY, rows=batch)
                total += len(batch)
                print(f"processed={total}")
                batch.clear()

            if batch:
                session.run(UPSERT_BATCH_QUERY, rows=batch)
                total += len(batch)

            print(f"done. total_processed={total}")
    finally:
        mysql_conn.close()
        neo4j_driver.close()


if __name__ == "__main__":
    main()
