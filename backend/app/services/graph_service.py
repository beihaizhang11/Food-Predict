from __future__ import annotations

from datetime import datetime
from typing import Any

from flask import current_app
from neo4j import GraphDatabase
from neo4j.exceptions import AuthError, Neo4jError, ServiceUnavailable

from .factor_service import merge_factors


class GraphService:
    """
    因子驱动型图谱 schema
    - (:Shop {id})
    - (:Review {id, rating, sentiment, time, month})
    - (:Factor {name, category})

    关系：
    - (Shop)-[:HAS_REVIEW]->(Review)
    - (Review)-[:MENTIONS_FACTOR {tag, polarity, effect}]->(Factor)
      effect = 0.6 * polarity + 0.4 * review_sentiment
    """

    def __init__(self) -> None:
        self._driver = None
        self._disabled = False

    def _get_driver(self):
        if self._disabled:
            return None
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                current_app.config["NEO4J_URI"],
                auth=(current_app.config["NEO4J_USER"], current_app.config["NEO4J_PASSWORD"]),
                connection_timeout=3,
            )
        return self._driver

    def _mark_disabled(self, exc: Exception) -> None:
        self._disabled = True
        current_app.logger.warning("Neo4j unavailable, graph features disabled: %s", exc)

    def _is_query_ready(self) -> bool:
        driver = self._get_driver()
        if driver is None:
            return False
        try:
            with driver.session() as session:
                row = session.run("CALL db.labels() YIELD label RETURN collect(label) AS labels").single()
                labels = set(row["labels"] or []) if row else set()
                return {"Shop", "Review", "Factor"}.issubset(labels)
        except (ServiceUnavailable, AuthError, Neo4jError, OSError) as exc:
            self._mark_disabled(exc)
            return False

    def upsert_review_graph(self, review: Any, entities: list[dict[str, Any]]) -> None:
        driver = self._get_driver()
        if driver is None:
            return
        review_month = review.review_time.strftime("%Y-%m")
        factors = merge_factors(review.tags, entities)

        try:
            with driver.session() as session:
                session.run(
                    """
                    MERGE (s:Shop {id: $shop_id})
                    ON CREATE SET s.name = $shop_id
                    MERGE (r:Review {id: $review_id})
                    SET r.rating = $rating,
                        r.sentiment = $sentiment,
                        r.time = datetime($review_time),
                        r.month = $review_month
                    MERGE (s)-[:HAS_REVIEW]->(r)
                    """,
                    shop_id=review.shop_id,
                    review_id=int(review.id),
                    rating=float(review.rating),
                    sentiment=float(review.sentiment or 0.0),
                    review_time=review.review_time.isoformat(),
                    review_month=review_month,
                )
                # 避免 update 时关系重复累加
                session.run(
                    """
                    MATCH (r:Review {id: $review_id})-[m:MENTIONS_FACTOR]->(:Factor)
                    DELETE m
                    """,
                    review_id=int(review.id),
                )

                for item in factors:
                    polarity = float(item.get("polarity", 0.0))
                    effect = 0.6 * polarity + 0.4 * float(review.sentiment or 0.0)
                    session.run(
                        """
                        MATCH (r:Review {id: $review_id})
                        MERGE (f:Factor {name: $factor})
                        ON CREATE SET f.category = $category
                        SET f.category = coalesce(f.category, $category)
                        MERGE (r)-[m:MENTIONS_FACTOR {factor: $factor}]->(f)
                        SET m.tag = $tag,
                            m.polarity = $polarity,
                            m.effect = $effect
                        """,
                        review_id=int(review.id),
                        factor=str(item["factor"]),
                        category=str(item["category"]),
                        tag=str(item.get("tag", item["factor"])),
                        polarity=polarity,
                        effect=float(effect),
                    )
        except (ServiceUnavailable, AuthError, Neo4jError, OSError) as exc:
            self._mark_disabled(exc)
            return

    def query_graph(
        self,
        start: datetime | None,
        end: datetime | None,
        shop_id: str | None,
        dish: str | None,  # 保留参数兼容旧调用
        limit: int = 20,
        view: str = "summary",
    ) -> dict[str, list[dict[str, Any]]]:
        if self._disabled or not self._is_query_ready():
            return {"nodes": [], "edges": []}
        if view == "detail":
            return self._query_detailed(start, end, shop_id, limit=max(30, limit))
        return self._query_summary(start, end, shop_id, limit=max(6, min(limit, 80)))

    def _build_where(self, start: datetime | None, end: datetime | None, shop_id: str | None):
        where = []
        params: dict[str, Any] = {}
        if shop_id:
            where.append("s.id = $shop_id")
            params["shop_id"] = shop_id
        if start:
            where.append("r.time >= datetime($start)")
            params["start"] = start.isoformat()
        if end:
            where.append("r.time <= datetime($end)")
            params["end"] = end.isoformat()
        return ("WHERE " + " AND ".join(where)) if where else "", params

    def _query_summary(
        self, start: datetime | None, end: datetime | None, shop_id: str | None, limit: int
    ) -> dict[str, list[dict[str, Any]]]:
        where_sql, params = self._build_where(start, end, shop_id)
        params["limit"] = limit

        query = f"""
        MATCH (s:Shop)-[sr]->(r:Review)-[m]->(f:Factor)
        WHERE type(sr) = 'HAS_REVIEW' AND type(m) = 'MENTIONS_FACTOR'
        {"AND " + where_sql[6:] if where_sql else ""}
        WITH s.id AS shop_id, f.name AS factor_name, f.category AS category,
             count(m) AS mention_count, avg(m.effect) AS avg_effect, avg(r.rating) AS avg_rating
        WITH shop_id, factor_name, category, mention_count, avg_effect, avg_rating,
             abs(avg_effect) * log10(mention_count + 1) AS score
        ORDER BY score DESC
        LIMIT $limit
        RETURN shop_id, factor_name, category, mention_count, avg_effect, avg_rating
        """

        nodes: dict[str, dict[str, Any]] = {}
        edges: list[dict[str, Any]] = []

        driver = self._get_driver()
        if driver is None:
            return {"nodes": [], "edges": []}
        try:
            with driver.session() as session:
                for row in session.run(query, **params):
                    shop_node = f"shop:{row['shop_id']}"
                    factor_node = f"factor:{row['factor_name']}"
                    nodes[shop_node] = {"id": shop_node, "label": row["shop_id"], "type": "Shop"}
                    nodes[factor_node] = {
                        "id": factor_node,
                        "label": row["factor_name"],
                        "type": "Factor",
                        "category": row["category"] or "其他",
                    }
                    edges.append(
                        {
                            "source": shop_node,
                            "target": factor_node,
                            "type": "AFFECTED_BY",
                            "weight": int(row["mention_count"] or 0),
                            "impact": float(row["avg_effect"] or 0.0),
                            "rating_score": float(row["avg_rating"] or 0.0),
                        }
                    )
        except (ServiceUnavailable, AuthError, Neo4jError, OSError) as exc:
            self._mark_disabled(exc)
            return {"nodes": [], "edges": []}

        return {"nodes": list(nodes.values()), "edges": edges}

    def _query_detailed(
        self, start: datetime | None, end: datetime | None, shop_id: str | None, limit: int
    ) -> dict[str, list[dict[str, Any]]]:
        where_sql, params = self._build_where(start, end, shop_id)
        params["limit"] = min(max(limit, 30), 400)

        query = f"""
        MATCH (s:Shop)-[sr]->(r:Review)-[m]->(f:Factor)
        WHERE type(sr) = 'HAS_REVIEW' AND type(m) = 'MENTIONS_FACTOR'
        {"AND " + where_sql[6:] if where_sql else ""}
        RETURN s.id AS shop_id, r.id AS review_id, f.name AS factor_name,
               m.effect AS effect, m.tag AS tag
        LIMIT $limit
        """

        nodes: dict[str, dict[str, Any]] = {}
        edges: list[dict[str, Any]] = []
        driver = self._get_driver()
        if driver is None:
            return {"nodes": [], "edges": []}
        try:
            with driver.session() as session:
                for row in session.run(query, **params):
                    shop_node = f"shop:{row['shop_id']}"
                    review_node = f"review:{row['review_id']}"
                    factor_node = f"factor:{row['factor_name']}"
                    nodes[shop_node] = {"id": shop_node, "label": row["shop_id"], "type": "Shop"}
                    nodes[review_node] = {"id": review_node, "label": str(row["review_id"]), "type": "Review"}
                    nodes[factor_node] = {"id": factor_node, "label": row["factor_name"], "type": "Factor"}
                    edges.append({"source": shop_node, "target": review_node, "type": "HAS_REVIEW", "weight": 1})
                    edges.append(
                        {
                            "source": review_node,
                            "target": factor_node,
                            "type": "MENTIONS_FACTOR",
                            "weight": 1,
                            "impact": float(row["effect"] or 0.0),
                        }
                    )
        except (ServiceUnavailable, AuthError, Neo4jError, OSError) as exc:
            self._mark_disabled(exc)
            return {"nodes": [], "edges": []}
        return {"nodes": list(nodes.values()), "edges": edges}

    def get_graph_stats(
        self, start: datetime | None, end: datetime | None, shop_id: str | None, dish: str | None
    ) -> dict[str, int]:
        if self._disabled or not self._is_query_ready():
            return {"shops": 0, "dishes": 0, "users": 0, "rated_edges": 0}

        where_sql, params = self._build_where(start, end, shop_id)
        query = f"""
        MATCH (s:Shop)-[sr]->(r:Review)
        WHERE type(sr) = 'HAS_REVIEW'
        {"AND " + where_sql[6:] if where_sql else ""}
        OPTIONAL MATCH (r)-[m]->(f:Factor)
        WHERE type(m) = 'MENTIONS_FACTOR'
        RETURN count(DISTINCT s) AS shops,
               count(DISTINCT f) AS factors,
               count(DISTINCT r) AS reviews,
               count(m) AS factor_edges
        """
        driver = self._get_driver()
        if driver is None:
            return {"shops": 0, "dishes": 0, "users": 0, "rated_edges": 0}
        try:
            with driver.session() as session:
                row = session.run(query, **params).single()
                if row is None:
                    return {"shops": 0, "dishes": 0, "users": 0, "rated_edges": 0}
                return {
                    "shops": int(row["shops"] or 0),
                    "dishes": int(row["factors"] or 0),  # 兼容旧字段名
                    "users": int(row["reviews"] or 0),   # 兼容旧字段名
                    "rated_edges": int(row["factor_edges"] or 0),
                }
        except (ServiceUnavailable, AuthError, Neo4jError, OSError) as exc:
            self._mark_disabled(exc)
            return {"shops": 0, "dishes": 0, "users": 0, "rated_edges": 0}
