from datetime import datetime
from typing import Any

from flask import current_app
from neo4j import GraphDatabase


class GraphService:
    def __init__(self) -> None:
        self._driver = None

    def _get_driver(self):
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                current_app.config["NEO4J_URI"],
                auth=(current_app.config["NEO4J_USER"], current_app.config["NEO4J_PASSWORD"]),
            )
        return self._driver

    def upsert_review_graph(self, review: Any, entities: list[dict[str, Any]]) -> None:
        query = """
        MERGE (u:User {id: $user_id})
        ON CREATE SET u.name = $user_id
        MERGE (s:Shop {id: $shop_id})
        ON CREATE SET s.name = $shop_id
        MERGE (d:Dish {name: $dish})
        MERGE (s)-[:OFFERS]->(d)
        MERGE (u)-[r:RATED {review_id: $review_id}]->(d)
        SET r.rating = $rating, r.time = datetime($review_time), r.sentiment = $sentiment
        """
        driver = self._get_driver()
        with driver.session() as session:
            session.run(
                query,
                user_id=review.user_id,
                shop_id=review.shop_id,
                dish=review.dish,
                review_id=int(review.id),
                rating=float(review.rating),
                review_time=review.review_time.isoformat(),
                sentiment=float(review.sentiment or 0.0),
            )
            for entity in entities:
                if entity.get("type") == "ATTRIBUTE":
                    session.run(
                        """
                        MERGE (d:Dish {name: $dish})
                        MERGE (a:Attribute {name: $attr})
                        MERGE (d)-[:HAS_ATTRIBUTE]->(a)
                        """,
                        dish=review.dish,
                        attr=str(entity.get("text")),
                    )

    def query_graph(
        self,
        start: datetime | None,
        end: datetime | None,
        shop_id: str | None,
        dish: str | None,
        limit: int = 20,
        view: str = "summary",
    ) -> dict[str, list[dict[str, Any]]]:
        return (
            self._query_detailed(start, end, shop_id, dish, limit)
            if view == "detail"
            else self._query_summary(start, end, shop_id, dish, limit)
        )

    def _build_where(self, start, end, shop_id, dish):
        where_clauses = []
        params: dict[str, Any] = {}
        if shop_id:
            where_clauses.append("s.id = $shop_id")
            params["shop_id"] = shop_id
        if dish:
            where_clauses.append("d.name = $dish")
            params["dish"] = dish
        if start:
            where_clauses.append("r.time >= datetime($start)")
            params["start"] = start.isoformat()
        if end:
            where_clauses.append("r.time <= datetime($end)")
            params["end"] = end.isoformat()
        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        return where_sql, params

    def _query_summary(
        self, start: datetime | None, end: datetime | None, shop_id: str | None, dish: str | None, limit: int
    ) -> dict[str, list[dict[str, Any]]]:
        where_sql, params = self._build_where(start, end, shop_id, dish)
        params["limit"] = max(5, min(limit, 60))

        # 聚合视图：Shop-Dish 按评论量与均分展示，避免用户节点爆炸
        top_dish_query = f"""
        MATCH (u:User)-[r:RATED]->(d:Dish)<-[:OFFERS]-(s:Shop)
        {where_sql}
        WITH s.id AS shop_id, d.name AS dish_name,
             count(r) AS review_count, avg(r.rating) AS avg_rating
        ORDER BY review_count DESC
        LIMIT $limit
        RETURN shop_id, dish_name, review_count, avg_rating
        """

        attr_query = """
        MATCH (d:Dish)-[:HAS_ATTRIBUTE]->(a:Attribute)
        WHERE d.name IN $dish_names
        RETURN d.name AS dish_name, a.name AS attr_name, count(*) AS strength
        """

        nodes: dict[str, dict[str, Any]] = {}
        edges: list[dict[str, Any]] = []
        dish_names = []

        driver = self._get_driver()
        with driver.session() as session:
            top_rows = list(session.run(top_dish_query, **params))
            for row in top_rows:
                shop = f"shop:{row['shop_id']}"
                dish_node = f"dish:{row['dish_name']}"
                dish_names.append(row["dish_name"])
                nodes[shop] = {"id": shop, "label": row["shop_id"], "type": "Shop"}
                nodes[dish_node] = {
                    "id": dish_node,
                    "label": row["dish_name"],
                    "type": "Dish",
                    "score": round(float(row["avg_rating"] or 0.0), 2),
                }
                edges.append(
                    {
                        "source": shop,
                        "target": dish_node,
                        "type": "OFFERS",
                        "weight": int(row["review_count"] or 0),
                    }
                )

            if dish_names:
                attr_rows = session.run(attr_query, dish_names=dish_names)
                for row in attr_rows:
                    dish_node = f"dish:{row['dish_name']}"
                    attr_node = f"attr:{row['attr_name']}"
                    nodes[attr_node] = {
                        "id": attr_node,
                        "label": row["attr_name"],
                        "type": "Attribute",
                    }
                    edges.append(
                        {
                            "source": dish_node,
                            "target": attr_node,
                            "type": "HAS_ATTRIBUTE",
                            "weight": int(row["strength"] or 1),
                        }
                    )
        return {"nodes": list(nodes.values()), "edges": edges}

    def get_graph_stats(
        self, start: datetime | None, end: datetime | None, shop_id: str | None, dish: str | None
    ) -> dict[str, int]:
        where_sql, params = self._build_where(start, end, shop_id, dish)
        query = f"""
        MATCH (u:User)-[r:RATED]->(d:Dish)<-[:OFFERS]-(s:Shop)
        {where_sql}
        RETURN count(DISTINCT s) as shops, count(DISTINCT d) as dishes,
               count(DISTINCT u) as users, count(r) as rated_edges
        """
        driver = self._get_driver()
        with driver.session() as session:
            row = session.run(query, **params).single()
            if row is None:
                return {"shops": 0, "dishes": 0, "users": 0, "rated_edges": 0}
            return {
                "shops": int(row["shops"] or 0),
                "dishes": int(row["dishes"] or 0),
                "users": int(row["users"] or 0),
                "rated_edges": int(row["rated_edges"] or 0),
            }

    def _query_detailed(
        self, start: datetime | None, end: datetime | None, shop_id: str | None, dish: str | None, limit: int
    ) -> dict[str, list[dict[str, Any]]]:
        where_sql, params = self._build_where(start, end, shop_id, dish)
        params["limit"] = max(20, min(limit, 500))
        query = f"""
        MATCH (u:User)-[r:RATED]->(d:Dish)<-[:OFFERS]-(s:Shop)
        {where_sql}
        RETURN u.id as user_id, s.id as shop_id, d.name as dish_name,
               r.rating as rating, r.sentiment as sentiment
        LIMIT $limit
        """
        nodes: dict[str, dict[str, Any]] = {}
        edges: list[dict[str, Any]] = []
        driver = self._get_driver()
        with driver.session() as session:
            for row in session.run(query, **params):
                user = f"user:{row['user_id']}"
                shop = f"shop:{row['shop_id']}"
                dish_node = f"dish:{row['dish_name']}"
                nodes[user] = {"id": user, "label": row["user_id"], "type": "User"}
                nodes[shop] = {"id": shop, "label": row["shop_id"], "type": "Shop"}
                nodes[dish_node] = {"id": dish_node, "label": row["dish_name"], "type": "Dish"}
                edges.append({"source": user, "target": dish_node, "type": "RATED", "weight": 1})
                edges.append({"source": shop, "target": dish_node, "type": "OFFERS", "weight": 1})
        return {"nodes": list(nodes.values()), "edges": edges}
