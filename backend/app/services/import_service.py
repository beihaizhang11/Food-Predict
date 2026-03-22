import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import select

from ..extensions import db
from ..models import AnalysisCache, Review
from ..utils import parse_datetime
from .graph_service import GraphService
from .nlp_service import NlpService


@dataclass
class ImportReport:
    total: int
    imported: int
    updated: int
    failed: int
    errors: list[str]


class ImportService:
    def __init__(self, nlp_service: NlpService, graph_service: GraphService) -> None:
        self.nlp_service = nlp_service
        self.graph_service = graph_service

    def import_reviews(self, rows: Iterable[dict[str, Any]]) -> ImportReport:
        imported = 0
        updated = 0
        failed = 0
        errors: list[str] = []

        for row in rows:
            try:
                payload = self._validate_row(row)
                review = db.session.get(Review, payload["id"])
                is_update = review is not None
                if review is None:
                    review = Review(id=payload["id"])
                    db.session.add(review)

                review.user_id = payload["user_id"]
                review.shop_id = payload["shop_id"]
                review.dish = payload["dish"]
                review.rating = payload["rating"]
                review.review_text = payload["review_text"]
                review.review_time = payload["review_time"]
                review.tags = payload["tags"]

                analysis = self.nlp_service.analyze(review.review_text)
                review.sentiment = analysis.sentiment
                cache = db.session.get(AnalysisCache, review.id)
                if cache is None:
                    cache = AnalysisCache(review_id=review.id)
                    db.session.add(cache)
                cache.embedding_json = self.nlp_service.serialize_embedding(analysis.embedding)
                cache.entities_json = self.nlp_service.serialize_dict_list(analysis.entities)
                cache.keywords_json = self.nlp_service.serialize_string_list(analysis.keywords)

                db.session.flush()
                self.graph_service.upsert_review_graph(review, analysis.entities)

                if is_update:
                    updated += 1
                else:
                    imported += 1
            except Exception as exc:
                failed += 1
                errors.append(str(exc))
                db.session.rollback()
            else:
                db.session.commit()

        total = imported + updated + failed
        return ImportReport(
            total=total, imported=imported, updated=updated, failed=failed, errors=errors
        )

    @staticmethod
    def load_json_file(file_path: str) -> list[dict[str, Any]]:
        with open(file_path, "r", encoding="utf-8") as fp:
            data = json.load(fp)
        if not isinstance(data, list):
            raise ValueError("JSON must be a list of review objects")
        return data

    def fetch_reviews(
        self,
        start: datetime | None,
        end: datetime | None,
        shop_id: str | None,
        dish: str | None,
        page: int,
        size: int,
    ) -> tuple[list[Review], int]:
        stmt = select(Review)
        if start is not None:
            stmt = stmt.where(Review.review_time >= start)
        if end is not None:
            stmt = stmt.where(Review.review_time <= end)
        if shop_id:
            stmt = stmt.where(Review.shop_id == shop_id)
        if dish:
            stmt = stmt.where(Review.dish == dish)

        total = db.session.execute(stmt).scalars().all()
        items = (
            db.session.execute(
                stmt.order_by(Review.review_time.desc()).offset((page - 1) * size).limit(size)
            )
            .scalars()
            .all()
        )
        return items, len(total)

    def _validate_row(self, row: dict[str, Any]) -> dict[str, Any]:
        required = ["id", "user_id", "shop_id", "dish", "rating", "review_text", "review_time"]
        missing = [k for k in required if k not in row]
        if missing:
            raise ValueError(f"missing fields: {missing}")

        tags = row.get("tags", [])
        if isinstance(tags, list):
            tags_str = ",".join(map(str, tags))
        else:
            tags_str = str(tags)

        return {
            "id": int(row["id"]),
            "user_id": str(row["user_id"])[:32],
            "shop_id": str(row["shop_id"])[:32],
            "dish": str(row["dish"])[:64],
            "rating": float(row["rating"]),
            "review_text": str(row["review_text"]),
            "review_time": parse_datetime(str(row["review_time"])),
            "tags": tags_str[:255],
        }
