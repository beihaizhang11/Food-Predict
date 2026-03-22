from datetime import datetime

import pandas as pd
from flask import Blueprint, current_app, jsonify, request
from sqlalchemy import select

from ..extensions import db
from ..models import Review
from ..services.graph_service import GraphService
from ..services.insight_service import build_prediction_explanation, build_workflow_metrics
from ..services.import_service import ImportService
from ..services.nlp_service import NlpService
from ..services.prediction_service import PredictionService
from ..services.workflow_service import get_workflow
from ..utils import parse_datetime

api_bp = Blueprint("api", __name__)

nlp_service = NlpService()
graph_service = GraphService()
import_service = ImportService(nlp_service=nlp_service, graph_service=graph_service)


def _parse_date(name: str) -> datetime | None:
    value = request.args.get(name)
    if not value:
        return None
    return parse_datetime(value)


@api_bp.get("/reviews")
def get_reviews():
    try:
        start = _parse_date("start")
        end = _parse_date("end")
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    shop_id = request.args.get("shop_id")
    dish = request.args.get("dish")
    page = max(1, int(request.args.get("page", 1)))
    size = min(200, max(1, int(request.args.get("size", 20))))
    rows, total = import_service.fetch_reviews(start, end, shop_id, dish, page, size)
    return jsonify(
        {
            "items": [
                {
                    "id": r.id,
                    "user_id": r.user_id,
                    "shop_id": r.shop_id,
                    "dish": r.dish,
                    "rating": r.rating,
                    "review_text": r.review_text,
                    "review_time": r.review_time.isoformat(),
                    "tags": r.tags,
                    "sentiment": r.sentiment,
                }
                for r in rows
            ],
            "pagination": {"page": page, "size": size, "total": total},
        }
    )


@api_bp.post("/import")
def import_reviews():
    payload = request.get_json(silent=True) or {}
    rows = payload.get("reviews")
    if rows is None:
        return jsonify({"error": "request body must include 'reviews'"}), 400
    report = import_service.import_reviews(rows)
    return jsonify(
        {
            "total": report.total,
            "imported": report.imported,
            "updated": report.updated,
            "failed": report.failed,
            "errors": report.errors[:20],
        }
    )


@api_bp.get("/graph")
def get_graph():
    try:
        start = _parse_date("start")
        end = _parse_date("end")
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    shop_id = request.args.get("shop_id")
    dish = request.args.get("dish")
    limit = int(request.args.get("limit", 20))
    view = request.args.get("view", "summary")
    graph = graph_service.query_graph(start, end, shop_id, dish, limit=limit, view=view)
    return jsonify(graph)


@api_bp.post("/predict")
def predict():
    payload = request.get_json(silent=True) or {}
    granularity = payload.get("granularity", "month")
    horizon = int(payload.get("horizon", 6))
    filters = payload.get("filters", {})

    stmt = select(Review)
    if filters.get("shop_id"):
        stmt = stmt.where(Review.shop_id == filters["shop_id"])
    if filters.get("dish"):
        stmt = stmt.where(Review.dish == filters["dish"])
    try:
        if filters.get("start"):
            stmt = stmt.where(Review.review_time >= parse_datetime(filters["start"]))
        if filters.get("end"):
            stmt = stmt.where(Review.review_time <= parse_datetime(filters["end"]))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    rows = db.session.execute(stmt.order_by(Review.review_time.asc())).scalars().all()
    df = pd.DataFrame(
        [
            {
                "id": r.id,
                "review_time": r.review_time,
                "rating": float(r.rating),
                "sentiment": float(r.sentiment or 0.0),
                "dish": r.dish,
                "shop_id": r.shop_id,
                "tags": r.tags or "",
            }
            for r in rows
        ]
    )
    svc = PredictionService(
        model_dir=current_app.config["MODEL_DIR"],
        lookback=current_app.config["LSTM_LOOKBACK"],
        epochs=current_app.config["LSTM_EPOCHS"],
        batch_size=current_app.config["LSTM_BATCH_SIZE"],
    )
    result = svc.predict(df, granularity=granularity, horizon=horizon)
    explanation = build_prediction_explanation(df, result.history, result.forecast)
    return jsonify(
        {
            "history": result.history,
            "forecast": result.forecast,
            "metrics": result.metrics,
            "explanation": explanation,
        }
    )


@api_bp.get("/workflow")
def workflow():
    try:
        start = _parse_date("start")
        end = _parse_date("end")
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    shop_id = request.args.get("shop_id")
    dish = request.args.get("dish")

    stmt = select(Review)
    if shop_id:
        stmt = stmt.where(Review.shop_id == shop_id)
    if dish:
        stmt = stmt.where(Review.dish == dish)
    if start:
        stmt = stmt.where(Review.review_time >= start)
    if end:
        stmt = stmt.where(Review.review_time <= end)

    rows = db.session.execute(stmt.order_by(Review.review_time.asc())).scalars().all()
    df = pd.DataFrame(
        [
            {
                "id": r.id,
                "review_time": r.review_time,
                "rating": float(r.rating),
                "sentiment": float(r.sentiment or 0.0),
                "dish": r.dish,
                "shop_id": r.shop_id,
                "tags": r.tags or "",
            }
            for r in rows
        ]
    )
    pred_service = PredictionService(
        model_dir=current_app.config["MODEL_DIR"],
        lookback=current_app.config["LSTM_LOOKBACK"],
        epochs=min(8, current_app.config["LSTM_EPOCHS"]),
        batch_size=current_app.config["LSTM_BATCH_SIZE"],
    )
    pred = pred_service.predict(df, granularity="month", horizon=1)
    explanation = build_prediction_explanation(df, pred.history, pred.forecast)
    graph_stats = graph_service.get_graph_stats(start, end, shop_id, dish)
    metrics = build_workflow_metrics(df, graph_stats, pred.metrics, explanation)
    return jsonify(
        {
            "workflow": get_workflow(),
            "metrics": metrics,
            "explanation": explanation,
        }
    )
