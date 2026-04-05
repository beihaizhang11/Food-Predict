from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Any

import pandas as pd

from .factor_service import FACTOR_MAP, parse_tag_factors


def build_prediction_explanation(review_df: pd.DataFrame, history: list[dict], forecast: list[dict]) -> dict:
    if review_df.empty:
        empty_bridge = {
            "sentiment_trend": 0.0,
            "volume_trend": 0.0,
            "factor_score_trend": 0.0,
            "predicted_delta": 0.0,
            "model_side_signal": "neutral",
            "signal_alignment": "neutral",
            "structured_factor_trends": [],
        }
        return {
            "summary": "No reviews under current filters. Unable to explain prediction.",
            "predicted_change": 0.0,
            "factor_label": "factor",
            "top_positive_factors": [],
            "top_negative_factors": [],
            "top_attributes": [],
            "top_positive_attributes": [],
            "top_negative_attributes": [],
            "supporting_attributes": [],
            "opposing_attributes": [],
            "attribute_impacts": [],
            "feature_bridge": empty_bridge,
            "attribute_alignment": "neutral",
            "top_positive_dishes": [],
            "top_negative_dishes": [],
        }

    work = review_df.copy()
    work["review_time"] = pd.to_datetime(work["review_time"])
    work = work.sort_values("review_time")

    recent, previous = _split_recent_previous(work)
    predicted_change = _predicted_delta(history, forecast)
    direction = "up" if predicted_change >= 0 else "down"

    attr_freq = _attribute_frequency(recent)
    attr_rows = _attribute_contribution_rows(recent, previous)
    positive = [x for x in attr_rows if x["impact"] > 0][:8]
    negative = [x for x in sorted(attr_rows, key=lambda v: v["impact"]) if x["impact"] < 0][:8]

    if predicted_change >= 0:
        supporting = positive
        opposing = negative
    else:
        supporting = negative
        opposing = positive

    net_attr_impact = float(sum(x["impact"] for x in attr_rows))
    attr_alignment = _direction_alignment(predicted_change, net_attr_impact)
    bridge = _feature_bridge(history, forecast)

    summary = (
        f"Forecast indicates rating may go {direction} by {abs(predicted_change):.2f}. "
        f"Main supporting factors: {_join_names(supporting, 'attribute')}. "
        f"Main opposing factors: {_join_names(opposing, 'attribute')}. "
        f"High-frequency factors: {_join_names(attr_freq, 'attribute')}. "
        f"Net factor impact {net_attr_impact:+.3f} ({attr_alignment}). "
        f"Model-side signals: sentiment {bridge['sentiment_trend']:+.3f}, "
        f"volume {bridge['volume_trend']:+.3f}, factor score {bridge['factor_score_trend']:+.3f}."
    )

    return {
        "summary": summary,
        "predicted_change": round(predicted_change, 4),
        "factor_label": "factor",
        "top_positive_factors": _as_factor_rows(positive),
        "top_negative_factors": _as_factor_rows(negative),
        "top_attributes": attr_freq,
        "top_positive_attributes": positive,
        "top_negative_attributes": negative,
        "supporting_attributes": supporting,
        "opposing_attributes": opposing,
        "attribute_impacts": attr_rows[:16],
        "attribute_alignment": attr_alignment,
        "feature_bridge": bridge,
        # Backward compatibility fields.
        "top_positive_dishes": _as_factor_rows(positive),
        "top_negative_dishes": _as_factor_rows(negative),
    }


def build_workflow_metrics(
    review_df: pd.DataFrame, graph_stats: dict, prediction_metrics: dict, explanation: dict
) -> dict:
    if review_df.empty:
        return {
            "data_import": {"total_reviews": 0, "shops": 0, "time_range": "-"},
            "nlp": {"avg_sentiment": 0.0, "tag_coverage": 0.0, "top_tags": []},
            "graph": graph_stats,
            "predict": {"mae": 0.0, "rmse": 0.0, "predicted_change": 0.0},
        }

    work = review_df.copy()
    work["review_time"] = pd.to_datetime(work["review_time"])
    min_t = work["review_time"].min().strftime("%Y-%m-%d")
    max_t = work["review_time"].max().strftime("%Y-%m-%d")

    return {
        "data_import": {
            "total_reviews": int(len(work)),
            "shops": int(work["shop_id"].nunique()),
            "time_range": f"{min_t} ~ {max_t}",
        },
        "nlp": {
            "avg_sentiment": round(float(work["sentiment"].fillna(0).mean()), 4),
            "tag_coverage": round(float((work["tags"].fillna("").str.len() > 0).mean()), 4),
            "top_tags": _top_tags(work["tags"].fillna("").tolist(), topn=8),
        },
        "graph": graph_stats,
        "predict": {
            "mae": round(float(prediction_metrics.get("mae", 0.0)), 4),
            "rmse": round(float(prediction_metrics.get("rmse", 0.0)), 4),
            "predicted_change": explanation.get("predicted_change", 0.0),
        },
    }


def compute_factor_insights(
    reviews: list[Any], top_k: int = 12, min_abs_impact: float = 0.0
) -> dict[str, Any]:
    factor_stats: dict[str, dict[str, Any]] = {}
    category_stats: dict[str, dict[str, float]] = defaultdict(
        lambda: {"mention_count": 0, "sum_impact": 0.0, "sum_confidence": 0.0}
    )
    total_mentions = 0

    for review in reviews:
        for item in _review_factor_mentions(review):
            total_mentions += 1
            name = item["factor"]
            category = item["category"]
            bucket = item["time_bucket"]

            if name not in factor_stats:
                factor_stats[name] = {
                    "factor": name,
                    "category": category,
                    "mention_count": 0,
                    "sum_impact": 0.0,
                    "sum_effect": 0.0,
                    "sum_sentiment": 0.0,
                    "sum_polarity": 0.0,
                    "sum_confidence": 0.0,
                    "timeline": defaultdict(lambda: {"count": 0, "impact": 0.0}),
                }
            row = factor_stats[name]
            row["mention_count"] += 1
            row["sum_impact"] += item["impact"]
            row["sum_effect"] += item["effect"]
            row["sum_sentiment"] += item["sentiment"]
            row["sum_polarity"] += item["polarity"]
            row["sum_confidence"] += item["confidence"]
            row["timeline"][bucket]["count"] += 1
            row["timeline"][bucket]["impact"] += item["impact"]

            category_stats[category]["mention_count"] += 1
            category_stats[category]["sum_impact"] += item["impact"]
            category_stats[category]["sum_confidence"] += item["confidence"]

    factors: list[dict[str, Any]] = []
    for item in factor_stats.values():
        count = max(1, int(item["mention_count"]))
        avg_impact = item["sum_impact"] / count
        net_impact = avg_impact * math.log2(count + 1)
        if abs(net_impact) < min_abs_impact:
            continue

        timeline_rows = []
        for bucket, t in sorted(item["timeline"].items(), key=lambda x: x[0]):
            timeline_rows.append(
                {"bucket": bucket, "impact": round(float(t["impact"]), 4), "count": int(t["count"])}
            )

        factors.append(
            {
                "factor": item["factor"],
                "category": item["category"],
                "mention_count": count,
                "avg_impact": round(float(avg_impact), 4),
                "net_impact": round(float(net_impact), 4),
                "avg_effect": round(float(item["sum_effect"] / count), 4),
                "avg_sentiment": round(float(item["sum_sentiment"] / count), 4),
                "avg_polarity": round(float(item["sum_polarity"] / count), 4),
                "confidence": round(float(item["sum_confidence"] / count), 4),
                "direction": "positive" if net_impact >= 0 else "negative",
                "timeline": timeline_rows,
            }
        )

    factors.sort(key=lambda x: abs(float(x["net_impact"])), reverse=True)
    if top_k > 0:
        factors = factors[:top_k]

    categories = []
    for name, row in category_stats.items():
        count = max(1, int(row["mention_count"]))
        avg_impact = row["sum_impact"] / count
        categories.append(
            {
                "category": name,
                "mention_count": int(row["mention_count"]),
                "avg_impact": round(float(avg_impact), 4),
                "net_impact": round(float(avg_impact * math.log2(count + 1)), 4),
                "confidence": round(float(row["sum_confidence"] / count), 4),
            }
        )
    categories.sort(key=lambda x: abs(float(x["net_impact"])), reverse=True)

    top_positive = [f for f in factors if float(f["net_impact"]) >= 0][:5]
    top_negative = [f for f in factors if float(f["net_impact"]) < 0][:5]

    return {
        "factors": factors,
        "categories": categories,
        "top_positive": top_positive,
        "top_negative": top_negative,
        "total_mentions": int(total_mentions),
        "factor_count": len(factors),
    }


def compute_factor_evidence(
    reviews: list[Any], factor: str | None, page: int = 1, size: int = 20
) -> dict[str, Any]:
    evidence_rows: list[dict[str, Any]] = []
    selected = (factor or "").strip()

    for review in reviews:
        for item in _review_factor_mentions(review):
            if selected and item["factor"] != selected:
                continue
            text = str(getattr(review, "review_text", "") or "")
            snippet = text[:180] + ("..." if len(text) > 180 else "")
            evidence_rows.append(
                {
                    "review_id": int(getattr(review, "id", 0)),
                    "shop_id": str(getattr(review, "shop_id", "")),
                    "review_time": getattr(review, "review_time").isoformat(),
                    "rating": round(float(getattr(review, "rating", 0.0) or 0.0), 4),
                    "sentiment": round(float(getattr(review, "sentiment", 0.0) or 0.0), 4),
                    "factor": item["factor"],
                    "category": item["category"],
                    "tag": item["tag"],
                    "polarity": round(float(item["polarity"]), 4),
                    "effect": round(float(item["effect"]), 4),
                    "impact": round(float(item["impact"]), 4),
                    "confidence": round(float(item["confidence"]), 4),
                    "time_bucket": item["time_bucket"],
                    "snippet": snippet,
                }
            )

    evidence_rows.sort(key=lambda x: abs(float(x["impact"])), reverse=True)
    total = len(evidence_rows)
    page = max(1, int(page))
    size = max(1, min(100, int(size)))
    start = (page - 1) * size
    end = start + size
    paged = evidence_rows[start:end]

    net_impact = float(sum(x["impact"] for x in evidence_rows))
    avg_confidence = float(sum(x["confidence"] for x in evidence_rows) / total) if total else 0.0
    positive = sum(1 for x in evidence_rows if float(x["impact"]) >= 0)
    negative = total - positive

    return {
        "items": paged,
        "pagination": {"page": page, "size": size, "total": total},
        "summary": {
            "factor": selected or None,
            "net_impact": round(net_impact, 4),
            "avg_confidence": round(avg_confidence, 4),
            "positive_count": int(positive),
            "negative_count": int(negative),
        },
    }


def _split_recent_previous(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df, df
    window = max(20, int(len(df) * 0.3))
    recent = df.tail(window)
    previous = df.iloc[max(0, len(df) - 2 * window) : max(0, len(df) - window)]
    return recent, previous


def _attribute_frequency(recent: pd.DataFrame) -> list[dict]:
    tags = _explode_tags(recent["tags"].fillna("").tolist())
    counter = Counter(tags)
    return [{"attribute": k, "count": v} for k, v in counter.most_common(8)]


def _attribute_contribution_rows(recent: pd.DataFrame, previous: pd.DataFrame) -> list[dict]:
    recent_tags = _explode_tags(recent["tags"].fillna("").tolist())
    all_attrs = set(recent_tags)
    if not all_attrs:
        return []

    total_recent_mentions = max(1, len(recent_tags))
    rows: list[dict[str, Any]] = []
    for attr in all_attrs:
        meta = FACTOR_MAP.get(attr)
        polarity = float(meta.polarity if meta else 0.0)

        r_subset = recent[recent["tags"].fillna("").astype(str).str.contains(attr, regex=False)]
        p_subset = previous[previous["tags"].fillna("").astype(str).str.contains(attr, regex=False)]

        r_count = int(len(r_subset))
        p_count = int(len(p_subset))
        if r_count == 0:
            continue

        r_sent = float(pd.to_numeric(r_subset["sentiment"], errors="coerce").fillna(0.0).mean())
        p_sent = (
            float(pd.to_numeric(p_subset["sentiment"], errors="coerce").fillna(0.0).mean())
            if p_count
            else r_sent
        )
        r_rating = float(pd.to_numeric(r_subset["rating"], errors="coerce").fillna(0.0).mean())
        p_rating = (
            float(pd.to_numeric(p_subset["rating"], errors="coerce").fillna(0.0).mean())
            if p_count
            else r_rating
        )

        recent_effect = 0.7 * polarity + 0.3 * r_sent
        prev_effect = 0.7 * polarity + 0.3 * p_sent
        effect_delta = recent_effect - prev_effect
        rating_delta = r_rating - p_rating
        count_delta_ratio = (r_count - p_count) / max(p_count, 1)
        mention_weight = math.log2(r_count + 1)
        exposure = r_count / total_recent_mentions
        impact = (0.75 * recent_effect + 0.25 * rating_delta) * mention_weight * (0.7 + 0.3 * exposure)

        rows.append(
            {
                "attribute": attr,
                "category": meta.category if meta else "other",
                "polarity": round(polarity, 4),
                "effect": round(float(recent_effect), 4),
                "delta": round(float(effect_delta), 4),
                "rating_delta": round(float(rating_delta), 4),
                "impact": round(float(impact), 4),
                "count": r_count,
                "count_delta_ratio": round(float(count_delta_ratio), 4),
            }
        )

    rows.sort(key=lambda x: abs(x["impact"]), reverse=True)
    return rows


def _as_factor_rows(rows: list[dict]) -> list[dict]:
    out = []
    for item in rows:
        out.append(
            {
                "factor": item["attribute"],
                "impact": item["impact"],
                "delta": item["delta"],
                "count": item["count"],
            }
        )
    return out


def _top_tags(raw_tags: list[str], topn: int = 8) -> list[dict]:
    counter = Counter(_explode_tags(raw_tags))
    return [{"tag": k, "count": v} for k, v in counter.most_common(topn)]


def _explode_tags(raw_tags: list[str]) -> list[str]:
    tags: list[str] = []
    for raw in raw_tags:
        tags.extend([x.strip() for x in str(raw).split(",") if x.strip()])
    return tags


def _predicted_delta(history: list[dict], forecast: list[dict]) -> float:
    if not history or not forecast:
        return 0.0
    return float(forecast[0].get("rating", 0.0)) - float(history[-1].get("rating", 0.0))


def _feature_bridge(history: list[dict], forecast: list[dict]) -> dict:
    if not history:
        return {
            "sentiment_trend": 0.0,
            "volume_trend": 0.0,
            "factor_score_trend": 0.0,
            "predicted_delta": 0.0,
            "model_side_signal": "neutral",
            "signal_alignment": "neutral",
            "structured_factor_trends": [],
        }

    h = pd.DataFrame(history)
    pred_delta = _predicted_delta(history, forecast)

    def window_delta(col: str) -> float:
        if col not in h.columns:
            return 0.0
        vals = pd.to_numeric(h[col], errors="coerce").fillna(0.0).tolist()
        if len(vals) < 6:
            return float(vals[-1] - vals[0]) if len(vals) >= 2 else 0.0
        recent = sum(vals[-3:]) / 3.0
        prev = sum(vals[-6:-3]) / 3.0
        return float(recent - prev)

    sentiment_trend = window_delta("sentiment")
    volume_trend = window_delta("review_count")
    factor_score_trend = window_delta("factor_score")

    structured = []
    for col, name in (
        ("rating_env", "env"),
        ("rating_flavor", "flavor"),
        ("rating_service", "service"),
    ):
        if col in h.columns:
            structured.append({"name": name, "delta": round(window_delta(col), 4)})

    signal_score = (
        0.45 * sentiment_trend
        + 0.15 * volume_trend * 0.1
        + 0.25 * factor_score_trend
        + 0.15 * sum(x["delta"] for x in structured)
    )
    if signal_score > 0.02:
        signal = "positive"
    elif signal_score < -0.02:
        signal = "negative"
    else:
        signal = "neutral"

    return {
        "sentiment_trend": round(sentiment_trend, 4),
        "volume_trend": round(volume_trend, 4),
        "factor_score_trend": round(factor_score_trend, 4),
        "predicted_delta": round(pred_delta, 4),
        "model_side_signal": signal,
        "signal_alignment": _direction_alignment(pred_delta, signal_score),
        "structured_factor_trends": structured,
    }


def _direction_alignment(predicted_delta: float, driver_score: float) -> str:
    if abs(predicted_delta) < 0.03 and abs(driver_score) < 0.03:
        return "neutral"
    if predicted_delta >= 0 and driver_score >= 0:
        return "aligned-up"
    if predicted_delta < 0 and driver_score < 0:
        return "aligned-down"
    return "conflict"


def _join_names(items: list[dict], key: str) -> str:
    if not items:
        return "none"
    return " / ".join(str(x.get(key, "")) for x in items[:3]) or "none"


def _review_factor_mentions(review: Any) -> list[dict[str, Any]]:
    tags_text = str(getattr(review, "tags", "") or "")
    sentiment = float(getattr(review, "sentiment", 0.0) or 0.0)
    rating = float(getattr(review, "rating", 0.0) or 0.0)
    review_time = getattr(review, "review_time")
    bucket = review_time.strftime("%Y-%m") if review_time else "-"

    mentions: list[dict[str, Any]] = []
    for item in parse_tag_factors(tags_text):
        polarity = float(item.get("polarity", 0.0))
        effect = 0.7 * polarity + 0.3 * sentiment
        rating_signal = max(-1.0, min(1.0, (rating - 3.0) / 2.0))
        impact = effect * (0.75 + 0.25 * rating_signal)
        confidence = _mention_confidence(polarity, sentiment, tags_text)
        mentions.append(
            {
                "factor": str(item.get("factor", "")),
                "category": str(item.get("category", "Other")),
                "tag": str(item.get("tag", "")),
                "polarity": polarity,
                "sentiment": sentiment,
                "effect": effect,
                "impact": impact,
                "confidence": confidence,
                "time_bucket": bucket,
            }
        )
    return mentions


def _mention_confidence(polarity: float, sentiment: float, tags_text: str) -> float:
    tag_count = len([x for x in str(tags_text).split(",") if x.strip()])
    score = 0.35 + min(0.3, abs(polarity) * 0.3) + min(0.2, abs(sentiment) * 0.2) + min(
        0.15, tag_count * 0.03
    )
    return float(max(0.0, min(1.0, score)))
