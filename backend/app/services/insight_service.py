from __future__ import annotations

import math
from collections import Counter, defaultdict
from typing import Any

import pandas as pd

from .factor_service import FACTOR_MAP


def build_prediction_explanation(review_df: pd.DataFrame, history: list[dict], forecast: list[dict]) -> dict:
    if review_df.empty:
        return {
            "summary": "当前筛选条件下没有评论数据，无法生成趋势解释。",
            "predicted_change": 0.0,
            "factor_label": "店铺",
            "top_positive_factors": [],
            "top_negative_factors": [],
            "top_attributes": [],
            "top_positive_attributes": [],
            "top_negative_attributes": [],
            "feature_bridge": {
                "sentiment_trend": 0.0,
                "volume_trend": 0.0,
                "predicted_delta": 0.0,
                "model_side_signal": "中性",
                "structured_factor_trends": [],
            },
        }

    work = review_df.copy()
    work["review_time"] = pd.to_datetime(work["review_time"])
    work = work.sort_values("review_time")

    recent, previous = _split_recent_previous(work)
    use_dish = _has_effective_dish_dimension(recent)
    group_col = "dish" if use_dish else "shop_id"
    factor_label = "菜品" if use_dish else "店铺"

    factor_drivers = _group_drivers(recent, previous, group_col, output_key="factor")
    attr_freq = _attribute_frequency(recent)
    attr_drivers = _attribute_sentiment_drivers(recent, previous)

    bridge = _feature_bridge(history, forecast)
    predicted_change = _predicted_delta(history, forecast)
    direction = "上升" if predicted_change >= 0 else "下降"

    summary = (
        f"模型预测下一阶段评分{direction}{abs(predicted_change):.2f}。"
        f"主要正向驱动{factor_label}：{_join_names(factor_drivers['positive'], 'factor')}；"
        f"主要负向驱动{factor_label}：{_join_names(factor_drivers['negative'], 'factor')}；"
        f"高频属性关注点：{_join_names(attr_freq, 'attribute')}。"
        f"模型输入侧信号：情感{bridge['sentiment_trend']:+.3f}，评论量{bridge['volume_trend']:+.3f}，"
        f"综合判断{bridge['model_side_signal']}。"
    )

    return {
        "summary": summary,
        "predicted_change": round(predicted_change, 4),
        "factor_label": factor_label,
        "top_positive_factors": factor_drivers["positive"],
        "top_negative_factors": factor_drivers["negative"],
        "top_attributes": attr_freq,
        "top_positive_attributes": attr_drivers["positive"],
        "top_negative_attributes": attr_drivers["negative"],
        "feature_bridge": bridge,
        # Backward compatibility
        "top_positive_dishes": factor_drivers["positive"],
        "top_negative_dishes": factor_drivers["negative"],
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


def _split_recent_previous(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df, df
    window = max(12, int(len(df) * 0.25))
    recent = df.tail(window)
    previous = df.iloc[max(0, len(df) - 2 * window) : max(0, len(df) - window)]
    return recent, previous


def _has_effective_dish_dimension(df: pd.DataFrame) -> bool:
    if df.empty or "dish" not in df.columns or "shop_id" not in df.columns:
        return False

    dish_values = df["dish"].dropna().astype(str).str.strip()
    if dish_values.empty:
        return False

    unique_dishes = dish_values.nunique()
    if unique_dishes <= 1:
        return False

    # If dish is effectively 1:1 mapped to shop (common in datasets without real dish field),
    # we should not treat dish as an explanatory dimension.
    mapping = (
        df[["shop_id", "dish"]]
        .dropna()
        .astype(str)
        .drop_duplicates()
    )
    if (
        len(mapping) > 0
        and mapping["shop_id"].nunique() == mapping["dish"].nunique() == len(mapping)
    ):
        return False

    top_ratio = dish_values.value_counts(normalize=True).head(1).iloc[0]
    return top_ratio < 0.9


def _group_drivers(recent: pd.DataFrame, previous: pd.DataFrame, group_col: str, output_key: str) -> dict:
    if recent.empty:
        return {"positive": [], "negative": []}

    r = recent.groupby(group_col).agg(avg_rating=("rating", "mean"), cnt=("rating", "count"))
    if previous.empty:
        p = pd.DataFrame(columns=["avg_rating_prev", "cnt_prev"])
    else:
        p = previous.groupby(group_col).agg(avg_rating_prev=("rating", "mean"), cnt_prev=("rating", "count"))

    joined = r.join(p, how="left")
    fallback = float(r["avg_rating"].mean()) if not r.empty else 0.0
    joined = joined.fillna({"avg_rating_prev": fallback, "cnt_prev": 0})
    joined["delta"] = joined["avg_rating"] - joined["avg_rating_prev"]
    joined["weight"] = joined["cnt"].apply(lambda x: math.log2(max(2, x)))
    joined["impact"] = joined["delta"] * joined["weight"]

    rows = [
        {
            output_key: str(idx),
            "delta": round(float(row["delta"]), 4),
            "impact": round(float(row["impact"]), 4),
            "count": int(row["cnt"]),
        }
        for idx, row in joined.iterrows()
    ]

    rows = sorted(rows, key=lambda x: x["impact"], reverse=True)
    positives = [x for x in rows if x["impact"] > 0][:5]
    negatives = [x for x in sorted(rows, key=lambda x: x["impact"]) if x["impact"] < 0][:5]
    return {"positive": positives, "negative": negatives}


def _attribute_frequency(recent: pd.DataFrame) -> list[dict]:
    tags = _explode_tags(recent["tags"].fillna("").tolist())
    counter = Counter(tags)
    return [{"attribute": k, "count": v} for k, v in counter.most_common(8)]


def _attribute_sentiment_drivers(recent: pd.DataFrame, previous: pd.DataFrame) -> dict:
    def score_by_attr(df: pd.DataFrame) -> dict[str, list[float]]:
        mapping: dict[str, list[float]] = defaultdict(list)
        for _, row in df.iterrows():
            sentiment = float(row.get("sentiment", 0.0))
            for tag in _explode_tags([str(row.get("tags", ""))]):
                base = FACTOR_MAP.get(tag).polarity if tag in FACTOR_MAP else 0.0
                mapping[tag].append(0.6 * base + 0.4 * sentiment)
        return mapping

    recent_map = score_by_attr(recent)
    prev_map = score_by_attr(previous)
    rows: list[dict[str, Any]] = []

    for attr, vals in recent_map.items():
        recent_avg = sum(vals) / len(vals)
        prev_vals = prev_map.get(attr, vals)
        prev_avg = sum(prev_vals) / len(prev_vals)
        delta = recent_avg - prev_avg

        recent_count = len(vals)
        prev_count = len(prev_vals)
        count_delta_ratio = (recent_count - prev_count) / max(prev_count, 1)
        impact = delta * math.log2(max(2, recent_count)) + 0.35 * count_delta_ratio

        rows.append(
            {
                "attribute": attr,
                "delta": round(float(delta), 4),
                "impact": round(float(impact), 4),
                "count": recent_count,
                "count_delta_ratio": round(float(count_delta_ratio), 4),
            }
        )

    rows = sorted(rows, key=lambda x: x["impact"], reverse=True)
    return {
        "positive": [x for x in rows if x["impact"] > 0][:8],
        "negative": [x for x in sorted(rows, key=lambda x: x["impact"]) if x["impact"] < 0][:8],
    }


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
            "predicted_delta": 0.0,
            "model_side_signal": "中性",
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

    structured = []
    for col, name in (
        ("rating_env", "环境评分"),
        ("rating_flavor", "口味评分"),
        ("rating_service", "服务评分"),
    ):
        if col in h.columns:
            structured.append({"name": name, "delta": round(window_delta(col), 4)})

    signal_score = 0.6 * sentiment_trend + 0.25 * volume_trend * 0.1 + 0.15 * sum(
        x["delta"] for x in structured
    )
    if signal_score > 0.02:
        signal = "偏正向"
    elif signal_score < -0.02:
        signal = "偏负向"
    else:
        signal = "中性"

    return {
        "sentiment_trend": round(sentiment_trend, 4),
        "volume_trend": round(volume_trend, 4),
        "predicted_delta": round(pred_delta, 4),
        "model_side_signal": signal,
        "structured_factor_trends": structured,
    }


def _join_names(items: list[dict], key: str) -> str:
    if not items:
        return "无"
    return "、".join(str(x.get(key, "")) for x in items[:3]) or "无"
