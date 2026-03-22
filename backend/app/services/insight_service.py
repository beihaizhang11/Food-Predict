from __future__ import annotations

import math
from collections import Counter

import pandas as pd


def build_prediction_explanation(
    review_df: pd.DataFrame, history: list[dict], forecast: list[dict]
) -> dict:
    if review_df.empty:
        return {
            "summary": "当前筛选条件下无评论数据，无法生成原因解释。",
            "predicted_change": 0.0,
            "top_positive_dishes": [],
            "top_negative_dishes": [],
            "top_attributes": [],
        }

    work = review_df.copy()
    work["review_time"] = pd.to_datetime(work["review_time"])
    work = work.sort_values("review_time")

    # 用最近90天和之前90天对比，解释“为什么会涨/跌”
    end_time = work["review_time"].max()
    recent_start = end_time - pd.Timedelta(days=90)
    prev_start = recent_start - pd.Timedelta(days=90)
    recent = work[(work["review_time"] > recent_start) & (work["review_time"] <= end_time)]
    previous = work[(work["review_time"] > prev_start) & (work["review_time"] <= recent_start)]

    dish_drivers = _dish_drivers(recent, previous)
    attr_drivers = _attribute_drivers(recent)
    predicted_change = _predicted_delta(history, forecast)
    direction = "上升" if predicted_change >= 0 else "下降"

    summary = (
        f"模型预测下一阶段评分{direction}{abs(predicted_change):.2f}。"
        f"主要正向驱动来自：{_join_names(dish_drivers['positive'])}；"
        f"主要负向驱动来自：{_join_names(dish_drivers['negative'])}；"
        f"高频属性关注点：{_join_names(attr_drivers)}。"
    )
    return {
        "summary": summary,
        "predicted_change": round(predicted_change, 4),
        "top_positive_dishes": dish_drivers["positive"],
        "top_negative_dishes": dish_drivers["negative"],
        "top_attributes": attr_drivers,
    }


def build_workflow_metrics(
    review_df: pd.DataFrame, graph_stats: dict, prediction_metrics: dict, explanation: dict
) -> dict:
    if review_df.empty:
        return {
            "data_import": {"total_reviews": 0, "shops": 0, "dishes": 0, "time_range": "-"},
            "nlp": {"avg_sentiment": 0.0, "tag_coverage": 0.0, "top_tags": []},
            "graph": graph_stats,
            "predict": {"mae": 0.0, "rmse": 0.0, "predicted_change": 0.0},
        }

    work = review_df.copy()
    work["review_time"] = pd.to_datetime(work["review_time"])
    min_t = work["review_time"].min().strftime("%Y-%m-%d")
    max_t = work["review_time"].max().strftime("%Y-%m-%d")
    total = int(len(work))
    tags_non_empty = float((work["tags"].fillna("").str.len() > 0).mean())
    top_tags = _top_tags(work["tags"].fillna("").tolist(), topn=5)

    return {
        "data_import": {
            "total_reviews": total,
            "shops": int(work["shop_id"].nunique()),
            "dishes": int(work["dish"].nunique()),
            "time_range": f"{min_t} ~ {max_t}",
        },
        "nlp": {
            "avg_sentiment": round(float(work["sentiment"].fillna(0).mean()), 4),
            "tag_coverage": round(tags_non_empty, 4),
            "top_tags": top_tags,
        },
        "graph": graph_stats,
        "predict": {
            "mae": round(float(prediction_metrics.get("mae", 0.0)), 4),
            "rmse": round(float(prediction_metrics.get("rmse", 0.0)), 4),
            "predicted_change": explanation.get("predicted_change", 0.0),
        },
    }


def _dish_drivers(recent: pd.DataFrame, previous: pd.DataFrame) -> dict:
    if recent.empty:
        return {"positive": [], "negative": []}

    r = recent.groupby("dish").agg(avg_rating=("rating", "mean"), cnt=("rating", "count"))
    p = previous.groupby("dish").agg(avg_rating_prev=("rating", "mean"), cnt_prev=("rating", "count"))
    joined = r.join(p, how="left").fillna({"avg_rating_prev": r["avg_rating"].mean(), "cnt_prev": 0})
    joined["delta"] = joined["avg_rating"] - joined["avg_rating_prev"]
    joined["weight"] = joined["cnt"].apply(lambda x: math.log2(max(2, x)))
    joined["impact"] = joined["delta"] * joined["weight"]
    rows = [
        {
            "dish": idx,
            "delta": round(float(row["delta"]), 4),
            "impact": round(float(row["impact"]), 4),
            "count": int(row["cnt"]),
        }
        for idx, row in joined.iterrows()
    ]
    rows = sorted(rows, key=lambda x: x["impact"], reverse=True)
    return {"positive": rows[:3], "negative": list(reversed(rows[-3:]))}


def _attribute_drivers(recent: pd.DataFrame) -> list[dict]:
    tags = []
    for raw in recent["tags"].fillna("").tolist():
        tags.extend([x.strip() for x in str(raw).split(",") if x.strip()])
    c = Counter(tags)
    return [{"attribute": k, "count": v} for k, v in c.most_common(5)]


def _top_tags(raw_tags: list[str], topn: int = 5) -> list[dict]:
    tags = []
    for raw in raw_tags:
        tags.extend([x.strip() for x in str(raw).split(",") if x.strip()])
    c = Counter(tags)
    return [{"tag": k, "count": v} for k, v in c.most_common(topn)]


def _predicted_delta(history: list[dict], forecast: list[dict]) -> float:
    if not history or not forecast:
        return 0.0
    return float(forecast[0].get("rating", 0.0)) - float(history[-1].get("rating", 0.0))


def _join_names(items: list[dict]) -> str:
    if not items:
        return "无"
    key = "dish" if "dish" in items[0] else "attribute"
    return "、".join(str(x.get(key, "")) for x in items[:3]) or "无"
