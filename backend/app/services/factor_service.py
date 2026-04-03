from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class FactorMeta:
    name: str
    category: str
    polarity: float


FACTOR_MAP: dict[str, FactorMeta] = {
    # 口味
    "口味很好": FactorMeta("口味", "产品体验", 0.90),
    "口味正常": FactorMeta("口味", "产品体验", 0.10),
    "口味一般": FactorMeta("口味", "产品体验", -0.45),
    "辣度偏高": FactorMeta("口味", "产品体验", -0.20),
    "咸度偏高": FactorMeta("口味", "产品体验", -0.30),
    "甜度偏高": FactorMeta("口味", "产品体验", -0.20),
    "油腻感偏高": FactorMeta("口味", "产品体验", -0.35),
    # 服务
    "服务好": FactorMeta("服务", "服务效率", 0.80),
    "服务正常": FactorMeta("服务", "服务效率", 0.10),
    "服务一般": FactorMeta("服务", "服务效率", -0.40),
    "出餐快": FactorMeta("出餐效率", "服务效率", 0.65),
    "等待时间长": FactorMeta("等待时长", "服务效率", -0.65),
    # 环境卫生
    "环境干净": FactorMeta("环境", "门店体验", 0.70),
    "环境正常": FactorMeta("环境", "门店体验", 0.10),
    "环境一般": FactorMeta("环境", "门店体验", -0.35),
    "卫生干净": FactorMeta("卫生", "门店体验", 0.75),
    "卫生一般": FactorMeta("卫生", "门店体验", -0.70),
    # 价格与价值
    "价格实惠": FactorMeta("价格", "价值感知", 0.60),
    "价格偏高": FactorMeta("价格", "价值感知", -0.55),
    "分量足": FactorMeta("分量", "价值感知", 0.55),
    "分量偏少": FactorMeta("分量", "价值感知", -0.45),
    # 复购推荐
    "复购意愿高": FactorMeta("复购意愿", "用户忠诚", 0.65),
    "复购意愿低": FactorMeta("复购意愿", "用户忠诚", -0.75),
    "推荐度高": FactorMeta("推荐度", "用户忠诚", 0.75),
    "推荐度低": FactorMeta("推荐度", "用户忠诚", -0.85),
}


ATTRIBUTE_TO_FACTOR: dict[str, FactorMeta] = {
    "口味": FactorMeta("口味", "产品体验", 0.0),
    "服务": FactorMeta("服务", "服务效率", 0.0),
    "环境": FactorMeta("环境", "门店体验", 0.0),
    "卫生": FactorMeta("卫生", "门店体验", 0.0),
    "价格": FactorMeta("价格", "价值感知", 0.0),
}


def parse_tag_factors(tags_text: str) -> list[dict[str, Any]]:
    factors = []
    seen: set[tuple[str, str]] = set()
    for raw in str(tags_text or "").split(","):
        tag = raw.strip()
        if not tag:
            continue
        meta = FACTOR_MAP.get(tag)
        if not meta:
            # 未登记标签先归于“其他体验”，保持可追踪
            meta = FactorMeta(name=tag, category="其他体验", polarity=0.0)
        key = (meta.name, meta.category)
        if key in seen:
            continue
        seen.add(key)
        factors.append(
            {
                "factor": meta.name,
                "category": meta.category,
                "tag": tag,
                "polarity": float(meta.polarity),
            }
        )
    return factors


def parse_entity_factors(entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    seen: set[str] = set()
    for item in entities or []:
        if item.get("type") != "ATTRIBUTE":
            continue
        text = str(item.get("text", "")).strip()
        if not text:
            continue
        meta = ATTRIBUTE_TO_FACTOR.get(text, FactorMeta(text, "其他体验", 0.0))
        if meta.name in seen:
            continue
        seen.add(meta.name)
        out.append(
            {
                "factor": meta.name,
                "category": meta.category,
                "tag": text,
                "polarity": float(meta.polarity),
            }
        )
    return out


def merge_factors(tags_text: str, entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for f in parse_tag_factors(tags_text) + parse_entity_factors(entities):
        key = (f["factor"], f["category"])
        if key not in by_key:
            by_key[key] = f
        else:
            # 标签极性优先于纯实体默认极性
            if abs(float(f.get("polarity", 0.0))) > abs(float(by_key[key].get("polarity", 0.0))):
                by_key[key] = f
    return list(by_key.values())
