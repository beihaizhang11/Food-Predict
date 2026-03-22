import argparse
import datetime as dt
import json
import random
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ShopProfile:
    shop_id: str
    style: str
    dishes: list[str]
    base_rating: float
    dish_bias: dict[str, float]


SHOP_PROFILES = [
    ShopProfile(
        shop_id="S101",
        style="川湘家常菜",
        dishes=["宫保鸡丁", "酸菜鱼", "麻婆豆腐", "鱼香肉丝", "回锅肉", "毛血旺"],
        base_rating=4.15,
        dish_bias={"酸菜鱼": 0.20, "宫保鸡丁": 0.15, "毛血旺": -0.05},
    ),
    ShopProfile(
        shop_id="S102",
        style="日式简餐",
        dishes=["寿司拼盘", "豚骨拉面", "照烧鸡饭", "鳗鱼饭", "可乐饼", "炸鸡块"],
        base_rating=4.00,
        dish_bias={"鳗鱼饭": 0.20, "豚骨拉面": 0.12, "可乐饼": -0.08},
    ),
]

ATTR_TEMPLATES = {
    "口味": ["口味很稳", "味道在线", "有点偏咸", "辣度刚好", "偏油一点"],
    "服务": ["服务态度不错", "出餐很快", "高峰期等得久", "店员很耐心", "打包仔细"],
    "环境": ["环境干净", "堂食有点挤", "座位舒适", "卫生不错", "噪音偏大"],
    "价格": ["性价比高", "价格略贵", "分量很足", "价格实惠", "活动后很划算"],
}

TAG_POLARITY = {
    "口味很好": 0.9,
    "服务好": 0.7,
    "环境干净": 0.6,
    "性价比高": 0.7,
    "分量足": 0.6,
    "出餐快": 0.6,
    "口味一般": -0.2,
    "偏咸": -0.3,
    "偏油": -0.2,
    "等太久": -0.7,
    "价格偏高": -0.5,
    "环境嘈杂": -0.4,
}

POSITIVE_TAGS = ["口味很好", "服务好", "环境干净", "性价比高", "分量足", "出餐快"]
NEGATIVE_TAGS = ["口味一般", "偏咸", "偏油", "等太久", "价格偏高", "环境嘈杂"]


def pick_review_time(start: dt.date, end: dt.date) -> dt.datetime:
    span_days = (end - start).days
    day = start + dt.timedelta(days=random.randint(0, span_days))

    # 高峰分布：午餐和晚餐更集中，周末稍晚
    peak = random.random()
    if peak < 0.45:
        hour = random.choice([11, 12, 13])
    elif peak < 0.85:
        hour = random.choice([18, 19, 20])
    else:
        hour = random.choice([9, 10, 14, 15, 16, 21])

    if day.weekday() >= 5 and hour < 11:
        hour += 1

    return dt.datetime(day.year, day.month, day.day, hour, random.randint(0, 59), random.randint(0, 59))


def compose_tags(score_seed: float) -> list[str]:
    tags = []
    pos_count = 2 if score_seed >= 4.2 else 1
    neg_count = 2 if score_seed <= 3.2 else (1 if score_seed <= 3.6 else 0)

    for _ in range(pos_count):
        tags.append(random.choice(POSITIVE_TAGS))
    for _ in range(neg_count):
        tags.append(random.choice(NEGATIVE_TAGS))

    if len(tags) < 2:
        tags.append(random.choice(POSITIVE_TAGS if random.random() > 0.35 else NEGATIVE_TAGS))

    random.shuffle(tags)
    # 去重后保留顺序
    return list(dict.fromkeys(tags))[:3]


def synthesize_review_text(shop: ShopProfile, dish: str, tags: list[str]) -> str:
    snippets = [
        random.choice(ATTR_TEMPLATES["口味"]),
        random.choice(ATTR_TEMPLATES["服务"]),
        random.choice(ATTR_TEMPLATES["环境"]),
        random.choice(ATTR_TEMPLATES["价格"]),
    ]
    random.shuffle(snippets)
    return (
        f"{shop.style}这家店点了{dish}。"
        f"{snippets[0]}，{snippets[1]}。"
        f"整体感觉：{'、'.join(tags)}。"
    )


def build_record(idx: int, shop: ShopProfile, users: list[str], start: dt.date, end: dt.date) -> dict:
    dish = random.choice(shop.dishes)
    time_value = pick_review_time(start, end)

    # 评分：门店基线 + 菜品偏置 + 标签情绪 + 随机噪声
    base = shop.base_rating + shop.dish_bias.get(dish, 0.0)
    tags = compose_tags(base)
    tag_score = sum(TAG_POLARITY.get(t, 0.0) for t in tags) / max(len(tags), 1)
    noisy = random.normalvariate(0.0, 0.28)
    rating = max(1.0, min(5.0, round(base + tag_score * 0.5 + noisy, 1)))

    return {
        "id": idx,
        "user_id": random.choice(users),
        "shop_id": shop.shop_id,
        "dish": dish,
        "rating": rating,
        "review_text": synthesize_review_text(shop, dish, tags),
        "review_time": time_value.strftime("%Y-%m-%d %H:%M:%S"),
        "tags": tags,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shop-count", type=int, default=2, choices=[1, 2])
    parser.add_argument("--per-shop", type=int, default=1000)
    parser.add_argument("--output", type=str, default="data/mock_reviews.json")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-date", type=str, default="2023-01-01")
    parser.add_argument("--end-date", type=str, default="2026-03-01")
    args = parser.parse_args()

    random.seed(args.seed)
    start = dt.datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end = dt.datetime.strptime(args.end_date, "%Y-%m-%d").date()

    profiles = SHOP_PROFILES[: args.shop_count]
    rows = []
    row_id = 1
    for shop in profiles:
        # 每家店约 300~500 回头客用户，更贴近真实外卖点评
        users = [f"U{shop.shop_id[-1]}{i:04d}" for i in range(1, 401)]
        for _ in range(args.per_shop):
            rows.append(build_record(row_id, shop, users, start, end))
            row_id += 1

    rows.sort(key=lambda x: x["review_time"])
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"generated {len(rows)} rows | shops={args.shop_count} | per_shop={args.per_shop} -> {output}"
    )


if __name__ == "__main__":
    main()
