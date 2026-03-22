import json
import re
from dataclasses import dataclass
from typing import Any

from flask import current_app
from sentence_transformers import SentenceTransformer
from transformers import pipeline


@dataclass
class NlpResult:
    sentiment: float
    entities: list[dict[str, Any]]
    keywords: list[str]
    embedding: list[float]


class NlpService:
    def __init__(self) -> None:
        self._sbert = None
        self._sentiment_pipe = None
        self._ner_pipe = None
        self.attribute_keywords = {
            "服务": ["服务", "态度", "店员", "上菜"],
            "价格": ["价格", "便宜", "实惠", "贵", "性价比"],
            "环境": ["环境", "卫生", "嘈杂", "干净", "装修"],
            "口味": ["好吃", "难吃", "咸", "辣", "甜", "鲜"],
        }

    def _load_models(self) -> None:
        if self._sbert is None:
            self._sbert = SentenceTransformer(current_app.config["SBERT_MODEL"])
        if self._sentiment_pipe is None:
            self._sentiment_pipe = pipeline(
                "sentiment-analysis", model=current_app.config["SENTIMENT_MODEL"]
            )
        if self._ner_pipe is None:
            self._ner_pipe = pipeline("ner", model=current_app.config["NER_MODEL"])

    def analyze(self, text: str) -> NlpResult:
        clean_text = re.sub(r"\s+", " ", text.strip())
        if not clean_text:
            return NlpResult(sentiment=0.0, entities=[], keywords=[], embedding=[])

        self._load_models()
        embedding = self._sbert.encode(clean_text).tolist()
        sentiment_raw = self._sentiment_pipe(clean_text)[0]
        sentiment = self._normalize_sentiment(sentiment_raw)

        entities = self._extract_entities(clean_text)
        keywords = self._extract_keywords(clean_text)

        return NlpResult(
            sentiment=sentiment, entities=entities, keywords=keywords, embedding=embedding
        )

    def _normalize_sentiment(self, raw: dict[str, Any]) -> float:
        label = str(raw.get("label", "")).lower()
        score = float(raw.get("score", 0.5))
        if "negative" in label or label.endswith("_0"):
            return -score
        if "neutral" in label or label.endswith("_1"):
            return 0.0
        return score

    def _extract_entities(self, text: str) -> list[dict[str, Any]]:
        found = []
        try:
            ner_results = self._ner_pipe(text)
            for item in ner_results:
                found.append(
                    {
                        "type": item.get("entity", "UNKNOWN"),
                        "text": item.get("word", ""),
                        "score": float(item.get("score", 0.0)),
                    }
                )
        except Exception:
            found = []

        for attr, words in self.attribute_keywords.items():
            for w in words:
                if w in text:
                    found.append({"type": "ATTRIBUTE", "text": attr, "score": 1.0})
                    break
        return found

    def _extract_keywords(self, text: str) -> list[str]:
        words = [w for w in re.split(r"[，。,.!?！？\s]+", text) if len(w) >= 2]
        freq: dict[str, int] = {}
        for word in words:
            freq[word] = freq.get(word, 0) + 1
        top = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:8]
        return [x[0] for x in top]

    @staticmethod
    def serialize_embedding(values: list[float]) -> str:
        return json.dumps(values, ensure_ascii=False)

    @staticmethod
    def serialize_dict_list(values: list[dict[str, Any]]) -> str:
        return json.dumps(values, ensure_ascii=False)

    @staticmethod
    def serialize_string_list(values: list[str]) -> str:
        return json.dumps(values, ensure_ascii=False)
