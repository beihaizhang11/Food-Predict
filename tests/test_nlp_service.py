from app.services.nlp_service import NlpService


class FakeSBERT:
    def encode(self, text):
        return [0.1, 0.2, 0.3]


def test_empty_text_returns_zero():
    svc = NlpService()
    result = svc.analyze("   ")
    assert result.sentiment == 0.0
    assert result.embedding == []


def test_noise_and_mixed_language_text(monkeypatch):
    svc = NlpService()
    svc._sbert = FakeSBERT()
    svc._sentiment_pipe = lambda _: [{"label": "positive", "score": 0.91}]
    svc._ner_pipe = lambda _: []
    monkeypatch.setattr(svc, "_load_models", lambda: None)
    result = svc.analyze("Great!! 服务很好!!! $$$")
    assert result.sentiment > 0
    assert "服务" in [e["text"] for e in result.entities]
