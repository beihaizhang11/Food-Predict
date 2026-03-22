import pandas as pd

from app.services.prediction_service import PredictionService


def test_prediction_with_short_series_uses_naive(tmp_path):
    svc = PredictionService(str(tmp_path), lookback=8, epochs=1, batch_size=4)
    df = pd.DataFrame(
        [
            {"id": 1, "review_time": "2026-01-01", "rating": 4.0, "sentiment": 0.6},
            {"id": 2, "review_time": "2026-02-01", "rating": 4.1, "sentiment": 0.7},
        ]
    )
    result = svc.predict(df, granularity="month", horizon=3)
    assert len(result.forecast) == 3
    assert "mae" in result.metrics


def test_prediction_handles_missing_sentiment(tmp_path):
    svc = PredictionService(str(tmp_path), lookback=2, epochs=1, batch_size=4)
    df = pd.DataFrame(
        [
            {"id": 1, "review_time": "2025-01-01", "rating": 4.0, "sentiment": 0.1},
            {"id": 2, "review_time": "2025-02-01", "rating": 4.2, "sentiment": 0.2},
            {"id": 3, "review_time": "2025-03-01", "rating": 4.3, "sentiment": 0.0},
            {"id": 4, "review_time": "2025-04-01", "rating": 4.4, "sentiment": -0.1},
            {"id": 5, "review_time": "2025-05-01", "rating": 4.1, "sentiment": 0.1},
            {"id": 6, "review_time": "2025-06-01", "rating": 4.5, "sentiment": 0.2},
            {"id": 7, "review_time": "2025-07-01", "rating": 4.2, "sentiment": 0.2},
            {"id": 8, "review_time": "2025-08-01", "rating": 4.6, "sentiment": 0.3},
            {"id": 9, "review_time": "2025-09-01", "rating": 4.1, "sentiment": 0.0},
            {"id": 10, "review_time": "2025-10-01", "rating": 4.7, "sentiment": 0.3},
            {"id": 11, "review_time": "2025-11-01", "rating": 4.4, "sentiment": 0.2},
            {"id": 12, "review_time": "2025-12-01", "rating": 4.3, "sentiment": 0.2},
            {"id": 13, "review_time": "2026-01-01", "rating": 4.5, "sentiment": 0.3},
            {"id": 14, "review_time": "2026-02-01", "rating": 4.4, "sentiment": 0.2},
        ]
    )
    result = svc.predict(df, granularity="month", horizon=2)
    assert len(result.history) > 0
    assert len(result.forecast) == 2
