from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import MinMaxScaler

from .factor_service import FACTOR_MAP


class TrendLSTM(nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int = 48):
        super().__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, batch_first=True)
        self.fc = nn.Linear(hidden_dim, 1)

    def forward(self, x):
        out, _ = self.lstm(x)
        return self.fc(out[:, -1, :])


@dataclass
class PredictResult:
    history: list[dict]
    forecast: list[dict]
    metrics: dict[str, float]


class PredictionService:
    def __init__(self, model_dir: str, lookback: int, epochs: int, batch_size: int) -> None:
        self.model_dir = Path(model_dir)
        self.lookback = lookback
        self.epochs = epochs
        self.batch_size = batch_size
        self.model_dir.mkdir(parents=True, exist_ok=True)
        self.model_path = self.model_dir / "trend_lstm.pt"
        self.meta_path = self.model_dir / "trend_lstm_meta.json"
        self.device = torch.device("cpu")

    def predict(self, df: pd.DataFrame, granularity: str = "month", horizon: int = 6) -> PredictResult:
        series_df = self._build_series(df, granularity)
        if len(series_df) < max(12, self.lookback + 2):
            return self._naive_predict(series_df, horizon)

        feature_cols = self._feature_cols(series_df)
        X, y, scaler = self._build_dataset(series_df, feature_cols)
        split = int(len(X) * 0.8)
        X_train, y_train = X[:split], y[:split]
        X_val, y_val = X[split:], y[split:]

        model = TrendLSTM(input_dim=X.shape[2]).to(self.device)
        self._train_model(model, X_train, y_train)
        val_pred = self._predict_tensor(model, X_val)
        metrics = {
            "mae": float(mean_absolute_error(y_val, val_pred)) if len(y_val) else 0.0,
            "rmse": float(np.sqrt(mean_squared_error(y_val, val_pred))) if len(y_val) else 0.0,
        }
        self._save_model(model, scaler, feature_cols, granularity)
        forecast = self._forecast(model, scaler, series_df, feature_cols, horizon, granularity)
        history = series_df.to_dict(orient="records")
        return PredictResult(history=history, forecast=forecast, metrics=metrics)

    def _build_series(self, df: pd.DataFrame, granularity: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["time", "rating", "sentiment", "review_count"])

        work = df.copy()
        work["review_time"] = pd.to_datetime(work["review_time"])
        if "tags" in work.columns:
            work["factor_score"] = work.apply(
                lambda row: self._factor_score(row.get("tags", ""), row.get("sentiment", 0.0)),
                axis=1,
            )
        freq = "W" if granularity == "week" else "MS"

        agg_spec = {
            "rating": "mean",
            "sentiment": "mean",
            "id": "count",
        }
        for col in ("rating_env", "rating_flavor", "rating_service"):
            if col in work.columns:
                agg_spec[col] = "mean"
        if "factor_score" in work.columns:
            agg_spec["factor_score"] = "mean"

        grouped = (
            work.set_index("review_time")
            .groupby(pd.Grouper(freq=freq))
            .agg(agg_spec)
            .rename(columns={"id": "review_count"})
        )
        if grouped.empty:
            return pd.DataFrame(columns=["time", "rating", "sentiment", "review_count"])

        full_index = pd.date_range(grouped.index.min(), grouped.index.max(), freq=freq)
        grouped = grouped.reindex(full_index)
        grouped["review_count"] = grouped["review_count"].fillna(0)

        # Ensure numeric dtype before interpolation; CSV imports may keep object dtype.
        for col in grouped.columns:
            grouped[col] = pd.to_numeric(grouped[col], errors="coerce")

        grouped["review_count"] = grouped["review_count"].fillna(0.0)
        if "rating" in grouped.columns:
            grouped["rating"] = grouped["rating"].fillna(grouped["rating"].median())
        if "sentiment" in grouped.columns:
            grouped["sentiment"] = grouped["sentiment"].fillna(0.0)
        for col in ("rating_env", "rating_flavor", "rating_service"):
            if col in grouped.columns:
                median = grouped[col].median()
                grouped[col] = grouped[col].fillna(0.0 if pd.isna(median) else median)
        if "factor_score" in grouped.columns:
            grouped["factor_score"] = grouped["factor_score"].fillna(0.0)

        # 连续化 + 平滑，避免稀疏月份导致断崖
        for col in grouped.columns:
            grouped[col] = grouped[col].interpolate(method="linear", limit_direction="both")
            grouped[col] = grouped[col].rolling(3, min_periods=1).mean()

        grouped = grouped.reset_index().rename(columns={"index": "review_time"})
        grouped["time"] = grouped["review_time"].dt.strftime("%Y-%m-%d")
        base_cols = ["time", "rating", "sentiment", "review_count"]
        extra = [
            c
            for c in ("rating_env", "rating_flavor", "rating_service", "factor_score")
            if c in grouped.columns
        ]
        return grouped[base_cols + extra]

    def _feature_cols(self, series_df: pd.DataFrame) -> list[str]:
        cols = ["rating", "sentiment", "review_count"]
        for c in ("rating_env", "rating_flavor", "rating_service", "factor_score"):
            if c in series_df.columns:
                cols.append(c)
        return cols

    def _build_dataset(self, series_df: pd.DataFrame, feature_cols: list[str]):
        features = series_df[feature_cols].to_numpy(dtype=np.float32)
        scaler = MinMaxScaler()
        scaled = scaler.fit_transform(features)
        X, y = [], []
        for i in range(len(scaled) - self.lookback):
            X.append(scaled[i : i + self.lookback, :])
            y.append(scaled[i + self.lookback, 0])  # rating
        return (
            np.array(X, dtype=np.float32),
            np.array(y, dtype=np.float32).reshape(-1, 1),
            scaler,
        )

    def _train_model(self, model: TrendLSTM, X_train: np.ndarray, y_train: np.ndarray) -> None:
        x_t = torch.tensor(X_train, dtype=torch.float32).to(self.device)
        y_t = torch.tensor(y_train, dtype=torch.float32).to(self.device)
        criterion = nn.MSELoss()
        optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
        model.train()
        for _ in range(self.epochs):
            optimizer.zero_grad()
            out = model(x_t)
            loss = criterion(out, y_t)
            loss.backward()
            optimizer.step()

    def _predict_tensor(self, model: TrendLSTM, X: np.ndarray) -> np.ndarray:
        if len(X) == 0:
            return np.array([])
        model.eval()
        with torch.no_grad():
            x_t = torch.tensor(X, dtype=torch.float32).to(self.device)
            out = model(x_t).cpu().numpy().reshape(-1, 1)
        return out.reshape(-1)

    def _save_model(self, model: TrendLSTM, scaler: MinMaxScaler, feature_cols: list[str], granularity: str) -> None:
        torch.save(model.state_dict(), self.model_path)
        meta = {
            "lookback": self.lookback,
            "granularity": granularity,
            "feature_cols": feature_cols,
            "feature_min": scaler.data_min_.tolist(),
            "feature_max": scaler.data_max_.tolist(),
        }
        self.meta_path.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")

    def _forecast(
        self,
        model: TrendLSTM,
        scaler: MinMaxScaler,
        series_df: pd.DataFrame,
        feature_cols: list[str],
        horizon: int,
        granularity: str,
    ) -> list[dict]:
        features = series_df[feature_cols].to_numpy(dtype=np.float32)
        scaled = scaler.transform(features)
        window = scaled[-self.lookback :, :].copy()
        forecast = []
        last_time = pd.to_datetime(series_df["time"].iloc[-1])
        last_rating = float(series_df["rating"].iloc[-1])

        idx_map = {name: i for i, name in enumerate(feature_cols)}
        for i in range(horizon):
            x = torch.tensor(window.reshape(1, self.lookback, len(feature_cols)), dtype=torch.float32).to(
                self.device
            )
            with torch.no_grad():
                pred_scaled = model(x).cpu().numpy()[0, 0]

            next_row = window[-1].copy()
            next_row[idx_map["rating"]] = pred_scaled
            window = np.vstack([window[1:], next_row])
            raw = scaler.inverse_transform(next_row.reshape(1, -1))[0]

            pred_rating = float(raw[idx_map["rating"]])
            if "factor_score" in idx_map:
                pred_rating += 0.15 * float(raw[idx_map["factor_score"]])
            # 连续性约束，避免突跳
            max_step = 0.35
            pred_rating = max(last_rating - max_step, min(last_rating + max_step, pred_rating))
            pred_rating = 0.7 * pred_rating + 0.3 * last_rating
            last_rating = pred_rating

            next_time = (
                last_time + pd.DateOffset(weeks=i + 1)
                if granularity == "week"
                else last_time + pd.DateOffset(months=i + 1)
            )
            item = {
                "time": next_time.strftime("%Y-%m-%d"),
                "rating": float(pred_rating),
                "sentiment": float(raw[idx_map["sentiment"]]) if "sentiment" in idx_map else 0.0,
                "review_count": float(raw[idx_map["review_count"]]) if "review_count" in idx_map else 0.0,
            }
            for c in ("rating_env", "rating_flavor", "rating_service"):
                if c in idx_map:
                    item[c] = float(raw[idx_map[c]])
            if "factor_score" in idx_map:
                item["factor_score"] = float(raw[idx_map["factor_score"]])
            forecast.append(item)
        return forecast

    def _naive_predict(self, series_df: pd.DataFrame, horizon: int) -> PredictResult:
        if series_df.empty:
            history = []
            last = {"rating": 0.0, "sentiment": 0.0, "review_count": 0.0}
            last_time = pd.Timestamp.today()
        else:
            history = series_df.to_dict(orient="records")
            last = history[-1]
            last_time = pd.to_datetime(last["time"])

        drift = 0.0
        if len(history) >= 4:
            drift = (float(history[-1]["rating"]) - float(history[-4]["rating"])) / 3.0
            drift = max(-0.25, min(0.25, drift))

        current = float(last["rating"])
        forecast = []
        for i in range(horizon):
            t = last_time + pd.DateOffset(months=i + 1)
            current = max(1.0, min(5.0, current + drift * 0.6))
            item = {
                "time": t.strftime("%Y-%m-%d"),
                "rating": float(current),
                "sentiment": float(last.get("sentiment", 0.0)),
                "review_count": float(last.get("review_count", 0.0)),
            }
            for c in ("rating_env", "rating_flavor", "rating_service"):
                if c in last:
                    item[c] = float(last[c])
            if "factor_score" in last:
                item["factor_score"] = float(last["factor_score"])
            forecast.append(item)
        return PredictResult(history=history, forecast=forecast, metrics={"mae": 0.0, "rmse": 0.0})

    @staticmethod
    def _factor_score(tags_text: str, sentiment: float) -> float:
        tags = [x.strip() for x in str(tags_text or "").split(",") if x.strip()]
        polarities = [FACTOR_MAP[t].polarity for t in tags if t in FACTOR_MAP]
        base = float(np.mean(polarities)) if polarities else 0.0
        return 0.7 * base + 0.3 * float(sentiment or 0.0)
