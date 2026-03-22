import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import MinMaxScaler


class TrendLSTM(nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int = 32):
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

    def predict(
        self, df: pd.DataFrame, granularity: str = "month", horizon: int = 6
    ) -> PredictResult:
        series_df = self._build_series(df, granularity)
        if len(series_df) < max(12, self.lookback + 2):
            return self._naive_predict(series_df, horizon)

        X, y, scaler = self._build_dataset(series_df)
        split = int(len(X) * 0.8)
        X_train = X[:split]
        y_train = y[:split]
        X_val = X[split:]
        y_val = y[split:]

        model = TrendLSTM(input_dim=X.shape[2]).to(self.device)
        self._train_model(model, X_train, y_train)
        val_pred = self._predict_tensor(model, X_val)
        metrics = {
            "mae": float(mean_absolute_error(y_val, val_pred)),
            "rmse": float(np.sqrt(mean_squared_error(y_val, val_pred))),
        }
        self._save_model(model, scaler, granularity)

        forecast = self._forecast(model, scaler, series_df, horizon, granularity)
        history = series_df.to_dict(orient="records")
        return PredictResult(history=history, forecast=forecast, metrics=metrics)

    def _build_series(self, df: pd.DataFrame, granularity: str) -> pd.DataFrame:
        work = df.copy()
        work["review_time"] = pd.to_datetime(work["review_time"])
        freq = "W" if granularity == "week" else "MS"
        grouped = (
            work.set_index("review_time")
            .groupby(pd.Grouper(freq=freq))
            .agg({"rating": "mean", "sentiment": "mean", "id": "count"})
            .rename(columns={"id": "review_count"})
            .reset_index()
            .fillna(0.0)
        )
        grouped["time"] = grouped["review_time"].dt.strftime("%Y-%m-%d")
        return grouped[["time", "rating", "sentiment", "review_count"]]

    def _build_dataset(self, series_df: pd.DataFrame):
        features = series_df[["rating", "sentiment", "review_count"]].to_numpy(dtype=np.float32)
        scaler = MinMaxScaler()
        scaled = scaler.fit_transform(features)
        X, y = [], []
        for i in range(len(scaled) - self.lookback):
            X.append(scaled[i : i + self.lookback, :])
            y.append(scaled[i + self.lookback, 0])
        X_arr = np.array(X, dtype=np.float32)
        y_arr = np.array(y, dtype=np.float32).reshape(-1, 1)
        return X_arr, y_arr, scaler

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

    def _save_model(self, model: TrendLSTM, scaler: MinMaxScaler, granularity: str) -> None:
        torch.save(model.state_dict(), self.model_path)
        meta = {
            "lookback": self.lookback,
            "granularity": granularity,
            "feature_min": scaler.data_min_.tolist(),
            "feature_max": scaler.data_max_.tolist(),
        }
        self.meta_path.write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")

    def _forecast(
        self,
        model: TrendLSTM,
        scaler: MinMaxScaler,
        series_df: pd.DataFrame,
        horizon: int,
        granularity: str,
    ) -> list[dict]:
        feature_cols = ["rating", "sentiment", "review_count"]
        features = series_df[feature_cols].to_numpy(dtype=np.float32)
        scaled = scaler.transform(features)
        window = scaled[-self.lookback :, :].copy()
        forecast = []
        last_time = pd.to_datetime(series_df["time"].iloc[-1])

        for i in range(horizon):
            x = torch.tensor(window.reshape(1, self.lookback, 3), dtype=torch.float32).to(
                self.device
            )
            with torch.no_grad():
                pred_scaled = model(x).cpu().numpy()[0, 0]
            next_row = window[-1].copy()
            next_row[0] = pred_scaled
            window = np.vstack([window[1:], next_row])
            raw = scaler.inverse_transform(next_row.reshape(1, -1))[0]
            next_time = (
                last_time + pd.DateOffset(weeks=i + 1)
                if granularity == "week"
                else last_time + pd.DateOffset(months=i + 1)
            )
            forecast.append(
                {
                    "time": next_time.strftime("%Y-%m-%d"),
                    "rating": float(raw[0]),
                    "sentiment": float(raw[1]),
                    "review_count": float(raw[2]),
                }
            )
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
        forecast = []
        for i in range(horizon):
            t = last_time + pd.DateOffset(months=i + 1)
            forecast.append(
                {
                    "time": t.strftime("%Y-%m-%d"),
                    "rating": float(last["rating"]),
                    "sentiment": float(last["sentiment"]),
                    "review_count": float(last["review_count"]),
                }
            )
        return PredictResult(history=history, forecast=forecast, metrics={"mae": 0.0, "rmse": 0.0})
