import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    SECRET_KEY: str = os.getenv("SECRET_KEY", "change-me")
    MYSQL_HOST: str = os.getenv("MYSQL_HOST", "127.0.0.1")
    MYSQL_PORT: int = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_USER: str = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD: str = os.getenv("MYSQL_PASSWORD", "")
    MYSQL_DATABASE: str = os.getenv("MYSQL_DATABASE", "restaurant_analytics")
    SQLALCHEMY_TRACK_MODIFICATIONS: bool = False
    SQLALCHEMY_DATABASE_URI: str = (
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}?charset=utf8mb4"
    )

    NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687")
    NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "neo4j")

    SBERT_MODEL: str = os.getenv(
        "SBERT_MODEL", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    )
    SENTIMENT_MODEL: str = os.getenv(
        "SENTIMENT_MODEL", "cardiffnlp/twitter-xlm-roberta-base-sentiment"
    )
    NER_MODEL: str = os.getenv("NER_MODEL", "dslim/bert-base-NER")

    MODEL_DIR: str = str(Path(os.getenv("MODEL_DIR", "models")).resolve())
    LSTM_LOOKBACK: int = int(os.getenv("LSTM_LOOKBACK", "8"))
    LSTM_EPOCHS: int = int(os.getenv("LSTM_EPOCHS", "30"))
    LSTM_BATCH_SIZE: int = int(os.getenv("LSTM_BATCH_SIZE", "16"))
