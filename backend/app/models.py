from datetime import datetime

from sqlalchemy import Index

from .extensions import db


class Review(db.Model):
    __tablename__ = "reviews"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.String(32), nullable=False)
    shop_id = db.Column(db.String(32), nullable=False)
    dish = db.Column(db.String(64), nullable=False)
    rating = db.Column(db.Float, nullable=False)
    rating_env = db.Column(db.Float, nullable=True)
    rating_flavor = db.Column(db.Float, nullable=True)
    rating_service = db.Column(db.Float, nullable=True)
    review_text = db.Column(db.Text, nullable=False)
    review_time = db.Column(db.DateTime, nullable=False)
    tags = db.Column(db.String(255), nullable=True)
    sentiment = db.Column(db.Float, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(
        db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    __table_args__ = (
        Index("idx_review_time", "review_time"),
        Index("idx_shop_id", "shop_id"),
        Index("idx_dish", "dish"),
    )


class AnalysisCache(db.Model):
    __tablename__ = "analysis_cache"

    review_id = db.Column(
        db.Integer, db.ForeignKey("reviews.id", ondelete="CASCADE"), primary_key=True
    )
    embedding_json = db.Column(db.Text, nullable=True)
    entities_json = db.Column(db.Text, nullable=True)
    keywords_json = db.Column(db.Text, nullable=True)
    updated_at = db.Column(
        db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )
