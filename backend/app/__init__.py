from pathlib import Path

from flask import Flask, send_from_directory
from flask_cors import CORS

from .api.routes import api_bp
from .config import Config
from .extensions import db


def create_app() -> Flask:
    root = Path(__file__).resolve().parents[2]
    frontend_dir = root / "frontend"
    app = Flask(__name__)
    app.config.from_object(Config())
    CORS(app, resources={r"/api/*": {"origins": "*"}})

    db.init_app(app)
    app.register_blueprint(api_bp, url_prefix="/api")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.after_request
    def add_cors_headers(resp):
        if resp.headers.get("Access-Control-Allow-Origin") is None:
            resp.headers["Access-Control-Allow-Origin"] = "*"
            resp.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
            resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
        return resp

    @app.get("/")
    def index():
        return send_from_directory(frontend_dir, "index.html")

    @app.get("/<path:filename>")
    def frontend_assets(filename: str):
        return send_from_directory(frontend_dir, filename)

    return app
