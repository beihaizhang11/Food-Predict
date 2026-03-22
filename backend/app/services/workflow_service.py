import json
from pathlib import Path


def get_workflow() -> dict:
    root = Path(__file__).resolve().parents[3]
    workflow_path = root / "data" / "workflow.json"
    if workflow_path.exists():
        return json.loads(workflow_path.read_text(encoding="utf-8"))
    return {"nodes": [], "edges": []}
