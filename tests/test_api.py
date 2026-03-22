from app import create_app


def test_workflow_endpoint():
    app = create_app()
    client = app.test_client()
    res = client.get("/api/workflow")
    assert res.status_code == 200
    payload = res.get_json()
    assert "nodes" in payload
    assert "edges" in payload


def test_import_missing_reviews_field():
    app = create_app()
    client = app.test_client()
    res = client.post("/api/import", json={})
    assert res.status_code == 400
