from app import create_app


def test_workflow_endpoint_shape():
    app = create_app()
    client = app.test_client()
    res = client.get("/api/workflow")
    assert res.status_code == 200
    payload = res.get_json()
    assert "workflow" in payload
    assert "metrics" in payload
    assert "explanation" in payload


def test_import_missing_reviews_field():
    app = create_app()
    client = app.test_client()
    res = client.post("/api/import", json={})
    assert res.status_code == 400


def test_new_insight_endpoints_exist():
    app = create_app()
    client = app.test_client()

    factors = client.get("/api/insights/factors")
    evidence = client.get("/api/insights/evidence")

    assert factors.status_code == 200
    assert evidence.status_code == 200
