from api.app.main import app
from fastapi.testclient import TestClient

def test_health():
    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data.get("status") == "OK"