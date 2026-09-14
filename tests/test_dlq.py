import pytest
import app as app_module


@pytest.fixture()
def client():
    app_module.queue = app_module.DeadLetterQueue(3)
    app_module.app.config["TESTING"] = True
    with app_module.app.test_client() as client:
        yield client


def make_job(client):
    return client.post("/api/jobs", json={"task": "process-payment"}).get_json()["id"]


def test_retry_then_complete(client):
    job_id = make_job(client)
    for attempt in (1, 2):
        claimed = client.post("/api/jobs/claim").get_json()
        assert claimed["attempts"] == attempt
        failed = client.post(f"/api/jobs/{job_id}/fail",
                             json={"error": "temporary"})
        assert failed.get_json()["status"] == "queued"

    assert client.post("/api/jobs/claim").get_json()["attempts"] == 3
    done = client.post(f"/api/jobs/{job_id}/complete",
                       json={"result": "success"})
    assert done.get_json()["status"] == "completed"


def test_moves_to_dead_letter_after_three_failures(client):
    job_id = make_job(client)
    for _ in range(3):
        client.post("/api/jobs/claim")
        response = client.post(f"/api/jobs/{job_id}/fail",
                               json={"error": "permanent failure"})
    assert response.get_json()["status"] == "dead"
    items = client.get("/api/dead-letters").get_json()["items"]
    assert len(items) == 1
    assert items[0]["id"] == job_id


def test_dead_letter_can_be_requeued(client):
    job_id = make_job(client)
    for _ in range(3):
        client.post("/api/jobs/claim")
        client.post(f"/api/jobs/{job_id}/fail", json={"error": "failure"})

    response = client.post(f"/api/dead-letters/{job_id}/requeue")
    assert response.status_code == 202
    assert response.get_json()["status"] == "queued"

    claimed = client.post("/api/jobs/claim").get_json()
    assert claimed["status"] == "processing"
    assert claimed["attempts"] == 4


def test_empty_claim(client):
    assert client.post("/api/jobs/claim").status_code == 204


def test_invalid_job(client):
    assert client.get("/api/jobs/not-found").status_code == 404


def test_stats(client):
    job_id = make_job(client)
    for _ in range(3):
        client.post("/api/jobs/claim")
        client.post(f"/api/jobs/{job_id}/fail", json={"error": "x"})
    stats = client.get("/api/jobs/stats").get_json()
    assert stats["dead"] == 1
    assert stats["dead_letters"] == 1
