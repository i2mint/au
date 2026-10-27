"""The HTTP endpoints report a task's timing (i2mint/au#24)."""

import time
from datetime import datetime

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient  # noqa: E402

from au.base import ComputationResult, ComputationStatus, FileSystemStore  # noqa: E402
from au.http import mk_http_interface  # noqa: E402
from au.testing import InMemoryStore, SyncTestBackend  # noqa: E402


def naps(seconds):
    time.sleep(seconds)
    return seconds


def _client(store):
    return TestClient(mk_http_interface([naps], backend=SyncTestBackend(), store=store))


@pytest.mark.parametrize("path", ["/tasks/{}/status", "/tasks/{}/result"])
def test_status_and_result_report_when_and_how_long(path):
    client = _client(InMemoryStore())
    task_id = client.post(
        "/tasks", json={"function_name": "naps", "args": [0.1]}
    ).json()["task_id"]
    body = client.get(path.format(task_id)).json()
    assert body["started_at"] < body["completed_at"]
    assert body["duration"] >= 0.1


def test_a_zero_duration_is_reported_as_zero():
    """A timedelta(0) used to reach the response model: HTTP 500."""
    store, now = InMemoryStore(), datetime.now()
    store["k"] = ComputationResult(
        1, ComputationStatus.COMPLETED, started_at=now, completed_at=now
    )
    response = _client(store).get("/tasks/k/status")
    assert response.status_code == 200 and response.json()["duration"] == 0.0


def test_a_running_task_has_a_start_but_no_end():
    store = InMemoryStore()
    store["k"] = ComputationResult(
        None, ComputationStatus.RUNNING, started_at=datetime.now()
    )
    body = _client(store).get("/tasks/k/status").json()
    assert body["started_at"] and body["completed_at"] is None
    assert body["duration"] is None


def test_an_unknown_id_gets_no_made_up_timestamps(tmp_path):
    """FileSystemStore reports any unknown id as PENDING, stamped at the read."""
    body = _client(FileSystemStore(tmp_path)).get("/tasks/nope/status").json()
    assert body["status"] == "pending" and body["created_at"] is None
