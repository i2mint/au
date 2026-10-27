"""The HTTP endpoints report a task's timing (i2mint/au#24)."""

import time

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient  # noqa: E402

from au.http import mk_http_interface  # noqa: E402
from au.testing import InMemoryStore, SyncTestBackend  # noqa: E402


def naps(seconds):
    time.sleep(seconds)
    return seconds


@pytest.mark.parametrize("path", ["/tasks/{}/status", "/tasks/{}/result"])
def test_status_and_result_report_when_and_how_long(path):
    app = mk_http_interface([naps], backend=SyncTestBackend(), store=InMemoryStore())
    client = TestClient(app)
    task_id = client.post(
        "/tasks", json={"function_name": "naps", "args": [0.1]}
    ).json()["task_id"]
    body = client.get(path.format(task_id)).json()
    assert body["created_at"] and body["completed_at"]
    assert body["duration"] >= 0.1
