"""Tests for simplified API module."""

import pytest
import time
import tempfile
from pathlib import Path

from au.api import (
    submit_task,
    get_result,
    get_status,
    is_ready,
    cancel_task,
    async_task,
    get_handle,
    submit_many,
    get_many,
    set_default_backend,
    set_default_store,
)
from au.base import ComputationStatus
from au.testing import SyncTestBackend, InMemoryStore


def simple_function(x: int, y: int = 2) -> int:
    """Simple test function."""
    return x * y


def slow_function(duration: float) -> str:
    """Function that takes some time."""
    time.sleep(duration)
    return "done"


def failing_function():
    """Function that always fails."""
    raise ValueError("This function always fails")


def test_submit_and_get_result():
    """Test basic submit_task and get_result."""
    # Use test backend for synchronous execution
    backend = SyncTestBackend()
    store = InMemoryStore()

    set_default_backend(backend)
    set_default_store(store)

    # Submit task
    task_id = submit_task(simple_function, 5, y=3)
    assert isinstance(task_id, str)

    # Get result
    result = get_result(task_id)
    assert result == 15


def test_get_status():
    """Test getting task status."""
    backend = SyncTestBackend()
    store = InMemoryStore()

    set_default_backend(backend)
    set_default_store(store)

    # Submit and check status
    task_id = submit_task(simple_function, 10)

    status = get_status(task_id)
    assert status == ComputationStatus.COMPLETED


def test_is_ready():
    """Test checking if task is ready."""
    backend = SyncTestBackend()
    store = InMemoryStore()

    set_default_backend(backend)
    set_default_store(store)

    task_id = submit_task(simple_function, 7)

    assert is_ready(task_id) is True


def test_async_task_context_manager():
    """Test async_task context manager."""
    backend = SyncTestBackend()
    store = InMemoryStore()

    with async_task(simple_function, 4, y=5, backend=backend, store=store) as handle:
        assert handle is not None

    # Result should be available after context
    result = handle.get_result()
    assert result == 20


def test_get_handle():
    """Test getting a handle for a task."""
    backend = SyncTestBackend()
    store = InMemoryStore()

    set_default_backend(backend)
    set_default_store(store)

    task_id = submit_task(simple_function, 3)

    handle = get_handle(task_id)
    assert handle is not None
    assert handle.key == task_id
    assert handle.is_ready()


def test_submit_many():
    """Test submitting multiple tasks."""
    backend = SyncTestBackend()
    store = InMemoryStore()

    set_default_backend(backend)
    set_default_store(store)

    tasks = [
        (simple_function, (2,), {'y': 3}),
        (simple_function, (4,), {'y': 5}),
        (simple_function, (6,), {'y': 7}),
    ]

    task_ids = submit_many(tasks)

    assert len(task_ids) == 3
    assert all(isinstance(tid, str) for tid in task_ids)


def test_get_many():
    """Test getting multiple results."""
    backend = SyncTestBackend()
    store = InMemoryStore()

    set_default_backend(backend)
    set_default_store(store)

    tasks = [
        (simple_function, (2,), {'y': 3}),
        (simple_function, (4,), {'y': 5}),
        (simple_function, (6,), {'y': 7}),
    ]

    task_ids = submit_many(tasks)
    results = get_many(task_ids)

    assert results == [6, 20, 42]


def test_failed_task():
    """Test handling failed tasks."""
    backend = SyncTestBackend()
    store = InMemoryStore()

    set_default_backend(backend)
    set_default_store(store)

    task_id = submit_task(failing_function)

    status = get_status(task_id)
    assert status == ComputationStatus.FAILED

    with pytest.raises(Exception):
        get_result(task_id)
