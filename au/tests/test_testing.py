"""Tests for testing utilities module."""

import pytest

from au.testing import (
    InMemoryStore,
    SyncTestBackend,
    TrackingTestBackend,
    MockTaskTracker,
    create_test_backend,
    create_test_store,
)
from au.base import ComputationResult, ComputationStatus


def simple_func(x: int) -> int:
    """Simple test function."""
    return x * 2


def failing_func():
    """Function that fails."""
    raise ValueError("Test error")


class TestInMemoryStore:
    """Tests for InMemoryStore."""

    def test_create_and_store(self):
        """Test creating and storing results."""
        store = InMemoryStore()

        key = store.create_key()
        assert isinstance(key, str)

        result = ComputationResult(
            value=42,
            status=ComputationStatus.COMPLETED,
        )

        store[key] = result
        assert store[key] == result

    def test_iteration(self):
        """Test iterating over keys."""
        store = InMemoryStore()

        keys = [store.create_key() for _ in range(3)]

        for key in keys:
            store[key] = ComputationResult(
                value=1,
                status=ComputationStatus.COMPLETED,
            )

        assert len(store) == 3
        assert set(store) == set(keys)

    def test_deletion(self):
        """Test deleting results."""
        store = InMemoryStore()

        key = store.create_key()
        store[key] = ComputationResult(
            value=1,
            status=ComputationStatus.COMPLETED,
        )

        assert key in store
        del store[key]
        assert key not in store

    def test_clear(self):
        """Test clearing store."""
        store = InMemoryStore()

        for _ in range(5):
            key = store.create_key()
            store[key] = ComputationResult(
                value=1,
                status=ComputationStatus.COMPLETED,
            )

        assert len(store) == 5
        store.clear()
        assert len(store) == 0


class TestSyncTestBackend:
    """Tests for SyncTestBackend."""

    def test_synchronous_execution(self):
        """Test synchronous task execution."""
        backend = SyncTestBackend()
        store = InMemoryStore()

        key = store.create_key()
        backend.launch(simple_func, (5,), {}, key, store)

        # Should complete immediately
        result = store[key]
        assert result.status == ComputationStatus.COMPLETED
        assert result.value == 10

    def test_failed_execution(self):
        """Test handling failed execution."""
        backend = SyncTestBackend()
        store = InMemoryStore()

        key = store.create_key()
        backend.launch(failing_func, (), {}, key, store)

        # Should store failure
        result = store[key]
        assert result.status == ComputationStatus.FAILED
        assert "Test error" in result.error

    def test_get_execution(self):
        """Test getting execution record."""
        backend = SyncTestBackend()
        store = InMemoryStore()

        key = store.create_key()
        backend.launch(simple_func, (7,), {}, key, store)

        # Should track execution
        execution = backend.get_execution(key)
        assert execution is not None
        assert execution[0] == simple_func
        assert execution[1] == (7,)
        assert execution[2] == {}


class TestTrackingTestBackend:
    """Tests for TrackingTestBackend."""

    def test_tracks_calls(self):
        """Test that calls are tracked."""
        backend = TrackingTestBackend()
        store = InMemoryStore()

        key1 = store.create_key()
        backend.launch(simple_func, (5,), {}, key1, store)

        assert backend.tracker.task_count == 1
        assert backend.tracker.call_count('simple_func') == 1

    def test_tracks_multiple_calls(self):
        """Test tracking multiple calls."""
        backend = TrackingTestBackend()
        store = InMemoryStore()

        for i in range(3):
            key = store.create_key()
            backend.launch(simple_func, (i,), {}, key, store)

        assert backend.tracker.task_count == 3
        assert backend.tracker.call_count('simple_func') == 3

    def test_last_call(self):
        """Test getting last call."""
        backend = TrackingTestBackend()
        store = InMemoryStore()

        for i in range(3):
            key = store.create_key()
            backend.launch(simple_func, (i,), {}, key, store)

        last = backend.tracker.last_call('simple_func')
        assert last is not None
        assert last.args == (2,)


class TestMockTaskTracker:
    """Tests for MockTaskTracker."""

    def test_record_call(self):
        """Test recording calls."""
        tracker = MockTaskTracker()

        tracker.record_call('func1', (1, 2), {'key': 'value'})

        assert tracker.task_count == 1
        assert tracker.call_count('func1') == 1

    def test_get_calls(self):
        """Test getting all calls for a function."""
        tracker = MockTaskTracker()

        tracker.record_call('func1', (1,), {})
        tracker.record_call('func1', (2,), {})
        tracker.record_call('func2', (3,), {})

        calls = tracker.get_calls('func1')
        assert len(calls) == 2
        assert calls[0].args == (1,)
        assert calls[1].args == (2,)


def test_create_test_backend():
    """Test creating test backend."""
    backend = create_test_backend()
    assert isinstance(backend, SyncTestBackend)


def test_create_test_store():
    """Test creating test store."""
    store = create_test_store()
    assert isinstance(store, InMemoryStore)

    store = create_test_store(ttl_seconds=7200)
    assert store.ttl_seconds == 7200
