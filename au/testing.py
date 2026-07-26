"""
Testing utilities for AU.

Provides test backends, mocking utilities, and helpers for testing async code.
"""

import time
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from contextlib import contextmanager
from datetime import datetime
import uuid

from au.base import (
    ComputationBackend,
    ComputationStore,
    ComputationResult,
    ComputationStatus,
    Middleware,
)


class InMemoryStore(ComputationStore):
    """In-memory store for testing.

    Stores results in a dictionary without any persistence.
    Useful for testing without filesystem dependencies.
    """

    def __init__(self, ttl_seconds: int = 3600):
        """Initialize in-memory store.

        Args:
            ttl_seconds: Time-to-live for results (not enforced in memory)
        """
        super().__init__(ttl_seconds=ttl_seconds)
        self._data: dict[str, ComputationResult] = {}

    def create_key(self) -> str:
        """Create a unique key for a computation."""
        return str(uuid.uuid4())

    def __getitem__(self, key: str) -> ComputationResult:
        """Get result by key."""
        if key not in self._data:
            raise KeyError(f"No result found for key: {key}")
        return self._data[key]

    def __setitem__(self, key: str, value: ComputationResult) -> None:
        """Store result by key."""
        self._data[key] = value

    def __delitem__(self, key: str) -> None:
        """Delete result by key."""
        del self._data[key]

    def __iter__(self):
        """Iterate over keys."""
        return iter(self._data)

    def __len__(self) -> int:
        """Return number of stored results."""
        return len(self._data)

    def cleanup_expired(self) -> int:
        """No-op for in-memory store (no automatic expiration).

        Returns:
            0 (nothing cleaned up)
        """
        return 0

    def get_reconstruction_info(self) -> dict:
        """Get info needed to reconstruct this store.

        Returns:
            Dictionary with store type and parameters
        """
        return {
            "type": "in_memory",
            "ttl_seconds": self.ttl_seconds,
        }

    def clear(self) -> None:
        """Clear all stored results."""
        self._data.clear()


class SyncTestBackend(ComputationBackend):
    """Synchronous test backend that executes immediately.

    This backend runs computations synchronously in the current process/thread,
    making it ideal for testing without the complexity of actual async execution.
    """

    def __init__(self, middleware: Optional[list[Middleware]] = None):
        """Initialize synchronous test backend.

        Args:
            middleware: Optional list of middleware
        """
        super().__init__(middleware=middleware)
        self._executions: dict[str, tuple[Callable, tuple, dict]] = {}

    def launch(
        self,
        func: Callable,
        args: tuple,
        kwargs: dict,
        key: str,
        store: ComputationStore | None = None,
    ) -> None:
        """Execute function synchronously and store result immediately.

        Args:
            func: Function to execute
            args: Positional arguments
            kwargs: Keyword arguments
            key: Result key
            store: Store for results (falls back to the backend's own store)
        """
        store = self._resolve_store(store)

        # Track execution
        self._executions[key] = (func, args, kwargs)

        # Execute with middleware (base.Middleware protocol:
        # before_compute(func, args, kwargs, key) / after_compute(key, result) /
        # on_error(key, error) — driven via the _run_middleware_* helpers).
        try:
            # Before middleware
            self._run_middleware_before(func, args, kwargs, key)

            # Execute function
            value = func(*args, **kwargs)

            # Store successful result, then run after middleware with it
            result = ComputationResult(
                value=value,
                status=ComputationStatus.COMPLETED,
                error=None,
                completed_at=datetime.now(),
            )
            store[key] = result
            self._run_middleware_after(key, result)

        except Exception as e:
            # Store failed result, then run error middleware
            store[key] = ComputationResult(
                value=None,
                status=ComputationStatus.FAILED,
                error=str(e),
                completed_at=datetime.now(),
            )
            self._run_middleware_error(key, e)

    def terminate(self, key: str) -> None:
        """No-op for synchronous backend (already completed).

        Args:
            key: Computation key
        """
        pass

    def get_execution(self, key: str) -> Optional[tuple[Callable, tuple, dict]]:
        """Get recorded execution for a key.

        Args:
            key: Computation key

        Returns:
            Tuple of (function, args, kwargs) or None
        """
        return self._executions.get(key)


@dataclass
class TaskCallRecord:
    """Record of a task call for testing/mocking."""

    func_name: str
    args: tuple
    kwargs: dict
    timestamp: float = field(default_factory=time.time)


@dataclass
class MockTaskTracker:
    """Tracks task executions for testing.

    Attributes:
        task_count: Total number of tasks executed
        tasks_by_name: Dictionary mapping function names to call records
        all_tasks: List of all task call records
    """

    task_count: int = 0
    tasks_by_name: dict[str, list[TaskCallRecord]] = field(default_factory=dict)
    all_tasks: list[TaskCallRecord] = field(default_factory=list)

    def record_call(self, func_name: str, args: tuple, kwargs: dict):
        """Record a task call.

        Args:
            func_name: Name of the function
            args: Positional arguments
            kwargs: Keyword arguments
        """
        record = TaskCallRecord(func_name, args, kwargs)
        self.task_count += 1
        self.all_tasks.append(record)

        if func_name not in self.tasks_by_name:
            self.tasks_by_name[func_name] = []
        self.tasks_by_name[func_name].append(record)

    def get_calls(self, func_name: str) -> list[TaskCallRecord]:
        """Get all calls for a specific function.

        Args:
            func_name: Name of the function

        Returns:
            List of call records
        """
        return self.tasks_by_name.get(func_name, [])

    def call_count(self, func_name: str) -> int:
        """Get call count for a specific function.

        Args:
            func_name: Name of the function

        Returns:
            Number of calls
        """
        return len(self.get_calls(func_name))

    def last_call(self, func_name: str) -> Optional[TaskCallRecord]:
        """Get last call for a specific function.

        Args:
            func_name: Name of the function

        Returns:
            Last call record or None
        """
        calls = self.get_calls(func_name)
        return calls[-1] if calls else None


class TrackingTestBackend(SyncTestBackend):
    """Test backend that tracks all executions.

    Extends SyncTestBackend with detailed tracking for testing/debugging.
    """

    def __init__(self, middleware: Optional[list[Middleware]] = None):
        """Initialize tracking test backend.

        Args:
            middleware: Optional list of middleware
        """
        super().__init__(middleware=middleware)
        self.tracker = MockTaskTracker()

    def launch(
        self,
        func: Callable,
        args: tuple,
        kwargs: dict,
        key: str,
        store: ComputationStore | None = None,
    ) -> None:
        """Execute and track function call.

        Args:
            func: Function to execute
            args: Positional arguments
            kwargs: Keyword arguments
            key: Result key
            store: Store for results (falls back to the backend's own store)
        """
        # Record the call
        self.tracker.record_call(func.__name__, args, kwargs)

        # Execute normally
        super().launch(func, args, kwargs, key, store)


@contextmanager
def mock_async(backend: Optional[ComputationBackend] = None):
    """Context manager for mocking async execution.

    Usage:
        with mock_async() as mock:
            @async_compute
            def my_func(n: int) -> int:
                return n * 2

            handle = my_func.async_run(n=5)
            assert mock.task_count == 1
            assert handle.get_result() == 10

    Args:
        backend: Optional custom backend (defaults to TrackingTestBackend)

    Yields:
        MockTaskTracker instance
    """
    if backend is None:
        backend = TrackingTestBackend()

    # Import here to avoid circular dependency
    from au.base import async_compute

    # Store original defaults
    original_backend = getattr(async_compute, '_default_backend', None)

    # Set test backend as default
    async_compute._default_backend = backend

    try:
        # Yield tracker if backend has one
        if isinstance(backend, TrackingTestBackend):
            yield backend.tracker
        else:
            yield MockTaskTracker()

    finally:
        # Restore original backend
        if original_backend is not None:
            async_compute._default_backend = original_backend
        elif hasattr(async_compute, '_default_backend'):
            delattr(async_compute, '_default_backend')


def create_test_backend(**kwargs) -> SyncTestBackend:
    """Create a test backend with optional configuration.

    Args:
        **kwargs: Configuration options (currently accepts 'middleware')

    Returns:
        SyncTestBackend instance
    """
    return SyncTestBackend(middleware=kwargs.get('middleware'))


def create_test_store(**kwargs) -> InMemoryStore:
    """Create a test store with optional configuration.

    Args:
        **kwargs: Configuration options (accepts 'ttl_seconds')

    Returns:
        InMemoryStore instance
    """
    return InMemoryStore(ttl_seconds=kwargs.get('ttl_seconds', 3600))
