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
    compute_and_record,
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

        Runs the body every au worker runs, so it records results as they do:
        ``result.error`` is the exception itself, and a ``BaseException`` is
        recorded FAILED, then re-raised to the caller.

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

        # The body every au worker runs: the same recording, middleware
        # lifecycle and BaseException handling as the real backends.
        compute_and_record(
            func, args, kwargs, key=key, store=store, middleware=self.middleware
        )

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
def mock_async(
    backend: Optional[ComputationBackend] = None,
    *,
    store: Optional[ComputationStore] = None,
):
    """Run every task launched inside the block on a synchronous test backend.

    It becomes the default backend of :func:`au.async_compute` and of the
    simple API (``au.submit_task``, ...). The simple API's results go to
    ``store`` (a fresh InMemoryStore by default), which is restored on exit:
    read them inside the block, or pass a store you keep.

    Usage:
        with mock_async() as mock:
            @async_compute
            def my_func(n: int) -> int:
                return n * 2

            handle = my_func(5)
            assert mock.task_count == 1
            assert handle.get_result() == 10

    Args:
        backend: Optional custom backend (defaults to TrackingTestBackend)
        store: The simple API's store inside the block (default: in memory)

    Yields:
        MockTaskTracker instance
    """
    import au.api
    import au.base

    if backend is None:
        backend = TrackingTestBackend()
    originals = (
        au.base._default_backend,
        au.api._default_backend,
        au.api._default_store,
    )
    au.base._default_backend = au.api._default_backend = backend
    au.api._default_store = store if store is not None else InMemoryStore()
    try:
        if isinstance(backend, TrackingTestBackend):
            yield backend.tracker
        else:
            yield MockTaskTracker()
    finally:
        (
            au.base._default_backend,
            au.api._default_backend,
            au.api._default_store,
        ) = originals


def create_test_backend(**kwargs) -> SyncTestBackend:
    """Create a test backend with optional configuration.

    Args:
        **kwargs: Configuration options (currently accepts 'middleware')

    Returns:
        SyncTestBackend instance
    """
    return SyncTestBackend(middleware=kwargs.get("middleware"))


def create_test_store(**kwargs) -> InMemoryStore:
    """Create a test store with optional configuration.

    Args:
        **kwargs: Configuration options (accepts 'ttl_seconds')

    Returns:
        InMemoryStore instance
    """
    return InMemoryStore(ttl_seconds=kwargs.get("ttl_seconds", 3600))
