"""
Simplified API for AU - direct task submission without decorators.

Provides simple functions for submitting and retrieving async tasks
without requiring decorator patterns.
"""

from typing import Any, Callable, Optional, TypeVar
from contextlib import contextmanager

from au.base import (
    ComputationBackend,
    ComputationStore,
    ComputationHandle,
    ComputationResult,
    ComputationStatus,
    FileSystemStore,
    ProcessBackend,
    SerializationFormat,
    Middleware,
)
from au.config import get_global_config
from au.retry import RetryPolicy, retry_with_policy

T = TypeVar('T')


# Global default backend and store
_default_backend: Optional[ComputationBackend] = None
_default_store: Optional[ComputationStore] = None


def _get_default_backend() -> ComputationBackend:
    """Get or create the default backend based on configuration."""
    global _default_backend

    if _default_backend is None:
        config = get_global_config()

        # Create backend based on config. Backends take the result store
        # per-launch (submit_task passes it), so no store is bound here.
        if config.backend == "redis":
            from au.backends.rq_backend import RQBackend
            import redis
            from rq import Queue

            redis_url = config.redis_url or "redis://localhost:6379"
            connection = redis.Redis.from_url(redis_url)
            _default_backend = RQBackend(rq_queue=Queue(connection=connection))

        elif config.backend == "supabase":
            from au.backends.supabase_backend import SupabaseQueueBackend
            if not config.supabase_url or not config.supabase_key:
                raise ValueError("Supabase backend requires AU_SUPABASE_URL and AU_SUPABASE_KEY")
            from supabase import create_client

            client = create_client(config.supabase_url, config.supabase_key)
            _default_backend = SupabaseQueueBackend(supabase_client=client)

        elif config.backend == "process":
            from au.base import ProcessBackend
            _default_backend = ProcessBackend()

        elif config.backend == "stdlib":
            from au.base import StdLibQueueBackend
            _default_backend = StdLibQueueBackend(
                max_workers=config.max_workers,
                use_processes=False,
            )

        else:  # Default to thread
            from au.base import ThreadBackend
            _default_backend = ThreadBackend()

    return _default_backend


def _get_default_store() -> ComputationStore:
    """Get or create the default store based on configuration."""
    global _default_store

    if _default_store is None:
        config = get_global_config()

        if config.storage == "memory":
            from au.testing import InMemoryStore
            _default_store = InMemoryStore(ttl_seconds=config.ttl_seconds)
        else:  # filesystem
            serialization = SerializationFormat.JSON
            if config.serialization == "pickle":
                serialization = SerializationFormat.PICKLE

            _default_store = FileSystemStore(
                base_path=config.storage_path,
                ttl_seconds=config.ttl_seconds,
                serialization=serialization,
            )

    return _default_store


def set_default_backend(backend: ComputationBackend) -> None:
    """Set the default backend for simple API calls.

    Args:
        backend: Backend to use by default
    """
    global _default_backend
    _default_backend = backend


def set_default_store(store: ComputationStore) -> None:
    """Set the default store for simple API calls.

    Args:
        store: Store to use by default
    """
    global _default_store
    _default_store = store


def _inflight_record(
    store: ComputationStore, key: str
) -> Optional[ComputationResult]:
    """Return an existing in-flight (RUNNING, non-expired) record for ``key``, else None.

    Used for idempotent submission: a matching in-flight record means the work is
    already running, so a resubmit with the same key should not launch a
    duplicate. Missing, terminal (COMPLETED / FAILED / CANCELLED), or expired
    records return None so the work is (re-)launched.

    Note: filesystem stores synthesize a ``PENDING`` result for absent keys, so
    only ``RUNNING`` is treated as a live claim (submit_task reserves the key
    with a RUNNING record before launching, closing the reserve→launch race).
    """
    try:
        existing = store[key]
    except KeyError:
        return None
    if existing.status != ComputationStatus.RUNNING:
        return None
    try:
        if store.is_expired(existing):
            return None
    except Exception:
        pass
    return existing


def submit_task(
    func: Callable,
    *args,
    key: Optional[str] = None,
    backend: Optional[ComputationBackend] = None,
    store: Optional[ComputationStore] = None,
    retry_policy: Optional[RetryPolicy] = None,
    **kwargs
) -> str:
    """Submit a task for async execution without decorator.

    Args:
        func: Function to execute
        *args: Positional arguments for function
        key: Optional caller-supplied idempotency key. When omitted, a fresh key
            is minted per call. When supplied and a live (in-flight) computation
            already exists for that key, the existing task is returned instead of
            launching a duplicate — so a resubmit runs the work at most once while
            it is in flight. A terminal (completed/failed/cancelled) or expired
            key re-runs.
        backend: Optional backend (uses default if not provided)
        store: Optional store (uses default if not provided)
        retry_policy: Optional retry policy
        **kwargs: Keyword arguments for function

    Returns:
        Task ID (key) for retrieving results

    Example:
        >>> task_id = submit_task(my_func, 5, multiplier=2)
        >>> result = get_result(task_id, timeout=10)

        >>> # Idempotent submission: resubmitting the same key while in flight
        >>> # returns the running task rather than launching a duplicate.
        >>> task_id = submit_task(my_func, 5, key="my-job")  # doctest: +SKIP
        >>> same_id = submit_task(my_func, 5, key="my-job")  # doctest: +SKIP
    """
    backend = backend if backend is not None else _get_default_backend()
    store = store if store is not None else _get_default_store()

    if key is None:
        # No idempotency requested: mint a fresh key per call.
        key = store.create_key()
    else:
        # Idempotency: dedup against a live in-flight computation, otherwise
        # reserve the key so a concurrent/sequential resubmit dedups too.
        if _inflight_record(store, key) is not None:
            return key
        store[key] = ComputationResult(None, ComputationStatus.RUNNING)

    # If retry policy provided, wrap function
    if retry_policy:
        original_func = func

        def wrapped_func(*args, **kwargs):
            return retry_with_policy(original_func, args, kwargs, retry_policy)

        func = wrapped_func

    # Launch task
    backend.launch(func, args, kwargs, key, store)

    return key


def get_result(
    task_id: str,
    timeout: Optional[float] = None,
    store: Optional[ComputationStore] = None,
) -> Any:
    """Get result for a task ID.

    Args:
        task_id: Task ID returned from submit_task
        timeout: Optional timeout in seconds
        store: Optional store (uses default if not provided)

    Returns:
        Task result

    Raises:
        TimeoutError: If timeout exceeded
        Exception: If task failed

    Example:
        >>> task_id = submit_task(my_func, 5)
        >>> result = get_result(task_id, timeout=10)
    """
    store = store if store is not None else _get_default_store()
    handle = ComputationHandle(task_id, store)
    return handle.get_result(timeout=timeout)


def get_status(
    task_id: str,
    store: Optional[ComputationStore] = None,
) -> ComputationStatus:
    """Get status for a task ID.

    Args:
        task_id: Task ID returned from submit_task
        store: Optional store (uses default if not provided)

    Returns:
        Task status (PENDING, RUNNING, COMPLETED, FAILED)

    Example:
        >>> task_id = submit_task(my_func, 5)
        >>> status = get_status(task_id)
        >>> if status == ComputationStatus.COMPLETED:
        >>>     result = get_result(task_id)
    """
    store = store if store is not None else _get_default_store()
    handle = ComputationHandle(task_id, store)
    return handle.get_status()


def is_ready(
    task_id: str,
    store: Optional[ComputationStore] = None,
) -> bool:
    """Check if task is ready (completed or failed).

    Args:
        task_id: Task ID returned from submit_task
        store: Optional store (uses default if not provided)

    Returns:
        True if task is complete, False otherwise

    Example:
        >>> task_id = submit_task(my_func, 5)
        >>> while not is_ready(task_id):
        >>>     time.sleep(0.1)
        >>> result = get_result(task_id)
    """
    store = store if store is not None else _get_default_store()
    handle = ComputationHandle(task_id, store)
    return handle.is_ready()


def cancel_task(
    task_id: str,
    backend: Optional[ComputationBackend] = None,
    store: Optional[ComputationStore] = None,
) -> bool:
    """Cancel a running task.

    Args:
        task_id: Task ID to cancel
        backend: Optional backend (uses default if not provided)
        store: Optional store (uses default if not provided)

    Returns:
        True if cancellation was attempted, False otherwise

    Example:
        >>> task_id = submit_task(long_running_func)
        >>> cancel_task(task_id)
    """
    backend = backend if backend is not None else _get_default_backend()
    store = store if store is not None else _get_default_store()
    handle = ComputationHandle(task_id, store, backend)
    return handle.cancel()


@contextmanager
def async_task(
    func: Callable,
    *args,
    backend: Optional[ComputationBackend] = None,
    store: Optional[ComputationStore] = None,
    timeout: Optional[float] = None,
    **kwargs
):
    """Context manager for async task execution.

    The task is submitted on enter and result retrieved on exit.

    Args:
        func: Function to execute
        *args: Positional arguments
        backend: Optional backend
        store: Optional store
        timeout: Optional timeout for getting result
        **kwargs: Keyword arguments

    Yields:
        ComputationHandle for the task

    Example:
        >>> with async_task(my_func, 5, multiplier=2) as handle:
        >>>     # Do other work while task runs
        >>>     print("Working...")
        >>> # Result is ready here
        >>> print(f"Result: {handle.result}")
    """
    backend = backend if backend is not None else _get_default_backend()
    store = store if store is not None else _get_default_store()

    # Submit task
    task_id = submit_task(func, *args, backend=backend, store=store, **kwargs)
    handle = ComputationHandle(task_id, store, backend)

    try:
        yield handle
    finally:
        # Wait for result on exit (blocks until complete or timeout)
        try:
            result = handle.get_result(timeout=timeout)
            # Attach result to handle for convenience
            handle.result = result
        except Exception as e:
            # Attach error to handle
            handle.error = e


def get_handle(
    task_id: str,
    backend: Optional[ComputationBackend] = None,
    store: Optional[ComputationStore] = None,
) -> ComputationHandle:
    """Get a handle for a task ID.

    Args:
        task_id: Task ID
        backend: Optional backend
        store: Optional store

    Returns:
        ComputationHandle instance

    Example:
        >>> task_id = submit_task(my_func, 5)
        >>> handle = get_handle(task_id)
        >>> result = handle.get_result(timeout=10)
    """
    backend = backend if backend is not None else _get_default_backend()
    store = store if store is not None else _get_default_store()
    return ComputationHandle(task_id, store, backend)


def submit_many(
    tasks: list[tuple[Callable, tuple, dict]],
    backend: Optional[ComputationBackend] = None,
    store: Optional[ComputationStore] = None,
) -> list[str]:
    """Submit multiple tasks at once.

    Args:
        tasks: List of (func, args, kwargs) tuples
        backend: Optional backend
        store: Optional store

    Returns:
        List of task IDs

    Example:
        >>> tasks = [
        >>>     (func1, (1,), {}),
        >>>     (func2, (2,), {'multiplier': 3}),
        >>> ]
        >>> task_ids = submit_many(tasks)
    """
    backend = backend if backend is not None else _get_default_backend()
    store = store if store is not None else _get_default_store()

    task_ids = []
    for func, args, kwargs in tasks:
        task_id = submit_task(func, *args, backend=backend, store=store, **kwargs)
        task_ids.append(task_id)

    return task_ids


def get_many(
    task_ids: list[str],
    timeout: Optional[float] = None,
    store: Optional[ComputationStore] = None,
) -> list[Any]:
    """Get results for multiple tasks.

    Args:
        task_ids: List of task IDs
        timeout: Optional timeout (applies to each task)
        store: Optional store

    Returns:
        List of results

    Example:
        >>> task_ids = submit_many(tasks)
        >>> results = get_many(task_ids, timeout=10)
    """
    store = store if store is not None else _get_default_store()

    results = []
    for task_id in task_ids:
        result = get_result(task_id, timeout=timeout, store=store)
        results.append(result)

    return results
