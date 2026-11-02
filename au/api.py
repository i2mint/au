"""
Simplified API for AU - direct task submission without decorators.

Provides simple functions for submitting and retrieving async tasks
without requiring decorator patterns.
"""

import functools
from datetime import timedelta
import logging
import math
from numbers import Real
from typing import Any, Callable, Optional, TypeVar
from contextlib import contextmanager, suppress

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
    LoggingMiddleware,
)
from au import base as _base
from au.config import get_global_config, setting_error
from au.retry import BackoffStrategy, RetryPolicy, retry_with_policy

T = TypeVar("T")


# Global default backend and store
_default_backend: Optional[ComputationBackend] = None
_default_store: Optional[ComputationStore] = None


TASKS_LOGGER = "au.tasks"
"""Where the simple API logs each task's lifecycle. It has a NullHandler: like
any library log, it shows only once the app configures logging (au's own
warnings and errors, on ``au.base``, still reach stderr by default)."""
logging.getLogger(TASKS_LOGGER).addHandler(logging.NullHandler())


def _middleware_from_config(config) -> list[Middleware]:
    """The middleware the observability settings ask for (``logging_enabled``,
    ``logging_level``), refusing a bad setting by its name."""
    if not config.logging_enabled:
        return []
    try:
        return [LoggingMiddleware(level=config.logging_level, logger_name=TASKS_LOGGER)]
    except (TypeError, ValueError) as e:
        raise setting_error("logging_level", f"is unusable: {e}") from e


def _retry_policy_from_config(config) -> Optional[RetryPolicy]:
    """The retry policy the retry settings ask for (``retry_enabled`` and the
    ``retry_*`` values), or None; refuses a bad value by its setting's name.

    It retries on every exception, a ``TypeError`` from bad arguments too: pass
    a ``RetryPolicy`` with ``retry_on`` / ``dont_retry_on`` to narrow it."""
    if not config.retry_enabled:
        return None
    try:
        backoff = BackoffStrategy(config.retry_backoff)
    except ValueError:
        choices = ", ".join(b.value for b in BackoffStrategy)
        raise setting_error(
            "retry_backoff", f"is {config.retry_backoff!r}; use one of: {choices}"
        ) from None
    if config.retry_max_attempts < 1:  # else the task would never run
        raise setting_error(
            "retry_max_attempts", f"is {config.retry_max_attempts}; it must be >= 1"
        )
    if config.retry_initial_delay < 0:
        raise setting_error(
            "retry_initial_delay", f"is {config.retry_initial_delay}; it must be >= 0"
        )
    return RetryPolicy(
        max_attempts=config.retry_max_attempts,
        backoff=backoff,
        initial_delay=config.retry_initial_delay,
    )


def _get_default_backend() -> ComputationBackend:
    """Get or create the default backend based on configuration, or build one
    with ``au.base._backend_factory`` when that is set (uncached)."""
    global _default_backend

    if _base._backend_factory is not None:
        config = get_global_config()
        return _base._backend_factory(None, _middleware_from_config(config))
    if _default_backend is None:
        config = get_global_config()
        middleware = _middleware_from_config(config)

        # Create backend based on config. Backends take the result store
        # per-launch (submit_task passes it), so no store is bound here.
        if config.backend == "redis":
            from au.backends.rq_backend import RQBackend
            import redis
            from rq import Queue

            redis_url = config.redis_url or "redis://localhost:6379"
            connection = redis.Redis.from_url(redis_url)
            _default_backend = RQBackend(
                rq_queue=Queue(connection=connection), middleware=middleware
            )

        elif config.backend == "supabase":
            from au.backends.supabase_backend import SupabaseQueueBackend

            if not config.supabase_url or not config.supabase_key:
                raise ValueError(
                    "Supabase backend requires AU_SUPABASE_URL and AU_SUPABASE_KEY"
                )
            from supabase import create_client

            client = create_client(config.supabase_url, config.supabase_key)
            _default_backend = SupabaseQueueBackend(
                supabase_client=client, middleware=middleware
            )

        elif config.backend == "process":
            from au.base import ProcessBackend

            _default_backend = ProcessBackend(middleware=middleware)

        elif config.backend == "stdlib":
            from au.base import StdLibQueueBackend

            _default_backend = StdLibQueueBackend(
                max_workers=config.max_workers,
                use_processes=False,
                middleware=middleware,
            )

        else:  # Default to thread
            from au.base import ThreadBackend

            _default_backend = ThreadBackend(middleware=middleware)

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


#: What makes a key "in flight" for an idempotent ``submit_task``: a record
#: that is RUNNING (missing, PENDING or terminal ones re-run). A RUNNING record
#: never expires (``is_expired`` is for finished ones), so it holds its key for
#: good unless ``reservation_lease_seconds`` sets a lease: see ``_reservation_lease``.
_IN_FLIGHT = frozenset({ComputationStatus.RUNNING})


def _reservation_lease(config) -> timedelta | None:
    """How long a RUNNING record holds its key against a resubmit, from
    ``reservation_lease_seconds`` (None: for good); refuses a bad value by name.

    Past it, the RUNNING record a crash left behind stops blocking its key
    (i2mint/au#36). The lease counts from the reservation until a worker records
    its start (``started_at``), so time spent queued counts too: set it above
    the queue wait plus the longest run. A job still running past its lease is
    run again by a resubmit, and its old worker's outcome may land over the new
    run's (no per-submission token fences it yet: i2mint/au#30).
    """
    lease = config.reservation_lease_seconds
    if lease is None:
        return None
    if isinstance(lease, Real) and not isinstance(lease, bool) and 0 < lease < math.inf:
        with suppress(OverflowError):  # finite, but past timedelta.max
            lease = timedelta(seconds=float(lease))
            if lease > timedelta(0):  # not below timedelta's microsecond
                return lease
    raise setting_error(
        "reservation_lease_seconds",
        f"is {lease!r}; it must be a finite number of seconds > 0",
    )


class _WithRetryPolicy:
    """``func`` retried under ``policy``. A class rather than a local function so
    it pickles by reference, which a spawned process needs (i2mint/au#7)."""

    def __init__(self, func: Callable, policy: RetryPolicy):
        self.func, self.policy = func, policy
        functools.update_wrapper(self, func)  # a name for logs and middleware

    def __call__(self, *args, **kwargs):
        return retry_with_policy(self.func, args, kwargs, self.policy)


def submit_task(
    func: Callable,
    *args,
    key: Optional[str] = None,
    backend: Optional[ComputationBackend] = None,
    store: Optional[ComputationStore] = None,
    retry_policy: Optional[RetryPolicy] = None,
    **kwargs,
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
        retry_policy: Optional retry policy. When omitted, the configured one
            applies (``AU_RETRY_ENABLED`` and the ``AU_RETRY_*`` settings);
            pass ``RetryPolicy(max_attempts=1)`` to run once regardless.
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
    config = get_global_config()
    if retry_policy is None:  # before the key is reserved: a bad setting raises
        retry_policy = _retry_policy_from_config(config)

    if key is None:
        # No idempotency requested: mint a fresh key per call.
        key = store.create_key()
    else:
        # Idempotency: reserve the key with RUNNING unless a computation is in
        # flight under it, in one compare-and-set, so of two concurrent
        # submits of one key only one launches (i2mint/au#31); a RUNNING record
        # older than the lease (a crash's) no longer counts (i2mint/au#36).
        lease = _reservation_lease(config)  # a bad setting raises before reserving
        reservation = ComputationResult(None, ComputationStatus.RUNNING)
        # Any mapping, as the workers' writes: not only a ComputationStore.
        reserved, _ = _base._write_unless(
            store, key, reservation, keep=_IN_FLIGHT, stale_after=lease
        )
        if not reserved:
            return key

    if retry_policy is not None:
        func = _WithRetryPolicy(func, retry_policy)

    # Launch task. A launch that raises must not leave the key RUNNING for good
    # (a resubmit with the same key would be taken for a live duplicate).
    try:
        backend.launch(func, args, kwargs, key, store)
    except Exception as e:
        store[key] = ComputationResult(None, ComputationStatus.FAILED, error=e)
        raise

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
    **kwargs,
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
