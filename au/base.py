"""
Asynchronous computation framework with pluggable backends.

This module provides a decorator-based approach to transform synchronous functions
into asynchronous ones that return immediately with a handle to check status
and retrieve results later.

Architecture Overview:
--------------------
1. Storage abstraction (ComputationStore): Implements MutableMapping interface
2. Execution abstraction (ComputationBackend): Handles computation launching
3. Result handling (ComputationHandle): Clean API for status/result retrieval
4. Middleware system: For cross-cutting concerns like logging and metrics
5. Cleanup mechanisms: Automatic expiration of old results

Usage Patterns:
--------------
Simple file-based async:
    >>> @async_compute()
    ... def my_function(x):
    ...     return x * 2

Custom configuration (example setup):
    >>> import tempfile
    >>> with tempfile.TemporaryDirectory() as tmpdir:
    ...     store = FileSystemStore(tmpdir, ttl_seconds=3600)
    ...     backend = ProcessBackend(store, middleware=[LoggingMiddleware(), MetricsMiddleware()])
    ...     @async_compute(backend=backend, store=store)
    ...     def api_computation(data):
    ...         return data * 2
    ...     # Function is ready to use
    ...     True
    True

Shared infrastructure:
    >>> import tempfile
    >>> with tempfile.TemporaryDirectory() as tmpdir:
    ...     store = FileSystemStore(tmpdir, ttl_seconds=3600)
    ...     backend = ProcessBackend(store)
    ...     @async_compute(backend=backend, store=store)
    ...     def step1(x):
    ...         return x + 1
    ...     @async_compute(backend=backend, store=store)
    ...     def step2(x):
    ...         return x * 2
    ...     # Functions are ready to use
    ...     True
    True
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import wraps
from pathlib import Path
from typing import Any, Generic, Optional, TypeVar, Union, List, Dict
from collections.abc import Callable
import inspect
import json
import logging
import multiprocessing
import pickle
import time
import uuid
import concurrent.futures
import threading
import random
import shutil
import sys
import tempfile
from contextlib import contextmanager

logger = logging.getLogger(__name__)


def _au_worker_entrypoint(serialized_data: bytes) -> None:
    """
    Universal entry point for any worker processing an au task from a queue.

    Args:
        serialized_data: Pickled data containing func, args, kwargs, key,
                        store_reconstruction_info, and middleware_configs
    """
    try:
        # Deserialize the task data
        task_data = pickle.loads(serialized_data)
        func = task_data["func"]
        args = task_data["args"]
        kwargs = task_data["kwargs"]
        key = task_data["key"]
        store_info = task_data["store_reconstruction_info"]
        middleware_configs = task_data.get("middleware_configs", [])

        # Reconstruct the store
        store = _reconstruct_store(store_info)

        # Recreate middleware instances
        middleware_instances = _reconstruct_middleware(middleware_configs)

        _compute_and_record(
            func, args, kwargs, key=key, store=store, middleware=middleware_instances
        )

    except Exception as e:
        logger.error(f"Worker entrypoint failed: {e}", exc_info=True)
        # Always write a failed result to the store on error
        try:
            # Try to extract key and store if possible
            key = (
                task_data["key"]
                if "task_data" in locals() and "key" in task_data
                else None
            )
            store = (
                _reconstruct_store(task_data["store_reconstruction_info"])
                if "task_data" in locals() and "store_reconstruction_info" in task_data
                else None
            )
            if key and store:
                store[key] = ComputationResult(None, ComputationStatus.FAILED, error=e)
        except Exception as inner_e:
            logger.error(
                f"Failed to write failed result in worker entrypoint: {inner_e}"
            )


def _reconstruct_store(store_info: dict[str, Any]) -> "ComputationStore":
    """Reconstruct a ComputationStore from reconstruction info."""
    store_class = store_info["class"]

    if store_class == "FileSystemStore":
        return FileSystemStore(
            base_path=store_info["base_path"],
            suffix=store_info.get("suffix", ".json"),
            ttl_seconds=store_info.get("ttl_seconds"),
            serialization=SerializationFormat(store_info.get("serialization", "json")),
            auto_cleanup=store_info.get("auto_cleanup", True),
            cleanup_probability=store_info.get("cleanup_probability", 0.1),
        )
    else:
        raise ValueError(f"Unknown store class: {store_class}")


def _middleware_configs(middleware: list["Middleware"]) -> list[dict[str, Any]]:
    """How each middleware is rebuilt in a queue worker: its class and its
    :meth:`Middleware.worker_kwargs`.

    Refuses, at launch, a middleware those cannot rebuild, rather than letting the
    worker fail (or, as before, silently drop it).
    """
    configs = []
    for mw in middleware:
        cls, kwargs = type(mw), mw.worker_kwargs()
        try:
            inspect.signature(cls).bind(**kwargs)
        except TypeError as e:
            raise TypeError(
                f"{cls.__name__} cannot be rebuilt in a worker from {kwargs} ({e}): "
                "override its worker_kwargs() to return its constructor's arguments."
            ) from None
        configs.append({"class": cls, "kwargs": kwargs})
    return configs


def _reconstruct_middleware(
    middleware_configs: list[dict[str, Any]],
) -> list["Middleware"]:
    """Rebuild the middleware :func:`_middleware_configs` described."""
    return [config["class"](**config["kwargs"]) for config in middleware_configs]


class ComputationStatus(Enum):
    """Status of a computation task."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ComputationCancelledError(Exception):
    """Raised by :meth:`ComputationHandle.get_result` when a computation was cancelled.

    This lets callers distinguish a deliberate user cancellation (a first-class
    :attr:`ComputationStatus.CANCELLED`) from a genuine computation failure
    (:attr:`ComputationStatus.FAILED`) without string-sniffing the error message.
    """


class SerializationFormat(Enum):
    """Supported serialization formats."""

    JSON = "json"
    PICKLE = "pickle"


@dataclass
class ComputationResult:
    """
    Result of a computation with metadata.

    >>> result = ComputationResult("done", ComputationStatus.COMPLETED)
    >>> result.is_ready
    True
    """

    value: Any
    status: ComputationStatus
    error: Exception | None = None
    created_at: datetime = field(default_factory=datetime.now)
    completed_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    terminal_reason: str | None = None

    @property
    def is_ready(self) -> bool:
        """Check if computation reached a terminal state (success, failure, or cancellation)."""
        return self.status in (
            ComputationStatus.COMPLETED,
            ComputationStatus.FAILED,
            ComputationStatus.CANCELLED,
        )

    @property
    def duration(self) -> timedelta | None:
        """Get computation duration if completed."""
        if self.completed_at:
            return self.completed_at - self.created_at
        return None


T = TypeVar("T")


class ComputationStore(MutableMapping[str, ComputationResult], ABC):
    """
    Abstract base for storing computation results with TTL support.

    Stores should implement cleanup of expired results.
    """

    def __init__(self, *, ttl_seconds: int | None = None):
        """
        Initialize store with optional TTL.

        Args:
            ttl_seconds: Time-to-live for results in seconds
        """
        self.ttl_seconds = ttl_seconds

    @abstractmethod
    def create_key(self) -> str:
        """Generate a unique key for a new computation."""
        pass

    @abstractmethod
    def get_reconstruction_info(self) -> dict[str, Any]:
        """Get information needed to reconstruct this store in a worker process."""
        pass

    def is_expired(self, result: ComputationResult) -> bool:
        """Check if a result has expired based on TTL."""
        if self.ttl_seconds is None or not result.is_ready:
            return False

        expiry_time = result.created_at + timedelta(seconds=self.ttl_seconds)
        return datetime.now() > expiry_time

    @abstractmethod
    def cleanup_expired(self) -> int:
        """Remove expired results. Returns count of removed items."""
        pass


#: On Windows a file another handle has open cannot be replaced (``os.replace``)
#: or opened while it is being replaced: both raise ``PermissionError`` for the
#: few milliseconds a reader or writer holds it. Elsewhere that error is real.
_RETRY_PERMISSION_ERRORS = sys.platform == "win32"
_PERMISSION_RETRIES = 50
_PERMISSION_RETRY_SECONDS = 0.01


def _retrying_on_windows(operation: Callable[[], Any]) -> Any:
    """``operation()``, retried briefly while Windows holds the file (au#10)."""
    for attempt in range(_PERMISSION_RETRIES):
        try:
            return operation()
        except PermissionError:
            if not _RETRY_PERMISSION_ERRORS or attempt == _PERMISSION_RETRIES - 1:
                raise
            time.sleep(_PERMISSION_RETRY_SECONDS)


#: Where ``FileSystemStore`` writes a result before renaming it into place.
_WRITING_DIR = ".writing"


class FileSystemStore(ComputationStore):
    """
    Store computation results in the filesystem with automatic cleanup.

    >>> import tempfile
    >>> with tempfile.TemporaryDirectory() as tmpdir:
    ...     store = FileSystemStore(tmpdir, ttl_seconds=3600)
    ...     key = store.create_key()
    ...     store[key] = ComputationResult(42, ComputationStatus.COMPLETED)
    ...     store[key].value
    42
    """

    def __init__(
        self,
        base_path: str | Path,
        *,
        suffix: str = ".json",
        ttl_seconds: int | None = None,
        serialization: SerializationFormat = SerializationFormat.JSON,
        auto_cleanup: bool = True,
        cleanup_probability: float = 0.1,
    ):
        super().__init__(ttl_seconds=ttl_seconds)
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.suffix = suffix
        self.serialization = serialization
        self.auto_cleanup = auto_cleanup
        self.cleanup_probability = cleanup_probability

    def create_key(self) -> str:
        """Generate a unique filename."""
        return str(uuid.uuid4())

    def get_reconstruction_info(self) -> dict[str, Any]:
        """Get information needed to reconstruct this FileSystemStore in a worker process."""
        return {
            "class": "FileSystemStore",
            "base_path": str(self.base_path),
            "suffix": self.suffix,
            "ttl_seconds": self.ttl_seconds,
            "serialization": self.serialization.value,
            "auto_cleanup": self.auto_cleanup,
            "cleanup_probability": self.cleanup_probability,
        }

    def _get_path(self, key: str) -> Path:
        """Get the full path for a key."""
        return self.base_path / f"{key}{self.suffix}"

    def _maybe_cleanup(self) -> None:
        """Randomly trigger cleanup based on probability."""
        if self.auto_cleanup and self.ttl_seconds:
            import random

            if random.random() < self.cleanup_probability:
                self.cleanup_expired()

    def _serialize(self, result: ComputationResult) -> bytes:
        """Serialize result based on configured format."""
        data = {
            "value": result.value,
            "status": result.status.value,
            # ``str()`` of a bare KeyboardInterrupt or CancelledError is empty.
            "error": (
                (str(result.error) or type(result.error).__name__)
                if result.error
                else None
            ),
            "created_at": result.created_at.isoformat(),
            "completed_at": (
                result.completed_at.isoformat() if result.completed_at else None
            ),
            "metadata": result.metadata,
            "terminal_reason": result.terminal_reason,
        }

        if self.serialization == SerializationFormat.JSON:
            return json.dumps(data).encode()
        else:  # PICKLE
            return pickle.dumps(data)

    def _deserialize(self, data: bytes) -> ComputationResult:
        """Deserialize result based on configured format."""
        if self.serialization == SerializationFormat.JSON:
            obj = json.loads(data.decode())
        else:  # PICKLE
            obj = pickle.loads(data)

        return ComputationResult(
            value=obj.get("value"),
            status=ComputationStatus(obj["status"]),
            error=Exception(obj["error"]) if obj.get("error") else None,
            created_at=datetime.fromisoformat(obj["created_at"]),
            completed_at=(
                datetime.fromisoformat(obj["completed_at"])
                if obj.get("completed_at")
                else None
            ),
            metadata=obj.get("metadata", {}),
            terminal_reason=obj.get("terminal_reason"),
        )

    def __getitem__(self, key: str) -> ComputationResult:
        self._maybe_cleanup()
        return self._read(key)

    def _read(self, key: str) -> ComputationResult:
        """Read ``key`` without the chance of an automatic cleanup."""
        path = self._get_path(key)

        if not path.exists():
            return ComputationResult(None, ComputationStatus.PENDING)

        try:
            with _retrying_on_windows(lambda: open(path, "rb")) as f:
                data = f.read()
                if not data:
                    # Empty file: treat as failed
                    return ComputationResult(
                        None,
                        ComputationStatus.FAILED,
                        error=Exception(
                            f"Result file for key {key} is empty or incomplete."
                        ),
                    )
                return self._deserialize(data)
        except Exception as e:
            logger.error(f"Failed to read result for key {key}: {e}")
            return ComputationResult(None, ComputationStatus.FAILED, error=e)

    def __setitem__(self, key: str, result: ComputationResult) -> None:
        path = self._get_path(key)

        # Update completion time if transitioning to ready state
        if result.is_ready and result.completed_at is None:
            result.completed_at = datetime.now()

        # A temp file of its own per write, on the same filesystem so the rename
        # is atomic. A shared ``<key>.json.tmp`` let two writers of one key (the
        # worker's RUNNING and the caller's CANCELLED) interleave bytes into one
        # file, which a reader saw as ``Extra data`` (i2mint/au#6). It lives in a
        # subdirectory so no suffix can make it look like a key to ``__iter__``,
        # and its name does not grow the key's.
        tmp_dir = self.base_path / _WRITING_DIR
        tmp_path = tmp_dir / f"{uuid.uuid4().hex}.tmp"
        try:
            tmp_dir.mkdir(exist_ok=True)
            with open(tmp_path, "wb") as f:
                f.write(self._serialize(result))
            _retrying_on_windows(lambda: tmp_path.replace(path))
        except Exception as e:
            logger.error(f"Failed to write result for key {key}: {e}")
            tmp_path.unlink(missing_ok=True)

    def __delitem__(self, key: str) -> None:
        self._get_path(key).unlink(missing_ok=True)

    def __iter__(self):
        return (p.stem for p in self.base_path.glob(f"*{self.suffix}"))

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def get_reconstruction_info(self) -> dict[str, Any]:
        """Get information needed to reconstruct this FileSystemStore in a worker process."""
        return {
            "class": "FileSystemStore",
            "base_path": str(self.base_path),
            "suffix": self.suffix,
            "ttl_seconds": self.ttl_seconds,
            "serialization": self.serialization.value,
            "auto_cleanup": self.auto_cleanup,
            "cleanup_probability": self.cleanup_probability,
        }

    def cleanup_expired(self) -> int:
        """Remove expired results from filesystem."""
        removed = 0
        for key in list(self):  # Create list to avoid modification during iteration
            try:
                # Not `self[key]`: that may start an automatic cleanup of its
                # own, which removes the very items this call is counting
                # (a 10% flake in `test_cleanup_expired`, i2mint/au#6).
                result = self._read(key)
                if self.is_expired(result):
                    del self[key]
                    removed += 1
                    logger.debug(f"Cleaned up expired result: {key}")
            except Exception as e:
                logger.error(f"Error during cleanup of {key}: {e}")

        if removed > 0:
            logger.info(f"Cleaned up {removed} expired results")

        return removed


class Middleware(ABC):
    """Base class for middleware components."""

    @abstractmethod
    def before_compute(
        self, func: Callable, args: tuple, kwargs: dict, key: str
    ) -> None:
        """Called before computation starts."""
        pass

    @abstractmethod
    def after_compute(self, key: str, result: ComputationResult) -> None:
        """Called after computation completes."""
        pass

    @abstractmethod
    def on_error(self, key: str, error: Exception) -> None:
        """Called when computation fails."""
        pass

    def worker_kwargs(self) -> dict[str, Any]:
        """Constructor arguments that rebuild this middleware in a queue worker.

        Queue backends (``StdLibQueueBackend``, RQ, Supabase) send each
        middleware as its class plus these, and rebuild it on the worker. Override
        when the constructor needs arguments.
        """
        return {}


class LoggingMiddleware(Middleware):
    """
    Middleware for logging computation lifecycle.

    >>> middleware = LoggingMiddleware(level=logging.INFO)
    """

    def __init__(self, *, level: int = logging.DEBUG, logger_name: str | None = None):
        self.level = level
        self.logger = logging.getLogger(logger_name or __name__)

    def worker_kwargs(self) -> dict[str, Any]:
        return {"level": self.level, "logger_name": self.logger.name}

    def before_compute(
        self, func: Callable, args: tuple, kwargs: dict, key: str
    ) -> None:
        """Log computation start."""
        self.logger.log(self.level, f"Starting computation {key} for {func.__name__}")

    def after_compute(self, key: str, result: ComputationResult) -> None:
        """Log computation completion."""
        duration = result.duration
        duration_str = f" in {duration.total_seconds():.2f}s" if duration else ""
        self.logger.log(self.level, f"Completed computation {key}{duration_str}")

    def on_error(self, key: str, error: Exception) -> None:
        """Log computation error."""
        self.logger.error(f"Computation {key} failed: {error}", exc_info=True)


class MetricsMiddleware(Middleware):
    """
    Middleware for collecting computation metrics.

    >>> metrics = MetricsMiddleware()
    >>> metrics.get_stats()
    {'total': 0, 'completed': 0, 'failed': 0, 'avg_duration': 0.0}
    """

    def __init__(self):
        self.total_computations = 0
        self.completed_computations = 0
        self.failed_computations = 0
        self.total_duration = 0.0
        self._lock = multiprocessing.Lock()

    def before_compute(
        self, func: Callable, args: tuple, kwargs: dict, key: str
    ) -> None:
        """Increment total computations."""
        with self._lock:
            self.total_computations += 1

    def after_compute(self, key: str, result: ComputationResult) -> None:
        """Update completion metrics."""
        with self._lock:
            self.completed_computations += 1
            if result.duration:
                self.total_duration += result.duration.total_seconds()

    def on_error(self, key: str, error: Exception) -> None:
        """Update failure metrics."""
        with self._lock:
            self.failed_computations += 1

    def get_stats(self) -> dict[str, Any]:
        """Get current metrics."""
        with self._lock:
            avg_duration = (
                self.total_duration / self.completed_computations
                if self.completed_computations > 0
                else 0.0
            )
            return {
                "total": self.total_computations,
                "completed": self.completed_computations,
                "failed": self.failed_computations,
                "avg_duration": avg_duration,
            }


class SharedMetricsMiddleware(Middleware):
    """Metrics middleware using shared memory for multiprocessing."""

    def __init__(
        self,
        total_computations=None,
        completed_computations=None,
        failed_computations=None,
        total_duration=None,
        lock=None,
    ):
        import multiprocessing

        self.total_computations = total_computations or multiprocessing.Value("i", 0)
        self.completed_computations = completed_computations or multiprocessing.Value(
            "i", 0
        )
        self.failed_computations = failed_computations or multiprocessing.Value("i", 0)
        self.total_duration = total_duration or multiprocessing.Value("d", 0.0)
        self._lock = lock or multiprocessing.Lock()

    def before_compute(self, func, args, kwargs, key):
        with self._lock:
            self.total_computations.value += 1

    def after_compute(self, key, result):
        with self._lock:
            self.completed_computations.value += 1
            if result.duration:
                self.total_duration.value += result.duration.total_seconds()

    def on_error(self, key, error):
        with self._lock:
            self.failed_computations.value += 1

    def get_stats(self) -> dict:
        with self._lock:
            completed = self.completed_computations.value
            avg_duration = (
                self.total_duration.value / completed if completed > 0 else 0.0
            )
            return {
                "total": self.total_computations.value,
                "completed": completed,
                "failed": self.failed_computations.value,
                "avg_duration": avg_duration,
            }

    def worker_kwargs(self) -> dict[str, Any]:
        return self.get_shared_state()

    def get_shared_state(self):
        return dict(
            total_computations=self.total_computations,
            completed_computations=self.completed_computations,
            failed_computations=self.failed_computations,
            total_duration=self.total_duration,
            lock=self._lock,
        )


class ComputationBackend(ABC):
    """Abstract base for computation execution backends."""

    def __init__(self, middleware: list[Middleware] | None = None):
        self.middleware = middleware or []

    @abstractmethod
    def launch(
        self,
        func: Callable,
        args: tuple,
        kwargs: dict,
        key: str,
        store: "ComputationStore | None" = None,
    ) -> None:
        """
        Launch the computation asynchronously.

        The backend should execute ``func(*args, **kwargs)`` and store the result
        under ``key``. The result store is resolved by :meth:`_resolve_store`:
        ``store`` (passed per-launch by the simple ``au.api`` surface) takes
        precedence, falling back to the backend's own ``self.store`` (set at
        construction by the ``async_compute`` decorator path).
        """
        pass

    def _resolve_store(
        self, store: "ComputationStore | None" = None
    ) -> "ComputationStore":
        """Resolve the effective result store for a launch.

        Prefers the per-launch ``store`` argument, then the backend's own
        ``self.store``. Raises if neither is available so the failure is
        immediate and legible rather than a later ``AttributeError``.
        """
        store = store if store is not None else getattr(self, "store", None)
        if store is None:
            raise ValueError(
                f"{type(self).__name__}.launch requires a result store: pass one "
                f"to launch(..., store=...) or construct the backend with a store."
            )
        return store

    def terminate(self, key: str) -> None:
        """Attempt to terminate the computation. Default: do nothing."""
        pass

    def _run_middleware_before(
        self, func: Callable, args: tuple, kwargs: dict, key: str
    ) -> None:
        """Run all middleware before hooks."""
        _call_hooks(self.middleware, "before_compute", func, args, kwargs, key)

    def _run_middleware_after(self, key: str, result: ComputationResult) -> None:
        """Run all middleware after hooks."""
        _call_hooks(self.middleware, "after_compute", key, result)

    def _run_middleware_error(self, key: str, error: Exception) -> None:
        """Run all middleware error hooks."""
        _call_hooks(self.middleware, "on_error", key, error)


def _call_hooks(middleware: list["Middleware"], hook: str, *args) -> None:
    """Call ``hook`` on every middleware, whatever the others raise.

    A failing hook is logged. A ``BaseException`` from one is re-raised once all
    have run, so one hook cannot leave another middleware's key registered.
    """
    interrupt = None
    for mw in middleware:
        try:
            getattr(mw, hook)(*args)
        except Exception:
            logger.exception(f"Middleware {mw.__class__.__name__} {hook} failed")
        except BaseException as e:
            if interrupt is None:
                interrupt = e
            else:
                logger.exception(f"Middleware {mw.__class__.__name__} {hook} failed")
    if interrupt is not None:
        raise interrupt


def _compute_and_record(
    func: Callable,
    args: tuple,
    kwargs: dict,
    *,
    key: str,
    store: "ComputationStore",
    middleware: list["Middleware"],
) -> None:
    """Run ``func``, record its outcome under ``key``, and drive the middleware.

    The body the thread and queue workers run, and ProcessBackend's child. Once
    ``before_compute`` has run, exactly one of ``after_compute`` / ``on_error``
    runs too, whatever fails (a hook, the RUNNING write, the COMPLETED write, the
    FAILED write), so middleware that registers a key in one can always release
    it in the other. A ``BaseException`` (``asyncio.CancelledError``,
    ``SystemExit``, even ``sys.exit(0)``) is recorded as FAILED like any error,
    then re-raised (i2mint/au#12). A cancellation already recorded is final.
    """
    try:
        _call_hooks(middleware, "before_compute", func, args, kwargs, key)
        _mark_running(store, key)
        result = _record_unless_cancelled(
            store,
            key,
            ComputationResult(func(*args, **kwargs), ComputationStatus.COMPLETED),
        )
    except BaseException as error:
        failure = ComputationResult(None, ComputationStatus.FAILED, error=error)
        try:
            _record_unless_cancelled(store, key, failure)
        except Exception:
            logger.exception(f"Could not record the failure of {key}")
        _call_hooks(middleware, "on_error", key, error)
        if not isinstance(error, Exception):
            raise
    else:
        _call_hooks(middleware, "after_compute", key, result)


def _record_unless_cancelled(
    store: "ComputationStore", key: str, result: ComputationResult
) -> ComputationResult:
    """Record a worker's outcome unless the caller cancelled meanwhile, and
    return what the store now holds.

    A thread cannot be killed, so a cancelled one runs on; its outcome must not
    turn the cancellation back into COMPLETED or FAILED. A read that fails means
    "not known to be cancelled": the outcome is written rather than lost.
    """
    try:
        current = store.get(key)
    except Exception:
        current = None
    if current is not None and current.status == ComputationStatus.CANCELLED:
        return current
    store[key] = result
    return result


def _mark_running(store: "ComputationStore", key: str) -> None:
    """Record RUNNING, unless the computation was already cancelled or finished.

    A worker starts after the caller may have cancelled it; writing RUNNING over
    that CANCELLED makes the cancellation disappear (i2mint/au#6).
    """
    current = store.get(key)  # a store may raise KeyError for a key not yet written
    if current is None or not current.is_ready:
        store[key] = ComputationResult(None, ComputationStatus.RUNNING)


def _explained_start_failure(error: Exception) -> Exception:
    """The error to raise when a process could not be started.

    Where processes are spawned (Windows; macOS and Python 3.14 by default if
    the start method is changed) the work crosses by pickle at ``start()``: a
    lambda, a local function, or a name rebound by a decorator fails there with
    an error that does not say so. Starting is the only honest test of what a
    spawned process accepts (a ``multiprocessing.Queue`` pickles only then).
    """
    # Pickling failures surface as PicklingError, TypeError ("cannot pickle ...")
    # or AttributeError ("Can't get local object ...").
    pickling = isinstance(error, (pickle.PicklingError, TypeError, AttributeError))
    if multiprocessing.get_start_method() == "fork" or not pickling:
        return error
    return ValueError(
        f"The work cannot be sent to a spawned process ({type(error).__name__}: "
        f"{error}). The start method is {multiprocessing.get_start_method()!r}, "
        "which pickles the function, its arguments and the middleware: use a "
        "module-level function, or ThreadBackend."
    )


class ProcessBackend(ComputationBackend):
    """Execute computations in separate processes with middleware support."""

    def __init__(
        self,
        store: ComputationStore | None = None,
        middleware: list[Middleware] | None = None,
    ):
        super().__init__(middleware)
        self.store = store
        self._ensure_fork_method()
        self._processes = {}

    def _ensure_fork_method(self) -> None:
        """Prefer ``fork`` for better pickling compatibility, where it exists.

        ``fork`` does not exist on Windows, where asking for it raises
        ``ValueError`` (i2mint/au#7): there the platform default (``spawn``) is
        kept, and ``launch`` checks that the function can be pickled.
        """
        if "fork" not in multiprocessing.get_all_start_methods():
            return
        try:
            current_method = multiprocessing.get_start_method()
            if current_method != "fork":
                multiprocessing.set_start_method("fork", force=True)
        except RuntimeError:
            # Start method already set, which is fine
            pass

    def launch(
        self,
        func: Callable,
        args: tuple,
        kwargs: dict,
        key: str,
        store: ComputationStore | None = None,
    ) -> None:
        """Launch computation in a new process.

        The middleware goes to the child with the work and runs there only, so
        each ``before_compute`` is released by an ``after_compute`` or
        ``on_error`` in the same process (i2mint/au#14). Count across processes
        with :class:`SharedMetricsMiddleware`; a plain one counts in the child.
        """
        store = self._resolve_store(store)
        process = multiprocessing.Process(
            target=_compute_and_record,
            args=(func, args, kwargs),
            kwargs=dict(key=key, store=store, middleware=self.middleware),
        )
        self._processes[key] = process
        try:
            process.start()
        except Exception as e:
            self._processes.pop(key, None)
            raise _explained_start_failure(e) from e

    def terminate(self, key: str) -> None:
        proc = self._processes.get(key)
        if proc and proc.is_alive():
            proc.terminate()
            proc.join()


class StdLibQueueBackend(ComputationBackend):
    """
    Execute computations using Python's standard library concurrent.futures.

    This backend uses ThreadPoolExecutor or ProcessPoolExecutor for in-memory
    task queuing. Not persistent across application restarts.
    """

    def __init__(
        self,
        store: ComputationStore | None = None,
        max_workers: int = 5,
        use_processes: bool = True,
        middleware: list[Middleware] | None = None,
    ):
        super().__init__(middleware)
        self.store = store
        self.max_workers = max_workers
        self.use_processes = use_processes
        self.executor_cls = (
            concurrent.futures.ProcessPoolExecutor
            if use_processes
            else concurrent.futures.ThreadPoolExecutor
        )
        self._executor: None | (
            concurrent.futures.ThreadPoolExecutor
            | concurrent.futures.ProcessPoolExecutor
        ) = None
        self._futures: dict[str, concurrent.futures.Future] = {}
        self._started = False

    def _ensure_started(self) -> None:
        """Lazy initialization of the executor."""
        if not self._started:
            self._executor = self.executor_cls(max_workers=self.max_workers)
            self._started = True

    def launch(
        self,
        func: Callable,
        args: tuple,
        kwargs: dict,
        key: str,
        store: ComputationStore | None = None,
    ) -> None:
        """Launch computation using executor."""
        store = self._resolve_store(store)
        self._ensure_started()

        # Serialize task data
        task_data = {
            "func": func,
            "args": args,
            "kwargs": kwargs,
            "key": key,
            "store_reconstruction_info": store.get_reconstruction_info(),
            "middleware_configs": _middleware_configs(self.middleware),
        }
        serialized_data = pickle.dumps(task_data)

        # Submit to executor
        future = self._executor.submit(_au_worker_entrypoint, serialized_data)
        self._futures[key] = future

    def terminate(self, key: str) -> None:
        """Attempt to cancel a running task."""
        future = self._futures.get(key)
        if future:
            future.cancel()
            self._futures.pop(key, None)

    def shutdown(self) -> None:
        """Shutdown the executor."""
        if self._executor:
            self._executor.shutdown(wait=True)
            self._started = False

    def __enter__(self):
        """Context manager entry."""
        self._ensure_started()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.shutdown()


@dataclass
class ComputationHandle(Generic[T]):
    """
    Handle to check status and retrieve results of async computation.

    >>> handle = ComputationHandle("test-key", FileSystemStore("/tmp"))
    >>> handle.is_ready()
    False
    """

    key: str
    store: ComputationStore
    backend: ComputationBackend | None = None  # Add this

    def is_ready(self) -> bool:
        """Check if the computation is complete."""
        return self.store[self.key].is_ready

    def get_status(self) -> ComputationStatus:
        """Get the current status of the computation."""
        return self.store[self.key].status

    def get_result(self, *, timeout: float | None = None) -> T:
        """
        Get the computation result, optionally waiting for completion.

        Args:
            timeout: Maximum seconds to wait. None means no waiting.

        Raises:
            TimeoutError: If timeout expires before completion.
            Exception: If the computation failed.
        """

        def _poll_interval(elapsed: float) -> float:
            """Adaptive polling interval that increases with time."""
            if elapsed < 1:
                return 0.01
            elif elapsed < 10:
                return 0.1
            else:
                return 0.5

        start_time = time.time()

        while True:
            result = self.store[self.key]

            if result.status == ComputationStatus.COMPLETED:
                return result.value
            elif result.status == ComputationStatus.FAILED:
                error_msg = result.error or "Computation failed"
                raise RuntimeError(error_msg)
            elif result.status == ComputationStatus.CANCELLED:
                reason = result.terminal_reason or "Computation cancelled"
                raise ComputationCancelledError(reason)

            if timeout is None:
                break

            elapsed = time.time() - start_time
            if elapsed > timeout:
                raise TimeoutError(f"Computation not ready after {timeout}s")

            time.sleep(_poll_interval(elapsed))

        raise RuntimeError("Result not ready and no timeout specified")

    def cancel(self, *, reason: str = "Computation cancelled") -> bool:
        """
        Attempt to cancel the computation.

        Writes a first-class :attr:`ComputationStatus.CANCELLED` result (not a
        ``FAILED`` one), so callers can tell a deliberate cancellation apart from
        a genuine failure via :meth:`get_status` without inspecting error text.

        Args:
            reason: Human-readable reason stored on the result's
                ``terminal_reason`` (surfaced by
                :class:`ComputationCancelledError` from :meth:`get_result`).

        Returns:
            True if cancelled, False if already in a terminal state.

        Note:
            A thread, or a pool task that is already running, cannot be
            stopped; the au workers keep CANCELLED final anyway by not writing
            their outcome over it. ``after_compute`` then receives the CANCELLED
            result. A worker finishing in the instant between its check and its
            write can still overwrite it (no compare-and-set).
        """
        result = self.store[self.key]
        if not result.is_ready:
            self.store[self.key] = ComputationResult(
                None,
                ComputationStatus.CANCELLED,
                terminal_reason=reason,
            )
            if self.backend:
                self.backend.terminate(self.key)
                # A worker that started between the first write and the
                # terminate may have written RUNNING over it, and nothing writes
                # after the terminate returns (ProcessBackend waits for the
                # process to exit): say it again so CANCELLED is what is left.
                self.store[self.key] = ComputationResult(
                    None,
                    ComputationStatus.CANCELLED,
                    terminal_reason=reason,
                )
            return True
        return False

    @property
    def metadata(self) -> dict[str, Any]:
        """Get computation metadata."""
        return self.store[self.key].metadata


def async_compute(
    backend: ComputationBackend | None = None,
    store: ComputationStore | None = None,
    *,
    base_path: str = "/tmp/computations",
    ttl_seconds: int | None = 3600,
    serialization: SerializationFormat = SerializationFormat.JSON,
    middleware: list[Middleware] | None = None,
) -> Callable:
    """
    Decorator to make functions asynchronous with status tracking.

    Args:
        backend: Backend to execute computations (default: ProcessBackend)
        store: Store for results (default: FileSystemStore)
        base_path: Path for FileSystemStore if no store provided
        ttl_seconds: Time-to-live for results (default: 1 hour)
        serialization: Format for storing results
        middleware: List of middleware components

    Example:
        >>> @async_compute(
        ...     ttl_seconds=7200,
        ...     middleware=[LoggingMiddleware(), MetricsMiddleware()]
        ... )
        ... def slow_computation(x: int) -> int:
        ...     return x * x
        >>> handle = slow_computation(5)  # Returns immediately
        >>> result = handle.get_result(timeout=10)
    """
    if store is None:
        store = FileSystemStore(
            base_path,
            ttl_seconds=ttl_seconds,
            serialization=serialization,  # This was missing - use the passed serialization
        )

    if backend is None:
        backend = ProcessBackend(store, middleware=middleware)

    def decorator(func: Callable[..., T]) -> Callable[..., ComputationHandle[T]]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> ComputationHandle[T]:
            key = store.create_key()
            backend.launch(func, args, kwargs, key, store)
            return ComputationHandle(key, store, backend)

        # Attach utility methods
        wrapper.cleanup_expired = lambda: store.cleanup_expired()
        wrapper.store = store
        wrapper.backend = backend

        return wrapper

    return decorator


# Example implementations for other backends
class ThreadBackend(ComputationBackend):
    """
    Execute computations in separate threads (good for I/O-bound tasks).

    >>> import threading
    >>> store = FileSystemStore("/tmp/thread_computations")
    >>> backend = ThreadBackend(store)
    """

    def __init__(
        self,
        store: ComputationStore | None = None,
        middleware: list[Middleware] | None = None,
    ):
        super().__init__(middleware)
        self.store = store

    def launch(
        self,
        func: Callable,
        args: tuple,
        kwargs: dict,
        key: str,
        store: ComputationStore | None = None,
    ) -> None:
        """Launch computation in a new thread."""
        import threading

        store = self._resolve_store(store)

        def _worker():
            _compute_and_record(
                func, args, kwargs, key=key, store=store, middleware=self.middleware
            )

        thread = threading.Thread(target=_worker)
        thread.start()


class RemoteAPIBackend(ComputationBackend):
    """
    Example backend for remote computation services.

    This integrates with external APIs that have their own
    job tracking mechanisms.
    """

    def __init__(
        self,
        store: ComputationStore,
        *,
        api_url: str,
        api_key: str = "",
        middleware: list[Middleware] | None = None,
    ):
        super().__init__(middleware)
        self.store = store
        self.api_url = api_url
        self.api_key = api_key

    def launch(
        self,
        func: Callable,
        args: tuple,
        kwargs: dict,
        key: str,
        store: ComputationStore | None = None,
    ) -> None:
        """
        Launch computation via remote API.

        In a real implementation, this would:
        1. Serialize the function and arguments
        2. Submit to the remote API
        3. Poll for status updates
        4. Store results when complete
        """
        # This is a skeleton - actual implementation would depend on the API
        raise NotImplementedError("Implement based on your specific API")


# Context manager for temporary async computations
@contextmanager
def temporary_async_compute(**kwargs):
    """
    Context manager for temporary async computations with automatic cleanup.

    Example:
        >>> with temporary_async_compute(ttl_seconds=60) as async_func:
        ...     @async_func
        ...     def compute(x):
        ...         return x * 2
        ...     handle = compute(5)
        ...     result = handle.get_result(timeout=5)
    """
    import tempfile
    import shutil

    temp_dir = tempfile.mkdtemp()
    kwargs["base_path"] = temp_dir

    try:
        yield async_compute(**kwargs)
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


# Example usage patterns
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Clean up any old files first
    import shutil
    import tempfile

    base_dir = "/tmp/computations_clean"
    if Path(base_dir).exists():
        shutil.rmtree(base_dir)

    # Create middleware
    logging_mw = LoggingMiddleware(level=logging.INFO)
    metrics_mw = MetricsMiddleware()

    # Example with middleware and TTL
    @async_compute(
        base_path=base_dir,
        ttl_seconds=300,  # 5 minutes
        middleware=[logging_mw, metrics_mw],
        serialization=SerializationFormat.PICKLE,  # For complex objects
    )
    def expensive_calculation(n: int) -> dict[str, Any]:
        """Calculate factorial with metadata."""
        result = 1
        for i in range(1, n + 1):
            result *= i
            time.sleep(0.1)  # Simulate slow computation

        return {
            "factorial": result,
            "input": n,
            "timestamp": datetime.now().isoformat(),
        }

    # Launch computation
    handle = expensive_calculation(5)
    print(f"Computation launched with key: {handle.key}")

    # Check status
    print(f"Initial status: {handle.get_status()}")

    # Wait for result
    try:
        result = handle.get_result(timeout=10.0)  # Increased timeout
        print(f"Result: {result}")
    except TimeoutError:
        print("Computation still running...")

    # Check metrics (note: metrics won't be shared between processes)
    print(f"Main process metrics: {metrics_mw.get_stats()}")

    # Manual cleanup
    cleaned = expensive_calculation.cleanup_expired()
    print(f"Cleaned up {cleaned} expired results")
