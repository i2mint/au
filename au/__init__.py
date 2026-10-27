"""AU - Asynchronous Utilities

A lightweight, convention-over-configuration async framework for Python.
"""

import logging

# A library logs, the app decides where it goes: no output unless configured.
logging.getLogger("au").addHandler(logging.NullHandler())

# Core functionality
from au.base import (
    async_compute,
    ComputationHandle,
    ComputationStore,
    ComputationResult,
    ComputationStatus,
    ComputationCancelledError,
    SerializationFormat,
    FileSystemStore,
    ProcessBackend,
    ThreadBackend,
    StdLibQueueBackend,
    Middleware,
    LoggingMiddleware,
    MetricsMiddleware,
    SharedMetricsMiddleware,
    temporary_async_compute,
)

# Simplified API
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

# Configuration
from au.config import (
    AUConfig,
    get_config,
    get_global_config,
    set_global_config,
    reset_global_config,
)

# Retry and error handling
from au.retry import (
    RetryPolicy,
    BackoffStrategy,
    RetryState,
    retry_with_policy,
    RetryableError,
    NonRetryableError,
    DEFAULT_RETRY_POLICY,
    AGGRESSIVE_RETRY_POLICY,
    CONSERVATIVE_RETRY_POLICY,
    NETWORK_RETRY_POLICY,
)

# Testing utilities
from au.testing import (
    InMemoryStore,
    SyncTestBackend,
    TrackingTestBackend,
    MockTaskTracker,
    mock_async,
    create_test_backend,
    create_test_store,
)

# Workflow and dependencies
from au.workflow import (
    TaskGraph,
    WorkflowTask,
    WorkflowBuilder,
    TaskState,
    depends_on,
)

# Observability and hooks
from au.hooks import (
    HooksMiddleware,
    TracingMiddleware,
    MetricsCollectorMiddleware,
    CompositeMiddleware,
    TaskEvent,
    create_observability_middleware,
)

# Optional HTTP interface. Resolved lazily (PEP 562): importing ``au.http`` pulls in
# fastapi and flask, which costs hundreds of milliseconds that a caller using only the
# backends and stores should not pay. ``from au import mk_http_interface`` and ``au.http``
# keep working; only the moment of import moves.
__all_http__ = [
    "mk_http_interface",
    "create_app_from_decorator",
    "mk_flask_interface",
]


def __getattr__(name):
    if name == "http" or name in __all_http__:
        import importlib

        http = importlib.import_module("au.http")
        return http if name == "http" else getattr(http, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | set(__all_http__) | {"http"})


# Version
__version__ = "0.1.0"

__all__ = [
    # Core
    "async_compute",
    "ComputationHandle",
    "ComputationStore",
    "ComputationResult",
    "ComputationStatus",
    "ComputationCancelledError",
    "SerializationFormat",
    "FileSystemStore",
    "ProcessBackend",
    "ThreadBackend",
    "StdLibQueueBackend",
    "Middleware",
    "LoggingMiddleware",
    "MetricsMiddleware",
    "SharedMetricsMiddleware",
    "temporary_async_compute",
    # API
    "submit_task",
    "get_result",
    "get_status",
    "is_ready",
    "cancel_task",
    "async_task",
    "get_handle",
    "submit_many",
    "get_many",
    "set_default_backend",
    "set_default_store",
    # Config
    "AUConfig",
    "get_config",
    "get_global_config",
    "set_global_config",
    "reset_global_config",
    # Retry
    "RetryPolicy",
    "BackoffStrategy",
    "RetryState",
    "retry_with_policy",
    "RetryableError",
    "NonRetryableError",
    "DEFAULT_RETRY_POLICY",
    "AGGRESSIVE_RETRY_POLICY",
    "CONSERVATIVE_RETRY_POLICY",
    "NETWORK_RETRY_POLICY",
    # Testing
    "InMemoryStore",
    "SyncTestBackend",
    "TrackingTestBackend",
    "MockTaskTracker",
    "mock_async",
    "create_test_backend",
    "create_test_store",
    # Workflow
    "TaskGraph",
    "WorkflowTask",
    "WorkflowBuilder",
    "TaskState",
    "depends_on",
    # Hooks
    "HooksMiddleware",
    "TracingMiddleware",
    "MetricsCollectorMiddleware",
    "CompositeMiddleware",
    "TaskEvent",
    "create_observability_middleware",
] + __all_http__
