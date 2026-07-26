"""AU - Asynchronous Utilities

A lightweight, convention-over-configuration async framework for Python.
"""

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

# Optional HTTP interface (only if FastAPI is installed)
try:
    from au.http import (
        mk_http_interface,
        create_app_from_decorator,
        mk_flask_interface,
    )

    __all_http__ = [
        "mk_http_interface",
        "create_app_from_decorator",
        "mk_flask_interface",
    ]
except ImportError:
    __all_http__ = []

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
