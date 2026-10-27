# au

AU - Asynchronous Utilities

A lightweight, convention-over-configuration async framework for Python.

### Functions

| [`async_compute`](#au.async_compute)([backend, store, base_path, ...])   | Decorator to make functions asynchronous with status tracking.                 |
|----------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| [`temporary_async_compute`](#au.temporary_async_compute)(\*\*kwargs)               | Context manager for temporary async computations with automatic cleanup.       |
| [`submit_task`](#au.submit_task)(func, \*args[, key, backend, ...])    | Submit a task for async execution without decorator.                           |
| [`get_result`](#au.get_result)(task_id[, timeout, store])             | Get result for a task ID.                                                      |
| [`get_status`](#au.get_status)(task_id[, store])                      | Get status for a task ID.                                                      |
| [`is_ready`](#au.is_ready)(task_id[, store])                        | Check if task is ready (completed or failed).                                  |
| [`cancel_task`](#au.cancel_task)(task_id[, backend, store])            | Cancel a running task.                                                         |
| [`async_task`](#au.async_task)(func, \*args[, backend, store, ...])   | Context manager for async task execution.                                      |
| [`get_handle`](#au.get_handle)(task_id[, backend, store])             | Get a handle for a task ID.                                                    |
| [`submit_many`](#au.submit_many)(tasks[, backend, store])              | Submit multiple tasks at once.                                                 |
| [`get_many`](#au.get_many)(task_ids[, timeout, store])              | Get results for multiple tasks.                                                |
| [`set_default_backend`](#au.set_default_backend)(backend)                      | Set the default backend for simple API calls.                                  |
| [`set_default_store`](#au.set_default_store)(store)                          | Set the default store for simple API calls.                                    |
| [`get_config`](#au.get_config)([config_file])                         | Get configuration with cascade: env vars → config file → defaults → overrides. |
| [`get_global_config`](#au.get_global_config)()                               | Get the global configuration instance.                                         |
| [`set_global_config`](#au.set_global_config)(config)                         | Set the global configuration instance.                                         |
| [`reset_global_config`](#au.reset_global_config)()                             | Reset the global configuration to None.                                        |
| [`retry_with_policy`](#au.retry_with_policy)(func[, args, kwargs, policy])   | Execute a function with retry policy.                                          |
| [`mock_async`](#au.mock_async)([backend])                             | Context manager for mocking async execution.                                   |
| [`create_test_backend`](#au.create_test_backend)(\*\*kwargs)                   | Create a test backend with optional configuration.                             |
| [`create_test_store`](#au.create_test_store)(\*\*kwargs)                     | Create a test store with optional configuration.                               |
| [`depends_on`](#au.depends_on)(\*dependency_funcs)                    | Decorator to specify task dependencies.                                        |
| [`create_observability_middleware`](#au.create_observability_middleware)([...])            | Create a complete observability middleware stack.                              |
| [`mk_http_interface`](#au.mk_http_interface)([functions, backend, ...])      | Create a FastAPI application for task management.                              |
| [`create_app_from_decorator`](#au.create_app_from_decorator)([title, description])   | Create a FastAPI app that auto-discovers @async_compute decorated functions.   |
| [`mk_flask_interface`](#au.mk_flask_interface)([functions, backend, store])   | Create a Flask application for task management.                                |

### Classes

| [`ComputationHandle`](#au.ComputationHandle)(key, store[, backend])          | Handle to check status and retrieve results of async computation.                     |
|----------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| [`ComputationStore`](#au.ComputationStore)(\*[, ttl_seconds])               | Abstract base for storing computation results with TTL support.                       |
| [`ComputationResult`](#au.ComputationResult)(value, status[, error, ...])    | Result of a computation with metadata.                                                |
| [`ComputationStatus`](#au.ComputationStatus)(\*values)                       | Status of a computation task.                                                         |
| [`SerializationFormat`](#au.SerializationFormat)(\*values)                     | Supported serialization formats.                                                      |
| [`FileSystemStore`](#au.FileSystemStore)(base_path, \*[, suffix, ...])     | Store computation results in the filesystem with automatic cleanup.                   |
| [`ProcessBackend`](#au.ProcessBackend)([store, middleware])               | Execute computations in separate processes with middleware support.                   |
| [`ThreadBackend`](#au.ThreadBackend)([store, middleware])                | Execute computations in separate threads (good for I/O-bound tasks).                  |
| [`StdLibQueueBackend`](#au.StdLibQueueBackend)([store, max_workers, ...])     | Execute computations using Python's standard library concurrent.futures.              |
| [`Middleware`](#au.Middleware)()                                      | Base class for middleware components.                                                 |
| [`LoggingMiddleware`](#au.LoggingMiddleware)(\*[, level, logger_name])       | Middleware for logging computation lifecycle.                                         |
| [`MetricsMiddleware`](#au.MetricsMiddleware)()                               | Middleware for collecting computation metrics, in the copy that runs the computation. |
| [`AUConfig`](#au.AUConfig)([backend, redis_url, supabase_url, ...]) | Configuration for AU with smart defaults.                                             |
| [`RetryPolicy`](#au.RetryPolicy)([max_attempts, backoff, ...])         | Configuration for retry behavior.                                                     |
| [`BackoffStrategy`](#au.BackoffStrategy)(\*values)                         | Backoff strategy for retries.                                                         |
| [`RetryState`](#au.RetryState)([attempt_count, last_error, ...])      | State tracking for retries.                                                           |
| [`InMemoryStore`](#au.InMemoryStore)([ttl_seconds])                      | In-memory store for testing.                                                          |
| [`SyncTestBackend`](#au.SyncTestBackend)([middleware])                     | Synchronous test backend that executes immediately.                                   |
| [`TrackingTestBackend`](#au.TrackingTestBackend)([middleware])                 | Test backend that tracks all executions.                                              |
| [`MockTaskTracker`](#au.MockTaskTracker)([task_count, tasks_by_name, ...]) | Tracks task executions for testing.                                                   |
| [`TaskGraph`](#au.TaskGraph)([backend, store])                       | Directed Acyclic Graph (DAG) for task execution.                                      |
| [`WorkflowTask`](#au.WorkflowTask)(func[, args, kwargs, ...])           | A task in a workflow with dependencies.                                               |
| [`WorkflowBuilder`](#au.WorkflowBuilder)([backend, store])                 | Fluent interface for building workflows.                                              |
| [`TaskState`](#au.TaskState)(\*values)                               | State of a task in a workflow.                                                        |
| [`HooksMiddleware`](#au.HooksMiddleware)([on_start, on_complete, ...])     | Middleware that provides lifecycle hooks.                                             |
| [`TracingMiddleware`](#au.TracingMiddleware)([service_name, trace_backend])  | Middleware for distributed tracing (OpenTelemetry compatible).                        |
| [`MetricsCollectorMiddleware`](#au.MetricsCollectorMiddleware)([metrics_backend])     | Enhanced metrics middleware with histogram support.                                   |
| [`CompositeMiddleware`](#au.CompositeMiddleware)(middlewares)                  | Combines multiple middleware into one.                                                |
| [`TaskEvent`](#au.TaskEvent)(task_id, event_type[, timestamp, data]) | Event fired during task lifecycle.                                                    |

### Exceptions

| [`ComputationCancelledError`](#au.ComputationCancelledError)   | Raised by [`ComputationHandle.get_result()`](#au.ComputationHandle.get_result) when a computation was cancelled.   |
|------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| [`RetryableError`](#au.RetryableError)              | Base class for errors that should be retried.                                                                                 |
| [`NonRetryableError`](#au.NonRetryableError)           | Base class for errors that should not be retried.                                                                             |

### *class* au.AUConfig(backend='thread', redis_url=None, supabase_url=None, supabase_key=None, max_workers=4, storage='filesystem', storage_path='/tmp/au_tasks', ttl_seconds=3600, serialization='json', retry_enabled=False, retry_max_attempts=3, retry_backoff='exponential', retry_initial_delay=1.0, logging_enabled=True, logging_level='INFO', metrics_enabled=False, http_host='127.0.0.1', http_port=8000, http_framework='fastapi')

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Configuration for AU with smart defaults.

### *class* au.BackoffStrategy(\*values)

Bases: [`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

Backoff strategy for retries.

### *class* au.CompositeMiddleware(middlewares)

Bases: [`Middleware`](au.base.html.md#au.base.Middleware)

Combines multiple middleware into one.

Allows using multiple middleware together.

#### after_compute(key, result)

Call after_compute on all middleware (base.Middleware protocol).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### before_compute(func, args, kwargs, key)

Call before_compute on all middleware (base.Middleware protocol).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### on_error(key, error)

Call on_error on all middleware (base.Middleware protocol).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *exception* au.ComputationCancelledError

Bases: [`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)

Raised by [`ComputationHandle.get_result()`](#au.ComputationHandle.get_result) when a computation was cancelled.

This lets callers distinguish a deliberate user cancellation (a first-class
`ComputationStatus.CANCELLED`) from a genuine computation failure
(`ComputationStatus.FAILED`) without string-sniffing the error message.

### *class* au.ComputationHandle(key, store, backend=None)

Bases: [`Generic`](https://docs.python.org/3/library/typing.html#typing.Generic)[`T`]

Handle to check status and retrieve results of async computation.

```pycon
>>> handle = ComputationHandle("test-key", FileSystemStore("/tmp"))
>>> handle.is_ready()
False
```

#### cancel(, reason='Computation cancelled')

Attempt to cancel the computation.

Writes a first-class `ComputationStatus.CANCELLED` result (not a
`FAILED` one), so callers can tell a deliberate cancellation apart from
a genuine failure via [`get_status()`](#au.get_status) without inspecting error text.

* **Parameters:**
  **reason** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Human-readable reason stored on the result’s
  `terminal_reason` (surfaced by
  [`ComputationCancelledError`](#au.ComputationCancelledError) from [`get_result()`](#au.get_result)).
* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)
* **Returns:**
  True if cancelled, False if already in a terminal state.

#### NOTE
A thread, or a pool task that is already running, cannot be
stopped; the au workers keep CANCELLED final anyway by not writing
their outcome over it. `after_compute` then receives the CANCELLED
result. A worker finishing in the instant between its check and its
write can still overwrite it (no compare-and-set).

#### get_result(, timeout=None)

Get the computation result, optionally waiting for completion.

* **Parameters:**
  **timeout** ([`float`](https://docs.python.org/3/builtins/functions.html#float) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – Maximum seconds to wait. None means no waiting.
* **Raises:**
  * [**TimeoutError**](https://docs.python.org/3/builtins/exceptions.html#TimeoutError) – If timeout expires before completion.
  * [**Exception**](https://docs.python.org/3/builtins/exceptions.html#Exception) – If the computation failed.
* **Return type:**
  `T`

#### get_status()

Get the current status of the computation.

* **Return type:**
  [`ComputationStatus`](au.base.html.md#au.base.ComputationStatus)

#### is_ready()

Check if the computation is complete.

* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)

#### *property* metadata *: [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [Any](https://docs.python.org/3/library/typing.html#typing.Any)]*

Get computation metadata.

### *class* au.ComputationResult(value, status, error=None, created_at=datetime.now(), completed_at=None, metadata=dict(), terminal_reason=None)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Result of a computation with metadata.

```pycon
>>> result = ComputationResult("done", ComputationStatus.COMPLETED)
>>> result.is_ready
True
```

#### *property* duration *: [timedelta](https://docs.python.org/3/library/datetime.html#datetime.timedelta) | [None](https://docs.python.org/3/builtins/constants.html#None)*

Get computation duration if completed.

#### *property* is_ready *: [bool](https://docs.python.org/3/builtins/functions.html#bool)*

Check if computation reached a terminal state (success, failure, or cancellation).

### *class* au.ComputationStatus(\*values)

Bases: [`Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

Status of a computation task.

### *class* au.ComputationStore(, ttl_seconds=None)

Bases: [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`ComputationResult`](au.base.html.md#au.base.ComputationResult)], [`ABC`](https://docs.python.org/3/library/abc.html#abc.ABC)

Abstract base for storing computation results with TTL support.

Stores should implement cleanup of expired results.

#### *abstractmethod* cleanup_expired()

Remove expired results. Returns count of removed items.

* **Return type:**
  [`int`](https://docs.python.org/3/builtins/functions.html#int)

#### *abstractmethod* create_key()

Generate a unique key for a new computation.

* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

#### *abstractmethod* get_reconstruction_info()

Get information needed to reconstruct this store in a worker process.

* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]

#### is_expired(result)

Check if a result has expired based on TTL.

* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)

### *class* au.FileSystemStore(base_path, , suffix='.json', ttl_seconds=None, serialization=SerializationFormat.JSON, auto_cleanup=True, cleanup_probability=0.1)

Bases: [`ComputationStore`](au.base.html.md#au.base.ComputationStore)

Store computation results in the filesystem with automatic cleanup.

```pycon
>>> import tempfile
>>> with tempfile.TemporaryDirectory() as tmpdir:
...     store = FileSystemStore(tmpdir, ttl_seconds=3600)
...     key = store.create_key()
...     store[key] = ComputationResult(42, ComputationStatus.COMPLETED)
...     store[key].value
42
```

#### cleanup_expired()

Remove expired results from filesystem.

* **Return type:**
  [`int`](https://docs.python.org/3/builtins/functions.html#int)

#### create_key()

Generate a unique filename.

* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

#### get_reconstruction_info()

Get information needed to reconstruct this FileSystemStore in a worker process.

* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]

### *class* au.HooksMiddleware(on_start=None, on_complete=None, on_error=None, on_retry=None)

Bases: [`Middleware`](au.base.html.md#au.base.Middleware)

Middleware that provides lifecycle hooks.

Allows registering callbacks for different task events.

#### after_compute(key, result)

Called after successful computation (base.Middleware protocol).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### before_compute(func, args, kwargs, key)

Called before computation starts (base.Middleware protocol).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### get_events()

Get all recorded events.

* **Return type:**
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`TaskEvent`](au.hooks.html.md#au.hooks.TaskEvent)]
* **Returns:**
  List of task events

#### on_error(key, error)

Called when computation fails (base.Middleware protocol).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* au.InMemoryStore(ttl_seconds=3600)

Bases: [`ComputationStore`](au.base.html.md#au.base.ComputationStore)

In-memory store for testing.

Stores results in a dictionary without any persistence.
Useful for testing without filesystem dependencies.

#### cleanup_expired()

No-op for in-memory store (no automatic expiration).

* **Return type:**
  [`int`](https://docs.python.org/3/builtins/functions.html#int)
* **Returns:**
  0 (nothing cleaned up)

#### clear()

Clear all stored results.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### create_key()

Create a unique key for a computation.

* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

#### get_reconstruction_info()

Get info needed to reconstruct this store.

* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)
* **Returns:**
  Dictionary with store type and parameters

### *class* au.LoggingMiddleware(, level=10, logger_name=None)

Bases: [`Middleware`](au.base.html.md#au.base.Middleware)

Middleware for logging computation lifecycle.

```pycon
>>> middleware = LoggingMiddleware(level=logging.INFO)
```

#### after_compute(key, result)

Log computation completion.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### before_compute(func, args, kwargs, key)

Log computation start.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### on_error(key, error)

Log computation error.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* au.MetricsCollectorMiddleware(metrics_backend=None)

Bases: [`Middleware`](au.base.html.md#au.base.Middleware)

Enhanced metrics middleware with histogram support.

Collects detailed metrics about task execution.

#### after_compute(key, result)

Record successful completion metrics (base.Middleware protocol).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### before_compute(func, args, kwargs, key)

Track function invocation (base.Middleware protocol).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### get_metrics()

Get collected metrics.

* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]
* **Returns:**
  Dictionary of metrics

#### on_error(key, error)

Record error metrics (base.Middleware protocol).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* au.MetricsMiddleware

Bases: [`Middleware`](au.base.html.md#au.base.Middleware)

Middleware for collecting computation metrics, in the copy that runs the
computation.

Every backend but ThreadBackend runs a copy (a child process’s, or one a
queue worker unpickled), so there the instance you hold counts nothing. Use
`SharedMetricsMiddleware` with ProcessBackend to count across
processes.

```pycon
>>> metrics = MetricsMiddleware()
>>> metrics.get_stats()
{'total': 0, 'completed': 0, 'failed': 0, 'avg_duration': 0.0}
```

#### after_compute(key, result)

Update completion metrics.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### before_compute(func, args, kwargs, key)

Increment total computations.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### get_stats()

Get current metrics.

* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]

#### on_error(key, error)

Update failure metrics.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* au.Middleware

Bases: [`ABC`](https://docs.python.org/3/library/abc.html#abc.ABC)

Base class for middleware components.

#### *abstractmethod* after_compute(key, result)

Called after computation completes.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### *abstractmethod* before_compute(func, args, kwargs, key)

Called before computation starts.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### *abstractmethod* on_error(key, error)

Called when computation fails (`error` may be a BaseException).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* au.MockTaskTracker(task_count=0, tasks_by_name=dict(), all_tasks=list())

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Tracks task executions for testing.

#### task_count

Total number of tasks executed

* **Type:**
  [`int`](https://docs.python.org/3/builtins/functions.html#int)

#### tasks_by_name

Dictionary mapping function names to call records

* **Type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`TaskCallRecord`](au.testing.html.md#au.testing.TaskCallRecord)]]

#### all_tasks

List of all task call records

* **Type:**
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`TaskCallRecord`](au.testing.html.md#au.testing.TaskCallRecord)]

#### call_count(func_name)

Get call count for a specific function.

* **Parameters:**
  **func_name** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Name of the function
* **Return type:**
  [`int`](https://docs.python.org/3/builtins/functions.html#int)
* **Returns:**
  Number of calls

#### get_calls(func_name)

Get all calls for a specific function.

* **Parameters:**
  **func_name** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Name of the function
* **Return type:**
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`TaskCallRecord`](au.testing.html.md#au.testing.TaskCallRecord)]
* **Returns:**
  List of call records

#### last_call(func_name)

Get last call for a specific function.

* **Parameters:**
  **func_name** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Name of the function
* **Return type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`TaskCallRecord`](au.testing.html.md#au.testing.TaskCallRecord)]
* **Returns:**
  Last call record or None

#### record_call(func_name, args, kwargs)

Record a task call.

* **Parameters:**
  * **func_name** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Name of the function
  * **args** ([`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple)) – Positional arguments
  * **kwargs** ([`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)) – Keyword arguments

### *exception* au.NonRetryableError

Bases: [`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)

Base class for errors that should not be retried.

### *class* au.ProcessBackend(store=None, middleware=None)

Bases: [`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)

Execute computations in separate processes with middleware support.

#### launch(func, args, kwargs, key, store=None)

Launch computation in a new process.

The middleware goes to the child with the work and runs there only, so
each `before_compute` is released by an `after_compute` or
`on_error` in the same process (i2mint/au#14). Count across processes
with `SharedMetricsMiddleware`; a plain one counts in the child.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### terminate(key)

Attempt to terminate the computation. Default: do nothing.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* au.RetryPolicy(max_attempts=3, backoff=BackoffStrategy.EXPONENTIAL, initial_delay=1.0, max_delay=60.0, retry_on=list(), dont_retry_on=list(), on_retry=None)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Configuration for retry behavior.

#### max_attempts

Maximum number of retry attempts (including initial try)

* **Type:**
  [`int`](https://docs.python.org/3/builtins/functions.html#int)

#### backoff

Backoff strategy (exponential, linear, constant)

* **Type:**
  [`BackoffStrategy`](au.retry.html.md#au.retry.BackoffStrategy)

#### initial_delay

Initial delay in seconds before first retry

* **Type:**
  [`float`](https://docs.python.org/3/builtins/functions.html#float)

#### max_delay

Maximum delay between retries

* **Type:**
  [`float`](https://docs.python.org/3/builtins/functions.html#float)

#### retry_on

List of exception types to retry on (empty means retry all)

* **Type:**
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`Type`](https://docs.python.org/3/library/typing.html#typing.Type)[[`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)]]

#### dont_retry_on

List of exception types to never retry

* **Type:**
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`Type`](https://docs.python.org/3/library/typing.html#typing.Type)[[`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)]]

#### on_retry

Optional callback called before each retry

* **Type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`Callable`](https://docs.python.org/3/library/typing.html#typing.Callable)[[[`int`](https://docs.python.org/3/builtins/functions.html#int), [`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)], [`None`](https://docs.python.org/3/builtins/constants.html#None)]]

#### get_delay(attempt)

Calculate delay before next retry.

* **Parameters:**
  **attempt** ([`int`](https://docs.python.org/3/builtins/functions.html#int)) – Current attempt number (1-indexed)
* **Return type:**
  [`float`](https://docs.python.org/3/builtins/functions.html#float)
* **Returns:**
  Delay in seconds

#### should_retry(attempt, error)

Determine if we should retry given an attempt number and error.

* **Parameters:**
  * **attempt** ([`int`](https://docs.python.org/3/builtins/functions.html#int)) – Current attempt number (1-indexed)
  * **error** ([`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)) – Exception that occurred
* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)
* **Returns:**
  True if should retry, False otherwise

### *class* au.RetryState(attempt_count=0, last_error=None, will_retry=False, next_retry_at=None, retry_history=list())

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

State tracking for retries.

#### attempt_count

Number of attempts made

* **Type:**
  [`int`](https://docs.python.org/3/builtins/functions.html#int)

#### last_error

Last exception encountered

* **Type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)]

#### will_retry

Whether another retry will be attempted

* **Type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)

#### next_retry_at

Timestamp of next retry attempt

* **Type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`float`](https://docs.python.org/3/builtins/functions.html#float)]

#### retry_history

List of (timestamp, exception) tuples

* **Type:**
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[`float`](https://docs.python.org/3/builtins/functions.html#float), [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]]

#### add_attempt(error, will_retry, next_retry_at=None)

Record a retry attempt.

* **Parameters:**
  * **error** ([`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)) – Exception that occurred
  * **will_retry** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool)) – Whether another retry will happen
  * **next_retry_at** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`float`](https://docs.python.org/3/builtins/functions.html#float)]) – Timestamp of next retry

### *exception* au.RetryableError

Bases: [`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)

Base class for errors that should be retried.

### *class* au.SerializationFormat(\*values)

Bases: [`Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

Supported serialization formats.

### *class* au.StdLibQueueBackend(store=None, max_workers=5, use_processes=True, middleware=None)

Bases: [`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)

Execute computations using Python’s standard library concurrent.futures.

This backend uses ThreadPoolExecutor or ProcessPoolExecutor for in-memory
task queuing. Not persistent across application restarts.

#### launch(func, args, kwargs, key, store=None)

Launch computation using executor.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### shutdown()

Shutdown the executor.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### terminate(key)

Attempt to cancel a running task.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* au.SyncTestBackend(middleware=None)

Bases: [`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)

Synchronous test backend that executes immediately.

This backend runs computations synchronously in the current process/thread,
making it ideal for testing without the complexity of actual async execution.

#### get_execution(key)

Get recorded execution for a key.

* **Parameters:**
  **key** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Computation key
* **Return type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[`Callable`](https://docs.python.org/3/library/typing.html#typing.Callable), [`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple), [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)]]
* **Returns:**
  Tuple of (function, args, kwargs) or None

#### launch(func, args, kwargs, key, store=None)

Execute function synchronously and store result immediately.

Runs the body every au worker runs, so it records results as they do:
`result.error` is the exception itself, and a `BaseException` is
recorded FAILED, then re-raised to the caller.

* **Parameters:**
  * **func** ([`Callable`](https://docs.python.org/3/library/typing.html#typing.Callable)) – Function to execute
  * **args** ([`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple)) – Positional arguments
  * **kwargs** ([`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)) – Keyword arguments
  * **key** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Result key
  * **store** ([`ComputationStore`](au.base.html.md#au.base.ComputationStore) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – Store for results (falls back to the backend’s own store)
* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### terminate(key)

No-op for synchronous backend (already completed).

* **Parameters:**
  **key** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Computation key
* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* au.TaskEvent(task_id, event_type, timestamp=datetime.now(), data=dict())

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Event fired during task lifecycle.

#### task_id

Task identifier

* **Type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

#### event_type

Type of event (start, complete, error, retry)

* **Type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)

#### timestamp

When event occurred

* **Type:**
  [`datetime`](https://docs.python.org/3/library/datetime.html#datetime.datetime)

#### data

Event-specific data

* **Type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]

### *class* au.TaskGraph(backend=None, store=None)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Directed Acyclic Graph (DAG) for task execution.

Manages task dependencies and orchestrates execution.

#### add_task(func, \*args, depends_on=None, task_id=None, \*\*kwargs)

Add a task to the graph.

* **Parameters:**
  * **func** ([`Callable`](https://docs.python.org/3/library/typing.html#typing.Callable)) – Function to execute
  * **\*args** – Positional arguments
  * **depends_on** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]]) – Optional list of task IDs this depends on
  * **task_id** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]) – Optional custom task ID
  * **\*\*kwargs** – Keyword arguments
* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)
* **Returns:**
  Task ID

### Example

```pycon
>>> graph = TaskGraph()
>>> t1 = graph.add_task(step1, 5)
>>> t2 = graph.add_task(step2, 10)
>>> t3 = graph.add_task(step3, depends_on=[t1, t2])
```

#### execute(timeout=None)

Execute all tasks in dependency order.

* **Parameters:**
  **timeout** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`float`](https://docs.python.org/3/builtins/functions.html#float)]) – Optional overall timeout in seconds
* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]
* **Returns:**
  Dictionary mapping task IDs to results
* **Raises:**
  * [**ValueError**](https://docs.python.org/3/builtins/exceptions.html#ValueError) – If circular dependencies detected
  * [**TimeoutError**](https://docs.python.org/3/builtins/exceptions.html#TimeoutError) – If overall timeout exceeded

### Example

```pycon
>>> graph = TaskGraph()
>>> t1 = graph.add_task(step1, 5)
>>> t2 = graph.add_task(step2, depends_on=[t1])
>>> results = graph.execute()
>>> print(results[t2])
```

#### get_task(task_id)

Get a task by ID.

* **Parameters:**
  **task_id** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Task ID
* **Return type:**
  [`WorkflowTask`](au.workflow.html.md#au.workflow.WorkflowTask)
* **Returns:**
  WorkflowTask instance
* **Raises:**
  [**KeyError**](https://docs.python.org/3/builtins/exceptions.html#KeyError) – If task ID not found

#### get_task_result(task_id)

Get result for a task.

* **Parameters:**
  **task_id** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Task ID
* **Return type:**
  [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)
* **Returns:**
  Task result
* **Raises:**
  [**ValueError**](https://docs.python.org/3/builtins/exceptions.html#ValueError) – If task not completed or failed

### *class* au.TaskState(\*values)

Bases: [`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

State of a task in a workflow.

### *class* au.ThreadBackend(store=None, middleware=None)

Bases: [`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)

Execute computations in separate threads (good for I/O-bound tasks).

```pycon
>>> import threading
>>> store = FileSystemStore("/tmp/thread_computations")
>>> backend = ThreadBackend(store)
```

#### launch(func, args, kwargs, key, store=None)

Launch computation in a new thread.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* au.TracingMiddleware(service_name='au-tasks', trace_backend=None)

Bases: [`Middleware`](au.base.html.md#au.base.Middleware)

Middleware for distributed tracing (OpenTelemetry compatible).

Provides trace IDs and span information for task execution.

#### after_compute(key, result)

Complete the trace span (base.Middleware protocol).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### before_compute(func, args, kwargs, key)

Start a new trace span (base.Middleware protocol).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### on_error(key, error)

Mark span as failed (base.Middleware protocol).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* au.TrackingTestBackend(middleware=None)

Bases: [`SyncTestBackend`](au.testing.html.md#au.testing.SyncTestBackend)

Test backend that tracks all executions.

Extends SyncTestBackend with detailed tracking for testing/debugging.

#### launch(func, args, kwargs, key, store=None)

Execute and track function call.

* **Parameters:**
  * **func** ([`Callable`](https://docs.python.org/3/library/typing.html#typing.Callable)) – Function to execute
  * **args** ([`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple)) – Positional arguments
  * **kwargs** ([`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)) – Keyword arguments
  * **key** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Result key
  * **store** ([`ComputationStore`](au.base.html.md#au.base.ComputationStore) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – Store for results (falls back to the backend’s own store)
* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* au.WorkflowBuilder(backend=None, store=None)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Fluent interface for building workflows.

### Example

```pycon
>>> workflow = (WorkflowBuilder()
>>>     .add_task('step1', process_data, data)
>>>     .add_task('step2', transform, depends_on=['step1'])
>>>     .add_task('step3', aggregate, depends_on=['step2'])
>>>     .build())
>>> results = workflow.execute()
```

#### add_task(task_id, func, \*args, depends_on=None, \*\*kwargs)

Add a task to the workflow.

* **Parameters:**
  * **task_id** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Task ID
  * **func** ([`Callable`](https://docs.python.org/3/library/typing.html#typing.Callable)) – Function to execute
  * **\*args** – Positional arguments
  * **depends_on** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]]) – Optional dependencies
  * **\*\*kwargs** – Keyword arguments
* **Return type:**
  [`WorkflowBuilder`](au.workflow.html.md#au.workflow.WorkflowBuilder)
* **Returns:**
  Self for chaining

#### build()

Build and return the task graph.

* **Return type:**
  [`TaskGraph`](au.workflow.html.md#au.workflow.TaskGraph)
* **Returns:**
  TaskGraph instance

### *class* au.WorkflowTask(func, args=tuple(), kwargs=dict(), depends_on=list(), task_id=None, state=TaskState.PENDING, result=None, error=None)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

A task in a workflow with dependencies.

#### func

Function to execute

* **Type:**
  [`Callable`](https://docs.python.org/3/library/typing.html#typing.Callable)

#### args

Positional arguments

* **Type:**
  [`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple)

#### kwargs

Keyword arguments

* **Type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)

#### depends_on

List of task IDs this task depends on

* **Type:**
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]

#### task_id

Unique task ID

* **Type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]

#### state

Current state

* **Type:**
  [`TaskState`](au.workflow.html.md#au.workflow.TaskState)

#### result

Result value (when completed)

* **Type:**
  [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)

#### error

Error message (when failed)

* **Type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]

#### is_ready_to_run(completed_tasks)

Check if all dependencies are completed.

* **Parameters:**
  **completed_tasks** ([`set`](https://docs.python.org/3/builtins/stdtypes.html#set)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]) – Set of completed task IDs
* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)
* **Returns:**
  True if ready to run

### au.async_compute(backend=None, store=None, , base_path='/tmp/computations', ttl_seconds=3600, serialization=SerializationFormat.JSON, middleware=None)

Decorator to make functions asynchronous with status tracking.

* **Parameters:**
  * **backend** ([`ComputationBackend`](au.base.html.md#au.base.ComputationBackend) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – Backend to execute computations (default: ProcessBackend)
  * **store** ([`ComputationStore`](au.base.html.md#au.base.ComputationStore) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – Store for results (default: FileSystemStore)
  * **base_path** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Path for FileSystemStore if no store provided
  * **ttl_seconds** ([`int`](https://docs.python.org/3/builtins/functions.html#int) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – Time-to-live for results (default: 1 hour)
  * **serialization** ([`SerializationFormat`](au.base.html.md#au.base.SerializationFormat)) – Format for storing results
  * **middleware** ([`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`Middleware`](au.base.html.md#au.base.Middleware)] | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – List of middleware components
* **Return type:**
  [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)

The default backend runs each call with a copy of its middleware, in a
child process: a plain [`MetricsMiddleware`](#au.MetricsMiddleware) counts there, not in the
instance you hold. Use `SharedMetricsMiddleware` to count across
processes.

### Example

```pycon
>>> metrics = SharedMetricsMiddleware()
>>> @async_compute(
...     ttl_seconds=7200,
...     middleware=[LoggingMiddleware(), metrics]
... )
... def slow_computation(x: int) -> int:
...     return x * x
>>> handle = slow_computation(5)  # Returns immediately
>>> result = handle.get_result(timeout=10)
```

### au.async_task(func, \*args, backend=None, store=None, timeout=None, \*\*kwargs)

Context manager for async task execution.

The task is submitted on enter and result retrieved on exit.

* **Parameters:**
  * **func** ([`Callable`](https://docs.python.org/3/library/typing.html#typing.Callable)) – Function to execute
  * **\*args** – Positional arguments
  * **backend** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)]) – Optional backend
  * **store** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationStore`](au.base.html.md#au.base.ComputationStore)]) – Optional store
  * **timeout** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`float`](https://docs.python.org/3/builtins/functions.html#float)]) – Optional timeout for getting result
  * **\*\*kwargs** – Keyword arguments
* **Yields:**
  ComputationHandle for the task

### Example

```pycon
>>> with async_task(my_func, 5, multiplier=2) as handle:
>>>     # Do other work while task runs
>>>     print("Working...")
>>> # Result is ready here
>>> print(f"Result: {handle.result}")
```

### au.cancel_task(task_id, backend=None, store=None)

Cancel a running task.

* **Parameters:**
  * **task_id** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Task ID to cancel
  * **backend** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)]) – Optional backend (uses default if not provided)
  * **store** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationStore`](au.base.html.md#au.base.ComputationStore)]) – Optional store (uses default if not provided)
* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)
* **Returns:**
  True if cancellation was attempted, False otherwise

### Example

```pycon
>>> task_id = submit_task(long_running_func)
>>> cancel_task(task_id)
```

### au.create_app_from_decorator(title='AU Task API', description='Async task management API')

Create a FastAPI app that auto-discovers @async_compute decorated functions.

#### NOTE
This requires functions to be imported/registered before app creation.

* **Parameters:**
  * **title** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – API title
  * **description** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – API description
* **Return type:**
  `FastAPI`
* **Returns:**
  FastAPI application instance

### au.create_observability_middleware(logging_level='INFO', enable_metrics=True, enable_tracing=False, on_start=None, on_complete=None, on_error=None)

Create a complete observability middleware stack.

* **Parameters:**
  * **logging_level** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Logging level
  * **enable_metrics** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool)) – Enable metrics collection
  * **enable_tracing** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool)) – Enable distributed tracing
  * **on_start** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`TaskEventHandler`](au.hooks.html.md#au.hooks.TaskEventHandler)]) – Optional start hook
  * **on_complete** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`TaskEventHandler`](au.hooks.html.md#au.hooks.TaskEventHandler)]) – Optional completion hook
  * **on_error** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`TaskEventHandler`](au.hooks.html.md#au.hooks.TaskEventHandler)]) – Optional error hook
* **Return type:**
  [`Middleware`](au.base.html.md#au.base.Middleware)
* **Returns:**
  Composite middleware with all observability features

### au.create_test_backend(\*\*kwargs)

Create a test backend with optional configuration.

* **Parameters:**
  **\*\*kwargs** – Configuration options (currently accepts ‘middleware’)
* **Return type:**
  [`SyncTestBackend`](au.testing.html.md#au.testing.SyncTestBackend)
* **Returns:**
  SyncTestBackend instance

### au.create_test_store(\*\*kwargs)

Create a test store with optional configuration.

* **Parameters:**
  **\*\*kwargs** – Configuration options (accepts ‘ttl_seconds’)
* **Return type:**
  [`InMemoryStore`](au.testing.html.md#au.testing.InMemoryStore)
* **Returns:**
  InMemoryStore instance

### au.depends_on(\*dependency_funcs)

Decorator to specify task dependencies.

Usage:

```default
@async_compute
def step1(n): return n * 2

@async_compute
def step2(n): return n + 10

@async_compute
@depends_on(step1, step2)
def step3(result1, result2):
    return result1 + result2
```

#### NOTE
This is a simplified version. Full implementation would
require integration with the async_compute decorator.

* **Parameters:**
  **\*dependency_funcs** – Functions this task depends on
* **Returns:**
  Decorator function

### au.get_config(config_file=None, \*\*overrides)

Get configuration with cascade: env vars → config file → defaults → overrides.

* **Parameters:**
  * **config_file** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`Path`](https://docs.python.org/3/library/pathlib.html#pathlib.Path)]) – Optional path to configuration file
  * **\*\*overrides** – Explicit configuration overrides
* **Return type:**
  [`AUConfig`](au.config.html.md#au.config.AUConfig)
* **Returns:**
  AUConfig instance with resolved configuration

### Example

```pycon
>>> config = get_config()  # Uses environment and defaults
>>> config = get_config(backend='redis', redis_url='redis://localhost')
>>> config = get_config(config_file=Path('custom.toml'))
```

### au.get_global_config()

Get the global configuration instance.

If not set, creates one from environment and config files.

* **Return type:**
  [`AUConfig`](au.config.html.md#au.config.AUConfig)
* **Returns:**
  Global AUConfig instance

### au.get_handle(task_id, backend=None, store=None)

Get a handle for a task ID.

* **Parameters:**
  * **task_id** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Task ID
  * **backend** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)]) – Optional backend
  * **store** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationStore`](au.base.html.md#au.base.ComputationStore)]) – Optional store
* **Return type:**
  [`ComputationHandle`](au.base.html.md#au.base.ComputationHandle)
* **Returns:**
  ComputationHandle instance

### Example

```pycon
>>> task_id = submit_task(my_func, 5)
>>> handle = get_handle(task_id)
>>> result = handle.get_result(timeout=10)
```

### au.get_many(task_ids, timeout=None, store=None)

Get results for multiple tasks.

* **Parameters:**
  * **task_ids** ([`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]) – List of task IDs
  * **timeout** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`float`](https://docs.python.org/3/builtins/functions.html#float)]) – Optional timeout (applies to each task)
  * **store** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationStore`](au.base.html.md#au.base.ComputationStore)]) – Optional store
* **Return type:**
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]
* **Returns:**
  List of results

### Example

```pycon
>>> task_ids = submit_many(tasks)
>>> results = get_many(task_ids, timeout=10)
```

### au.get_result(task_id, timeout=None, store=None)

Get result for a task ID.

* **Parameters:**
  * **task_id** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Task ID returned from submit_task
  * **timeout** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`float`](https://docs.python.org/3/builtins/functions.html#float)]) – Optional timeout in seconds
  * **store** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationStore`](au.base.html.md#au.base.ComputationStore)]) – Optional store (uses default if not provided)
* **Return type:**
  [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)
* **Returns:**
  Task result
* **Raises:**
  * [**TimeoutError**](https://docs.python.org/3/builtins/exceptions.html#TimeoutError) – If timeout exceeded
  * [**Exception**](https://docs.python.org/3/builtins/exceptions.html#Exception) – If task failed

### Example

```pycon
>>> task_id = submit_task(my_func, 5)
>>> result = get_result(task_id, timeout=10)
```

### au.get_status(task_id, store=None)

Get status for a task ID.

* **Parameters:**
  * **task_id** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Task ID returned from submit_task
  * **store** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationStore`](au.base.html.md#au.base.ComputationStore)]) – Optional store (uses default if not provided)
* **Return type:**
  [`ComputationStatus`](au.base.html.md#au.base.ComputationStatus)
* **Returns:**
  Task status (PENDING, RUNNING, COMPLETED, FAILED)

### Example

```pycon
>>> task_id = submit_task(my_func, 5)
>>> status = get_status(task_id)
>>> if status == ComputationStatus.COMPLETED:
>>>     result = get_result(task_id)
```

### au.is_ready(task_id, store=None)

Check if task is ready (completed or failed).

* **Parameters:**
  * **task_id** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Task ID returned from submit_task
  * **store** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationStore`](au.base.html.md#au.base.ComputationStore)]) – Optional store (uses default if not provided)
* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)
* **Returns:**
  True if task is complete, False otherwise

### Example

```pycon
>>> task_id = submit_task(my_func, 5)
>>> while not is_ready(task_id):
>>>     time.sleep(0.1)
>>> result = get_result(task_id)
```

### au.mk_flask_interface(functions=None, backend=None, store=None)

Create a Flask application for task management.

* **Parameters:**
  * **functions** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`Callable`](https://docs.python.org/3/library/typing.html#typing.Callable)]]) – Optional list of functions to register
  * **backend** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)]) – Optional backend
  * **store** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationStore`](au.base.html.md#au.base.ComputationStore)]) – Optional store
* **Return type:**
  Flask
* **Returns:**
  Flask application instance
* **Raises:**
  [**ImportError**](https://docs.python.org/3/builtins/exceptions.html#ImportError) – If Flask is not installed

### au.mk_http_interface(functions=None, backend=None, store=None, title='AU Task API', description='Async task management API', version='0.1.0')

Create a FastAPI application for task management.

* **Parameters:**
  * **functions** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`Callable`](https://docs.python.org/3/library/typing.html#typing.Callable)]]) – Optional list of functions to register
  * **backend** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)]) – Optional backend for task execution
  * **store** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationStore`](au.base.html.md#au.base.ComputationStore)]) – Optional store for results
  * **title** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – API title
  * **description** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – API description
  * **version** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – API version
* **Return type:**
  `FastAPI`
* **Returns:**
  FastAPI application instance
* **Raises:**
  [**ImportError**](https://docs.python.org/3/builtins/exceptions.html#ImportError) – If FastAPI is not installed

### Example

```pycon
>>> @async_compute
>>> def my_func(n: int) -> int:
>>>     return n * 2
>>>
>>> app = mk_http_interface([my_func])
>>> # Run with: uvicorn main:app --reload
```

### au.mock_async(backend=None)

Context manager for mocking async execution.

Usage:

```default
with mock_async() as mock:
    @async_compute
    def my_func(n: int) -> int:
        return n * 2

    handle = my_func.async_run(n=5)
    assert mock.task_count == 1
    assert handle.get_result() == 10
```

* **Parameters:**
  **backend** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)]) – Optional custom backend (defaults to TrackingTestBackend)
* **Yields:**
  MockTaskTracker instance

### au.reset_global_config()

Reset the global configuration to None.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### au.retry_with_policy(func, args=(), kwargs=None, policy=None)

Execute a function with retry policy.

* **Parameters:**
  * **func** ([`Callable`](https://docs.python.org/3/library/typing.html#typing.Callable)) – Function to execute
  * **args** ([`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple)) – Positional arguments for function
  * **kwargs** ([`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)) – Keyword arguments for function
  * **policy** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`RetryPolicy`](au.retry.html.md#au.retry.RetryPolicy)]) – Retry policy (None means no retry)
* **Return type:**
  [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)
* **Returns:**
  Function return value
* **Raises:**
  **Last exception if all retries exhausted** – 

### au.set_default_backend(backend)

Set the default backend for simple API calls.

* **Parameters:**
  **backend** ([`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)) – Backend to use by default
* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### au.set_default_store(store)

Set the default store for simple API calls.

* **Parameters:**
  **store** ([`ComputationStore`](au.base.html.md#au.base.ComputationStore)) – Store to use by default
* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### au.set_global_config(config)

Set the global configuration instance.

* **Parameters:**
  **config** ([`AUConfig`](au.config.html.md#au.config.AUConfig)) – AUConfig instance to use globally
* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### au.submit_many(tasks, backend=None, store=None)

Submit multiple tasks at once.

* **Parameters:**
  * **tasks** ([`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple)[[`Callable`](https://docs.python.org/3/library/typing.html#typing.Callable), [`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple), [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)]]) – List of (func, args, kwargs) tuples
  * **backend** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)]) – Optional backend
  * **store** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationStore`](au.base.html.md#au.base.ComputationStore)]) – Optional store
* **Return type:**
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]
* **Returns:**
  List of task IDs

### Example

```pycon
>>> tasks = [
>>>     (func1, (1,), {}),
>>>     (func2, (2,), {'multiplier': 3}),
>>> ]
>>> task_ids = submit_many(tasks)
```

### au.submit_task(func, \*args, key=None, backend=None, store=None, retry_policy=None, \*\*kwargs)

Submit a task for async execution without decorator.

* **Parameters:**
  * **func** ([`Callable`](https://docs.python.org/3/library/typing.html#typing.Callable)) – Function to execute
  * **\*args** – Positional arguments for function
  * **key** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str)]) – Optional caller-supplied idempotency key. When omitted, a fresh key
    is minted per call. When supplied and a live (in-flight) computation
    already exists for that key, the existing task is returned instead of
    launching a duplicate — so a resubmit runs the work at most once while
    it is in flight. A terminal (completed/failed/cancelled) or expired
    key re-runs.
  * **backend** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)]) – Optional backend (uses default if not provided)
  * **store** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`ComputationStore`](au.base.html.md#au.base.ComputationStore)]) – Optional store (uses default if not provided)
  * **retry_policy** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`RetryPolicy`](au.retry.html.md#au.retry.RetryPolicy)]) – Optional retry policy
  * **\*\*kwargs** – Keyword arguments for function
* **Return type:**
  [`str`](https://docs.python.org/3/builtins/stdtypes.html#str)
* **Returns:**
  Task ID (key) for retrieving results

### Example

```pycon
>>> task_id = submit_task(my_func, 5, multiplier=2)
>>> result = get_result(task_id, timeout=10)
```

```pycon
>>> # Idempotent submission: resubmitting the same key while in flight
>>> # returns the running task rather than launching a duplicate.
>>> task_id = submit_task(my_func, 5, key="my-job")
>>> same_id = submit_task(my_func, 5, key="my-job")
```

### au.temporary_async_compute(\*\*kwargs)

Context manager for temporary async computations with automatic cleanup.

### Example

```pycon
>>> with temporary_async_compute(ttl_seconds=60) as async_func:
...     @async_func
...     def compute(x):
...         return x * 2
...     handle = compute(5)
...     result = handle.get_result(timeout=5)
```

### Modules

| [`api`](au.api.html.md#module-au.api)           | Simplified API for AU - direct task submission without decorators.          |
|------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| [`backends`](au.backends.html.md#module-au.backends) | \_\_init_\_.py for au.backends package                                      |
| [`base`](au.base.html.md#module-au.base)         | Asynchronous computation framework with pluggable backends.                 |
| [`config`](au.config.html.md#module-au.config)     | Configuration management for AU with convention-over-configuration support. |
| [`hooks`](au.hooks.html.md#module-au.hooks)       | Enhanced hooks and observability for AU.                                    |
| [`http`](au.http.html.md#module-au.http)         | HTTP interface for AU task management.                                      |
| [`retry`](au.retry.html.md#module-au.retry)       | Retry policies and error handling for AU.                                   |
| [`testing`](au.testing.html.md#module-au.testing)   | Testing utilities for AU.                                                   |
| [`workflow`](au.workflow.html.md#module-au.workflow) | Workflow and task dependency management for AU.                             |
