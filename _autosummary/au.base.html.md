# au.base

Asynchronous computation framework with pluggable backends.

This module provides a decorator-based approach to transform synchronous functions
into asynchronous ones that return immediately with a handle to check status
and retrieve results later.

## Architecture Overview:

1. Storage abstraction (ComputationStore): Implements MutableMapping interface
2. Execution abstraction (ComputationBackend): Handles computation launching
3. Result handling (ComputationHandle): Clean API for status/result retrieval
4. Middleware system: For cross-cutting concerns like logging and metrics
5. Cleanup mechanisms: Automatic expiration of old results

## Usage Patterns:

Simple file-based async:

```pycon
>>> @async_compute()
... def my_function(x):
...     return x * 2
```

Custom configuration (example setup):

```pycon
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
```

Shared infrastructure:

```pycon
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
```

### Functions

| [`async_compute`](#au.base.async_compute)([backend, store, base_path, ...])   | Decorator to make functions asynchronous with status tracking.           |
|----------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------|
| [`temporary_async_compute`](#au.base.temporary_async_compute)(\*\*kwargs)               | Context manager for temporary async computations with automatic cleanup. |

### Classes

| [`ComputationBackend`](#au.base.ComputationBackend)([middleware])               | Abstract base for computation execution backends.                                     |
|-------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| [`ComputationHandle`](#au.base.ComputationHandle)(key, store[, backend])       | Handle to check status and retrieve results of async computation.                     |
| [`ComputationResult`](#au.base.ComputationResult)(value, status[, error, ...]) | Result of a computation with metadata.                                                |
| [`ComputationStatus`](#au.base.ComputationStatus)(\*values)                    | Status of a computation task.                                                         |
| [`ComputationStore`](#au.base.ComputationStore)(\*[, ttl_seconds])            | Abstract base for storing computation results with TTL support.                       |
| [`FileSystemStore`](#au.base.FileSystemStore)(base_path, \*[, suffix, ...])  | Store computation results in the filesystem with automatic cleanup.                   |
| [`LoggingMiddleware`](#au.base.LoggingMiddleware)(\*[, level, logger_name])    | Middleware for logging computation lifecycle.                                         |
| [`MetricsMiddleware`](#au.base.MetricsMiddleware)()                            | Middleware for collecting computation metrics, in the copy that runs the computation. |
| [`Middleware`](#au.base.Middleware)()                                   | Base class for middleware components.                                                 |
| [`ProcessBackend`](#au.base.ProcessBackend)([store, middleware])            | Execute computations in separate processes with middleware support.                   |
| [`RemoteAPIBackend`](#au.base.RemoteAPIBackend)(store, \*, api_url[, ...])    | Example backend for remote computation services.                                      |
| [`SerializationFormat`](#au.base.SerializationFormat)(\*values)                  | Supported serialization formats.                                                      |
| [`SharedMetricsMiddleware`](#au.base.SharedMetricsMiddleware)([...])                 | Metrics middleware using shared memory for multiprocessing.                           |
| [`StdLibQueueBackend`](#au.base.StdLibQueueBackend)([store, max_workers, ...])  | Execute computations using Python's standard library concurrent.futures.              |
| [`ThreadBackend`](#au.base.ThreadBackend)([store, middleware])             | Execute computations in separate threads (good for I/O-bound tasks).                  |

### Exceptions

| [`ComputationCancelledError`](#au.base.ComputationCancelledError)   | Raised by [`ComputationHandle.get_result()`](#au.base.ComputationHandle.get_result) when a computation was cancelled.   |
|------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|

### *class* au.base.ComputationBackend(middleware=None)

Bases: [`ABC`](https://docs.python.org/3/library/abc.html#abc.ABC)

Abstract base for computation execution backends.

#### *abstractmethod* launch(func, args, kwargs, key, store=None)

Launch the computation asynchronously.

The backend should execute `func(*args, **kwargs)` and store the result
under `key`. The result store is resolved by `_resolve_store()`:
`store` (passed per-launch by the simple `au.api` surface) takes
precedence, falling back to the backend’s own `self.store` (set at
construction by the `async_compute` decorator path).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### terminate(key)

Attempt to terminate the computation. Default: do nothing.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *exception* au.base.ComputationCancelledError

Bases: [`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)

Raised by [`ComputationHandle.get_result()`](#au.base.ComputationHandle.get_result) when a computation was cancelled.

This lets callers distinguish a deliberate user cancellation (a first-class
`ComputationStatus.CANCELLED`) from a genuine computation failure
(`ComputationStatus.FAILED`) without string-sniffing the error message.

### *class* au.base.ComputationHandle(key, store, backend=None)

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
a genuine failure via [`get_status()`](#au.base.ComputationHandle.get_status) without inspecting error text.

* **Parameters:**
  **reason** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Human-readable reason stored on the result’s
  `terminal_reason` (surfaced by
  [`ComputationCancelledError`](#au.base.ComputationCancelledError) from [`get_result()`](#au.base.ComputationHandle.get_result)).
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
  [`ComputationStatus`](#au.base.ComputationStatus)

#### is_ready()

Check if the computation is complete.

* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)

#### *property* metadata *: [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [Any](https://docs.python.org/3/library/typing.html#typing.Any)]*

Get computation metadata.

### *class* au.base.ComputationResult(value, status, error=None, created_at=datetime.now(), completed_at=None, metadata=dict(), terminal_reason=None)

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

### *class* au.base.ComputationStatus(\*values)

Bases: [`Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

Status of a computation task.

### *class* au.base.ComputationStore(, ttl_seconds=None)

Bases: [`MutableMapping`](https://docs.python.org/3/library/collections.abc.html#collections.abc.MutableMapping)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`ComputationResult`](#au.base.ComputationResult)], [`ABC`](https://docs.python.org/3/library/abc.html#abc.ABC)

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

### *class* au.base.FileSystemStore(base_path, , suffix='.json', ttl_seconds=None, serialization=SerializationFormat.JSON, auto_cleanup=True, cleanup_probability=0.1)

Bases: [`ComputationStore`](#au.base.ComputationStore)

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

### *class* au.base.LoggingMiddleware(, level=10, logger_name=None)

Bases: [`Middleware`](#au.base.Middleware)

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

### *class* au.base.MetricsMiddleware

Bases: [`Middleware`](#au.base.Middleware)

Middleware for collecting computation metrics, in the copy that runs the
computation.

Every backend but ThreadBackend runs a copy (a child process’s, or one a
queue worker unpickled), so there the instance you hold counts nothing. Use
[`SharedMetricsMiddleware`](#au.base.SharedMetricsMiddleware) with ProcessBackend to count across
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

### *class* au.base.Middleware

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

Called when computation fails.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* au.base.ProcessBackend(store=None, middleware=None)

Bases: [`ComputationBackend`](#au.base.ComputationBackend)

Execute computations in separate processes with middleware support.

#### launch(func, args, kwargs, key, store=None)

Launch computation in a new process.

The middleware goes to the child with the work and runs there only, so
each `before_compute` is released by an `after_compute` or
`on_error` in the same process (i2mint/au#14). Count across processes
with [`SharedMetricsMiddleware`](#au.base.SharedMetricsMiddleware); a plain one counts in the child.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### terminate(key)

Attempt to terminate the computation. Default: do nothing.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* au.base.RemoteAPIBackend(store, , api_url, api_key='', middleware=None)

Bases: [`ComputationBackend`](#au.base.ComputationBackend)

Example backend for remote computation services.

This integrates with external APIs that have their own
job tracking mechanisms.

#### launch(func, args, kwargs, key, store=None)

Launch computation via remote API.

In a real implementation, this would:

1. Serialize the function and arguments
2. Submit to the remote API
3. Poll for status updates
4. Store results when complete

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* au.base.SerializationFormat(\*values)

Bases: [`Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

Supported serialization formats.

### *class* au.base.SharedMetricsMiddleware(total_computations=None, completed_computations=None, failed_computations=None, total_duration=None, lock=None)

Bases: [`Middleware`](#au.base.Middleware)

Metrics middleware using shared memory for multiprocessing.

#### after_compute(key, result)

Called after computation completes.

#### before_compute(func, args, kwargs, key)

Called before computation starts.

#### on_error(key, error)

Called when computation fails.

### *class* au.base.StdLibQueueBackend(store=None, max_workers=5, use_processes=True, middleware=None)

Bases: [`ComputationBackend`](#au.base.ComputationBackend)

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

### *class* au.base.ThreadBackend(store=None, middleware=None)

Bases: [`ComputationBackend`](#au.base.ComputationBackend)

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

### au.base.async_compute(backend=None, store=None, , base_path='/tmp/computations', ttl_seconds=3600, serialization=SerializationFormat.JSON, middleware=None)

Decorator to make functions asynchronous with status tracking.

* **Parameters:**
  * **backend** ([`ComputationBackend`](#au.base.ComputationBackend) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – Backend to execute computations (default: ProcessBackend)
  * **store** ([`ComputationStore`](#au.base.ComputationStore) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – Store for results (default: FileSystemStore)
  * **base_path** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Path for FileSystemStore if no store provided
  * **ttl_seconds** ([`int`](https://docs.python.org/3/builtins/functions.html#int) | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – Time-to-live for results (default: 1 hour)
  * **serialization** ([`SerializationFormat`](#au.base.SerializationFormat)) – Format for storing results
  * **middleware** ([`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`Middleware`](#au.base.Middleware)] | [`None`](https://docs.python.org/3/builtins/constants.html#None)) – List of middleware components
* **Return type:**
  [`Callable`](https://docs.python.org/3/library/collections.abc.html#collections.abc.Callable)

The default backend runs each call with a copy of its middleware, in a
child process: a plain [`MetricsMiddleware`](#au.base.MetricsMiddleware) counts there, not in the
instance you hold. Use [`SharedMetricsMiddleware`](#au.base.SharedMetricsMiddleware) to count across
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

### au.base.temporary_async_compute(\*\*kwargs)

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
