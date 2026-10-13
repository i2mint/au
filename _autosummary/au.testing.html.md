# au.testing

Testing utilities for AU.

Provides test backends, mocking utilities, and helpers for testing async code.

### Functions

| [`create_test_backend`](#au.testing.create_test_backend)(\*\*kwargs)   | Create a test backend with optional configuration.   |
|------------------------------------------------------------------------------------|------------------------------------------------------|
| [`create_test_store`](#au.testing.create_test_store)(\*\*kwargs)     | Create a test store with optional configuration.     |
| [`mock_async`](#au.testing.mock_async)([backend])             | Context manager for mocking async execution.         |

### Classes

| [`InMemoryStore`](#au.testing.InMemoryStore)([ttl_seconds])                      | In-memory store for testing.                        |
|----------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| [`MockTaskTracker`](#au.testing.MockTaskTracker)([task_count, tasks_by_name, ...]) | Tracks task executions for testing.                 |
| [`SyncTestBackend`](#au.testing.SyncTestBackend)([middleware])                     | Synchronous test backend that executes immediately. |
| [`TaskCallRecord`](#au.testing.TaskCallRecord)(func_name, args, kwargs[, ...])    | Record of a task call for testing/mocking.          |
| [`TrackingTestBackend`](#au.testing.TrackingTestBackend)([middleware])                 | Test backend that tracks all executions.            |

### *class* au.testing.InMemoryStore(ttl_seconds=3600)

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

### *class* au.testing.MockTaskTracker(task_count=0, tasks_by_name=<factory>, all_tasks=<factory>)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Tracks task executions for testing.

#### task_count

Total number of tasks executed

#### tasks_by_name

Dictionary mapping function names to call records

#### all_tasks

List of all task call records

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
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`TaskCallRecord`](#au.testing.TaskCallRecord)]
* **Returns:**
  List of call records

#### last_call(func_name)

Get last call for a specific function.

* **Parameters:**
  **func_name** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Name of the function
* **Return type:**
  [`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`TaskCallRecord`](#au.testing.TaskCallRecord)]
* **Returns:**
  Last call record or None

#### record_call(func_name, args, kwargs)

Record a task call.

* **Parameters:**
  * **func_name** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Name of the function
  * **args** ([`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple)) – Positional arguments
  * **kwargs** ([`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)) – Keyword arguments

### *class* au.testing.SyncTestBackend(middleware=None)

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

### *class* au.testing.TaskCallRecord(func_name, args, kwargs, timestamp=<factory>)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Record of a task call for testing/mocking.

### *class* au.testing.TrackingTestBackend(middleware=None)

Bases: [`SyncTestBackend`](#au.testing.SyncTestBackend)

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

### au.testing.create_test_backend(\*\*kwargs)

Create a test backend with optional configuration.

* **Parameters:**
  **\*\*kwargs** – Configuration options (currently accepts ‘middleware’)
* **Return type:**
  [`SyncTestBackend`](#au.testing.SyncTestBackend)
* **Returns:**
  SyncTestBackend instance

### au.testing.create_test_store(\*\*kwargs)

Create a test store with optional configuration.

* **Parameters:**
  **\*\*kwargs** – Configuration options (accepts ‘ttl_seconds’)
* **Return type:**
  [`InMemoryStore`](#au.testing.InMemoryStore)
* **Returns:**
  InMemoryStore instance

### au.testing.mock_async(backend=None)

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
