# au.api

Simplified API for AU - direct task submission without decorators.

Provides simple functions for submitting and retrieving async tasks
without requiring decorator patterns.

### Module Attributes

| [`TASKS_LOGGER`](#au.api.TASKS_LOGGER)   | like any library log, it shows only once the app configures logging (au's own warnings and errors, on `au.base`, still reach stderr by default).   |
|-----------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|

### Functions

| [`async_task`](#au.api.async_task)(func, \*args[, backend, store, ...])   | Context manager for async task execution.            |
|----------------------------------------------------------------------------------------------------|------------------------------------------------------|
| [`cancel_task`](#au.api.cancel_task)(task_id[, backend, store])            | Cancel a running task.                               |
| [`get_handle`](#au.api.get_handle)(task_id[, backend, store])             | Get a handle for a task ID.                          |
| [`get_many`](#au.api.get_many)(task_ids[, timeout, store])              | Get results for multiple tasks.                      |
| [`get_result`](#au.api.get_result)(task_id[, timeout, store])             | Get result for a task ID.                            |
| [`get_status`](#au.api.get_status)(task_id[, store])                      | Get status for a task ID.                            |
| [`is_ready`](#au.api.is_ready)(task_id[, store])                        | Check if task is ready (completed or failed).        |
| [`set_default_backend`](#au.api.set_default_backend)(backend)                      | Set the default backend for simple API calls.        |
| [`set_default_store`](#au.api.set_default_store)(store)                          | Set the default store for simple API calls.          |
| [`submit_many`](#au.api.submit_many)(tasks[, backend, store])              | Submit multiple tasks at once.                       |
| [`submit_task`](#au.api.submit_task)(func, \*args[, key, backend, ...])    | Submit a task for async execution without decorator. |

### au.api.TASKS_LOGGER *= 'au.tasks'*

like
any library log, it shows only once the app configures logging (au’s own
warnings and errors, on `au.base`, still reach stderr by default).

* **Type:**
  Where the simple API logs each task’s lifecycle. It has a NullHandler

### au.api.async_task(func, \*args, backend=None, store=None, timeout=None, \*\*kwargs)

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

### au.api.cancel_task(task_id, backend=None, store=None)

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

### au.api.get_handle(task_id, backend=None, store=None)

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

### au.api.get_many(task_ids, timeout=None, store=None)

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

### au.api.get_result(task_id, timeout=None, store=None)

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

### au.api.get_status(task_id, store=None)

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

### au.api.is_ready(task_id, store=None)

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

### au.api.set_default_backend(backend)

Set the default backend for simple API calls.

* **Parameters:**
  **backend** ([`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)) – Backend to use by default
* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### au.api.set_default_store(store)

Set the default store for simple API calls.

* **Parameters:**
  **store** ([`ComputationStore`](au.base.html.md#au.base.ComputationStore)) – Store to use by default
* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### au.api.submit_many(tasks, backend=None, store=None)

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

### au.api.submit_task(func, \*args, key=None, backend=None, store=None, retry_policy=None, \*\*kwargs)

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
