# au.hooks

Enhanced hooks and observability for AU.

Provides lifecycle hooks, event callbacks, and improved middleware.

### Functions

| [`create_observability_middleware`](#au.hooks.create_observability_middleware)([...])   | Create a complete observability middleware stack.   |
|-------------------------------------------------------------------------------------------|-----------------------------------------------------|

### Classes

| [`CompositeMiddleware`](#au.hooks.CompositeMiddleware)(middlewares)                  | Combines multiple middleware into one.                         |
|----------------------------------------------------------------------------------------------------|----------------------------------------------------------------|
| [`HooksMiddleware`](#au.hooks.HooksMiddleware)([on_start, on_complete, ...])     | Middleware that provides lifecycle hooks.                      |
| [`MetricsCollectorMiddleware`](#au.hooks.MetricsCollectorMiddleware)([metrics_backend])     | Enhanced metrics middleware with histogram support.            |
| [`TaskEvent`](#au.hooks.TaskEvent)(task_id, event_type[, timestamp, data]) | Event fired during task lifecycle.                             |
| [`TaskEventHandler`](#au.hooks.TaskEventHandler)(\*args, \*\*kwargs)              | Protocol for task event handlers.                              |
| [`TracingMiddleware`](#au.hooks.TracingMiddleware)([service_name, trace_backend])  | Middleware for distributed tracing (OpenTelemetry compatible). |

### *class* au.hooks.CompositeMiddleware(middlewares)

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

### *class* au.hooks.HooksMiddleware(on_start=None, on_complete=None, on_error=None, on_retry=None)

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
  [`list`](https://docs.python.org/3/builtins/stdtypes.html#list)[[`TaskEvent`](#au.hooks.TaskEvent)]
* **Returns:**
  List of task events

#### on_error(key, error)

Called when computation fails (base.Middleware protocol).

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### *class* au.hooks.MetricsCollectorMiddleware(metrics_backend=None)

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

### *class* au.hooks.TaskEvent(task_id, event_type, timestamp=<factory>, data=<factory>)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Event fired during task lifecycle.

#### task_id

Task identifier

#### event_type

Type of event (start, complete, error, retry)

#### timestamp

When event occurred

#### data

Event-specific data

### *class* au.hooks.TaskEventHandler(\*args, \*\*kwargs)

Bases: [`Protocol`](https://docs.python.org/3/library/typing.html#typing.Protocol)

Protocol for task event handlers.

### *class* au.hooks.TracingMiddleware(service_name='au-tasks', trace_backend=None)

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

### au.hooks.create_observability_middleware(logging_level='INFO', enable_metrics=True, enable_tracing=False, on_start=None, on_complete=None, on_error=None)

Create a complete observability middleware stack.

* **Parameters:**
  * **logging_level** ([`str`](https://docs.python.org/3/builtins/stdtypes.html#str)) – Logging level
  * **enable_metrics** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool)) – Enable metrics collection
  * **enable_tracing** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool)) – Enable distributed tracing
  * **on_start** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`TaskEventHandler`](#au.hooks.TaskEventHandler)]) – Optional start hook
  * **on_complete** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`TaskEventHandler`](#au.hooks.TaskEventHandler)]) – Optional completion hook
  * **on_error** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`TaskEventHandler`](#au.hooks.TaskEventHandler)]) – Optional error hook
* **Return type:**
  [`Middleware`](au.base.html.md#au.base.Middleware)
* **Returns:**
  Composite middleware with all observability features
