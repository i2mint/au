# au.http

HTTP interface for AU task management.

Provides REST API endpoints for async task management using FastAPI.

### Functions

| `Body`(\*args, \*\*kwargs)                                                                       |                                                                              |
|--------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| `Field`(\*args, \*\*kwargs)                                                                      |                                                                              |
| `Query`(\*args, \*\*kwargs)                                                                      |                                                                              |
| [`create_app_from_decorator`](#au.http.create_app_from_decorator)([title, description]) | Create a FastAPI app that auto-discovers @async_compute decorated functions. |
| [`mk_flask_interface`](#au.http.mk_flask_interface)([functions, backend, store]) | Create a Flask application for task management.                              |
| [`mk_http_interface`](#au.http.mk_http_interface)([functions, backend, ...])    | Create a FastAPI application for task management.                            |

### Classes

| `BaseModel`()                                                         |                                       |
|-----------------------------------------------------------------------|---------------------------------------|
| `FastAPI`()                                                           |                                       |
| [`TaskCancelResponse`](#au.http.TaskCancelResponse)() | Response model for task cancellation. |
| [`TaskListResponse`](#au.http.TaskListResponse)()   | Response model for task list.         |
| [`TaskResultResponse`](#au.http.TaskResultResponse)() | Response model for task result.       |
| [`TaskStatusResponse`](#au.http.TaskStatusResponse)() | Response model for task status.       |
| [`TaskSubmitRequest`](#au.http.TaskSubmitRequest)()  | Request model for task submission.    |
| [`TaskSubmitResponse`](#au.http.TaskSubmitResponse)() | Response model for task submission.   |

### Exceptions

| [`HTTPException`](#au.http.HTTPException)   |    |
|------------------------------------------------------------------|----|

### *exception* au.http.HTTPException

Bases: [`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)

### *class* au.http.TaskCancelResponse

Bases: `BaseModel`

Response model for task cancellation.

### *class* au.http.TaskListResponse

Bases: `BaseModel`

Response model for task list.

### *class* au.http.TaskResultResponse

Bases: `BaseModel`

Response model for task result.

### *class* au.http.TaskStatusResponse

Bases: `BaseModel`

Response model for task status.

### *class* au.http.TaskSubmitRequest

Bases: `BaseModel`

Request model for task submission.

### *class* au.http.TaskSubmitResponse

Bases: `BaseModel`

Response model for task submission.

### au.http.create_app_from_decorator(title='AU Task API', description='Async task management API')

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

### au.http.mk_flask_interface(functions=None, backend=None, store=None)

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

### au.http.mk_http_interface(functions=None, backend=None, store=None, title='AU Task API', description='Async task management API', version='0.1.0')

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
