# au.workflow

Workflow and task dependency management for AU.

Provides DAG-based task orchestration with dependency tracking.

### Functions

| [`depends_on`](#au.workflow.depends_on)(\*dependency_funcs)   | Decorator to specify task dependencies.   |
|-----------------------------------------------------------------------------------|-------------------------------------------|

### Classes

| [`TaskGraph`](#au.workflow.TaskGraph)([backend, store])             | Directed Acyclic Graph (DAG) for task execution.   |
|------------------------------------------------------------------------------------------|----------------------------------------------------|
| [`TaskState`](#au.workflow.TaskState)(\*values)                     | State of a task in a workflow.                     |
| [`WorkflowBuilder`](#au.workflow.WorkflowBuilder)([backend, store])       | Fluent interface for building workflows.           |
| [`WorkflowTask`](#au.workflow.WorkflowTask)(func[, args, kwargs, ...]) | A task in a workflow with dependencies.            |

### *class* au.workflow.TaskGraph(backend=None, store=None)

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
  [`WorkflowTask`](#au.workflow.WorkflowTask)
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

### *class* au.workflow.TaskState(\*values)

Bases: [`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

State of a task in a workflow.

### *class* au.workflow.WorkflowBuilder(backend=None, store=None)

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
  [`WorkflowBuilder`](#au.workflow.WorkflowBuilder)
* **Returns:**
  Self for chaining

#### build()

Build and return the task graph.

* **Return type:**
  [`TaskGraph`](#au.workflow.TaskGraph)
* **Returns:**
  TaskGraph instance

### *class* au.workflow.WorkflowTask(func, args=tuple(), kwargs=dict(), depends_on=list(), task_id=None, state=TaskState.PENDING, result=None, error=None)

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
  [`TaskState`](#au.workflow.TaskState)

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

### au.workflow.depends_on(\*dependency_funcs)

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
