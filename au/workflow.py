"""
Workflow and task dependency management for AU.

Provides DAG-based task orchestration with dependency tracking.
"""

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import time

from au.base import ComputationHandle, ComputationStore, ComputationBackend
from au.api import submit_task, get_result, _get_default_backend, _get_default_store


class TaskState(str, Enum):
    """State of a task in a workflow."""

    PENDING = "pending"
    WAITING = "waiting"  # Waiting for dependencies
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class WorkflowTask:
    """A task in a workflow with dependencies.

    Attributes:
        func: Function to execute
        args: Positional arguments
        kwargs: Keyword arguments
        depends_on: List of task IDs this task depends on
        task_id: Unique task ID
        state: Current state
        result: Result value (when completed)
        error: Error message (when failed)
    """

    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    depends_on: list[str] = field(default_factory=list)
    task_id: Optional[str] = None
    state: TaskState = TaskState.PENDING
    result: Any = None
    error: Optional[str] = None

    def is_ready_to_run(self, completed_tasks: set[str]) -> bool:
        """Check if all dependencies are completed.

        Args:
            completed_tasks: Set of completed task IDs

        Returns:
            True if ready to run
        """
        if self.state not in (TaskState.PENDING, TaskState.WAITING):
            return False

        return all(dep_id in completed_tasks for dep_id in self.depends_on)


class TaskGraph:
    """Directed Acyclic Graph (DAG) for task execution.

    Manages task dependencies and orchestrates execution.
    """

    def __init__(
        self,
        backend: Optional[ComputationBackend] = None,
        store: Optional[ComputationStore] = None,
    ):
        """Initialize task graph.

        Args:
            backend: Optional backend for task execution
            store: Optional store for results
        """
        self.backend = backend if backend is not None else _get_default_backend()
        self.store = store if store is not None else _get_default_store()
        self.tasks: dict[str, WorkflowTask] = {}
        self._task_counter = 0

    def add_task(
        self,
        func: Callable,
        *args,
        depends_on: Optional[list[str]] = None,
        task_id: Optional[str] = None,
        **kwargs
    ) -> str:
        """Add a task to the graph.

        Args:
            func: Function to execute
            *args: Positional arguments
            depends_on: Optional list of task IDs this depends on
            task_id: Optional custom task ID
            **kwargs: Keyword arguments

        Returns:
            Task ID

        Example:
            >>> graph = TaskGraph()
            >>> t1 = graph.add_task(step1, 5)
            >>> t2 = graph.add_task(step2, 10)
            >>> t3 = graph.add_task(step3, depends_on=[t1, t2])
        """
        if task_id is None:
            self._task_counter += 1
            task_id = f"task_{self._task_counter}"

        if task_id in self.tasks:
            raise ValueError(f"Task ID {task_id} already exists")

        task = WorkflowTask(
            func=func,
            args=args,
            kwargs=kwargs,
            depends_on=depends_on or [],
        )

        self.tasks[task_id] = task
        return task_id

    def _check_for_cycles(self) -> None:
        """Check for circular dependencies.

        Raises:
            ValueError: If circular dependency detected
        """
        visited = set()
        rec_stack = set()

        def has_cycle(task_id: str) -> bool:
            visited.add(task_id)
            rec_stack.add(task_id)

            task = self.tasks[task_id]
            for dep_id in task.depends_on:
                if dep_id not in visited:
                    if has_cycle(dep_id):
                        return True
                elif dep_id in rec_stack:
                    return True

            rec_stack.remove(task_id)
            return False

        for task_id in self.tasks:
            if task_id not in visited:
                if has_cycle(task_id):
                    raise ValueError("Circular dependency detected in task graph")

    def execute(self, timeout: Optional[float] = None) -> dict[str, Any]:
        """Execute all tasks in dependency order.

        Args:
            timeout: Optional overall timeout in seconds

        Returns:
            Dictionary mapping task IDs to results

        Raises:
            ValueError: If circular dependencies detected
            TimeoutError: If overall timeout exceeded

        Example:
            >>> graph = TaskGraph()
            >>> t1 = graph.add_task(step1, 5)
            >>> t2 = graph.add_task(step2, depends_on=[t1])
            >>> results = graph.execute()
            >>> print(results[t2])
        """
        # Check for cycles
        self._check_for_cycles()

        start_time = time.time()
        completed_tasks: set[str] = set()
        running_tasks: dict[str, str] = {}  # workflow_task_id -> execution_task_id
        results: dict[str, Any] = {}

        while len(completed_tasks) < len(self.tasks):
            # Check timeout
            if timeout and (time.time() - start_time) > timeout:
                raise TimeoutError("Workflow execution timed out")

            # Find tasks ready to run
            for task_id, task in self.tasks.items():
                if task.state == TaskState.PENDING and task.is_ready_to_run(completed_tasks):
                    # Submit task
                    execution_id = submit_task(
                        task.func,
                        *task.args,
                        backend=self.backend,
                        store=self.store,
                        **task.kwargs
                    )
                    running_tasks[task_id] = execution_id
                    task.state = TaskState.RUNNING

            # Check running tasks
            for task_id in list(running_tasks.keys()):
                execution_id = running_tasks[task_id]
                task = self.tasks[task_id]

                try:
                    # Try to get result (with short timeout to avoid blocking)
                    result = get_result(execution_id, timeout=0.1, store=self.store)

                    # Task completed
                    task.state = TaskState.COMPLETED
                    task.result = result
                    results[task_id] = result
                    completed_tasks.add(task_id)
                    del running_tasks[task_id]

                except TimeoutError:
                    # Still running
                    continue

                except Exception as e:
                    # Task failed
                    task.state = TaskState.FAILED
                    task.error = str(e)
                    del running_tasks[task_id]

                    # Could choose to continue or abort workflow
                    # For now, we'll mark as failed but continue
                    completed_tasks.add(task_id)

            # Small sleep to avoid busy waiting
            if running_tasks:
                time.sleep(0.05)

        return results

    def get_task(self, task_id: str) -> WorkflowTask:
        """Get a task by ID.

        Args:
            task_id: Task ID

        Returns:
            WorkflowTask instance

        Raises:
            KeyError: If task ID not found
        """
        return self.tasks[task_id]

    def get_task_result(self, task_id: str) -> Any:
        """Get result for a task.

        Args:
            task_id: Task ID

        Returns:
            Task result

        Raises:
            ValueError: If task not completed or failed
        """
        task = self.tasks[task_id]

        if task.state == TaskState.COMPLETED:
            return task.result
        elif task.state == TaskState.FAILED:
            raise RuntimeError(f"Task {task_id} failed: {task.error}")
        else:
            raise ValueError(f"Task {task_id} not yet completed (state: {task.state})")


def depends_on(*dependency_funcs):
    """Decorator to specify task dependencies.

    Usage:
        @async_compute
        def step1(n): return n * 2

        @async_compute
        def step2(n): return n + 10

        @async_compute
        @depends_on(step1, step2)
        def step3(result1, result2):
            return result1 + result2

    Note: This is a simplified version. Full implementation would
    require integration with the async_compute decorator.

    Args:
        *dependency_funcs: Functions this task depends on

    Returns:
        Decorator function
    """
    def decorator(func: Callable) -> Callable:
        # Store dependencies as function attribute
        func._au_dependencies = dependency_funcs
        return func

    return decorator


class WorkflowBuilder:
    """Fluent interface for building workflows.

    Example:
        >>> workflow = (WorkflowBuilder()
        >>>     .add_task('step1', process_data, data)
        >>>     .add_task('step2', transform, depends_on=['step1'])
        >>>     .add_task('step3', aggregate, depends_on=['step2'])
        >>>     .build())
        >>> results = workflow.execute()
    """

    def __init__(
        self,
        backend: Optional[ComputationBackend] = None,
        store: Optional[ComputationStore] = None,
    ):
        """Initialize workflow builder.

        Args:
            backend: Optional backend
            store: Optional store
        """
        self.graph = TaskGraph(backend=backend, store=store)

    def add_task(
        self,
        task_id: str,
        func: Callable,
        *args,
        depends_on: Optional[list[str]] = None,
        **kwargs
    ) -> 'WorkflowBuilder':
        """Add a task to the workflow.

        Args:
            task_id: Task ID
            func: Function to execute
            *args: Positional arguments
            depends_on: Optional dependencies
            **kwargs: Keyword arguments

        Returns:
            Self for chaining
        """
        self.graph.add_task(
            func,
            *args,
            depends_on=depends_on,
            task_id=task_id,
            **kwargs
        )
        return self

    def build(self) -> TaskGraph:
        """Build and return the task graph.

        Returns:
            TaskGraph instance
        """
        return self.graph
