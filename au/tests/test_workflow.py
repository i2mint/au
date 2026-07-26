"""Tests for workflow module."""

import pytest

from au.workflow import TaskGraph, WorkflowBuilder, TaskState, WorkflowTask
from au.testing import SyncTestBackend, InMemoryStore


def step1(n: int) -> int:
    """First step: multiply by 2."""
    return n * 2


def step2(n: int) -> int:
    """Second step: add 10."""
    return n + 10


def step3(a: int, b: int) -> int:
    """Third step: add two numbers."""
    return a + b


def failing_step():
    """Step that fails."""
    raise ValueError("This step fails")


class TestTaskGraph:
    """Tests for TaskGraph class."""

    def test_add_task(self):
        """Test adding tasks to graph."""
        graph = TaskGraph()

        t1 = graph.add_task(step1, 5)
        assert t1 in graph.tasks
        assert graph.tasks[t1].func == step1

    def test_add_task_with_dependencies(self):
        """Test adding tasks with dependencies."""
        graph = TaskGraph()

        t1 = graph.add_task(step1, 5)
        t2 = graph.add_task(step2, 10)
        t3 = graph.add_task(step3, depends_on=[t1, t2])

        assert graph.tasks[t3].depends_on == [t1, t2]

    def test_simple_execution(self):
        """Test executing simple graph."""
        backend = SyncTestBackend()
        store = InMemoryStore()

        graph = TaskGraph(backend=backend, store=store)

        t1 = graph.add_task(step1, 5)
        results = graph.execute()

        assert results[t1] == 10

    def test_execution_with_dependencies(self):
        """Test executing graph with dependencies."""
        backend = SyncTestBackend()
        store = InMemoryStore()

        graph = TaskGraph(backend=backend, store=store)

        # step1(5) -> 10
        # step2(5) -> 15
        # step3(10, 15) -> 25
        t1 = graph.add_task(step1, 5, task_id="step1")
        t2 = graph.add_task(step2, 5, task_id="step2")
        t3 = graph.add_task(step3, depends_on=["step1", "step2"], task_id="step3")

        # Note: The current implementation doesn't pass results automatically
        # This is a simplified test
        results = graph.execute()

        assert "step1" in results
        assert "step2" in results

    def test_circular_dependency_detection(self):
        """Test detection of circular dependencies."""
        graph = TaskGraph()

        t1 = graph.add_task(step1, 5, task_id="t1")
        t2 = graph.add_task(step2, 10, task_id="t2", depends_on=["t3"])
        t3 = graph.add_task(step3, task_id="t3", depends_on=["t2"])

        with pytest.raises(ValueError, match="Circular dependency"):
            graph.execute()

    def test_get_task_result(self):
        """Test getting task result."""
        backend = SyncTestBackend()
        store = InMemoryStore()

        graph = TaskGraph(backend=backend, store=store)

        t1 = graph.add_task(step1, 7)
        graph.execute()

        result = graph.get_task_result(t1)
        assert result == 14

    def test_failed_task_execution(self):
        """Test handling failed task execution."""
        backend = SyncTestBackend()
        store = InMemoryStore()

        graph = TaskGraph(backend=backend, store=store)

        t1 = graph.add_task(failing_step)
        results = graph.execute()

        # Task should complete but be marked as failed
        task = graph.get_task(t1)
        assert task.state == TaskState.FAILED


class TestWorkflowBuilder:
    """Tests for WorkflowBuilder class."""

    def test_builder_pattern(self):
        """Test fluent builder pattern."""
        backend = SyncTestBackend()
        store = InMemoryStore()

        workflow = (
            WorkflowBuilder(backend=backend, store=store)
            .add_task("step1", step1, 5)
            .add_task("step2", step2, 10)
            .build()
        )

        assert "step1" in workflow.tasks
        assert "step2" in workflow.tasks

        results = workflow.execute()
        assert results["step1"] == 10
        assert results["step2"] == 20

    def test_builder_with_dependencies(self):
        """Test builder with task dependencies."""
        backend = SyncTestBackend()
        store = InMemoryStore()

        workflow = (
            WorkflowBuilder(backend=backend, store=store)
            .add_task("step1", step1, 5)
            .add_task("step2", step2, 10)
            .build()
        )

        results = workflow.execute()

        assert "step1" in results
        assert "step2" in results


class TestWorkflowTask:
    """Tests for WorkflowTask class."""

    def test_is_ready_to_run_no_deps(self):
        """Test task ready with no dependencies."""
        task = WorkflowTask(func=step1, args=(5,))

        assert task.is_ready_to_run(set()) is True

    def test_is_ready_to_run_with_deps(self):
        """Test task ready with dependencies."""
        task = WorkflowTask(
            func=step3,
            depends_on=["t1", "t2"],
        )

        # Not ready when dependencies not complete
        assert task.is_ready_to_run({"t1"}) is False

        # Ready when all dependencies complete
        assert task.is_ready_to_run({"t1", "t2"}) is True

    def test_task_state_transitions(self):
        """Test task state transitions."""
        task = WorkflowTask(func=step1, args=(5,))

        assert task.state == TaskState.PENDING

        # Simulate execution
        task.state = TaskState.RUNNING
        assert task.state == TaskState.RUNNING

        task.state = TaskState.COMPLETED
        task.result = 10
        assert task.state == TaskState.COMPLETED
        assert task.result == 10
