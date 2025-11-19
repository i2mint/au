"""
HTTP interface for AU task management.

Provides REST API endpoints for async task management using FastAPI.
"""

from typing import Any, Callable, Optional, Union
from datetime import datetime
from enum import Enum

try:
    from fastapi import FastAPI, HTTPException, Query, Body
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel, Field
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False
    # Create dummy classes for type hints
    class FastAPI: pass
    class HTTPException(Exception): pass
    class BaseModel: pass
    def Field(*args, **kwargs): pass
    def Query(*args, **kwargs): pass
    def Body(*args, **kwargs): pass

from au.base import ComputationStatus, ComputationHandle, ComputationBackend, ComputationStore
from au.api import (
    submit_task,
    get_result,
    get_status,
    is_ready,
    cancel_task,
    _get_default_backend,
    _get_default_store,
)


# Pydantic models for API


class TaskSubmitRequest(BaseModel):
    """Request model for task submission."""

    function_name: str = Field(..., description="Name of the function to execute")
    args: list[Any] = Field(default_factory=list, description="Positional arguments")
    kwargs: dict[str, Any] = Field(default_factory=dict, description="Keyword arguments")


class TaskSubmitResponse(BaseModel):
    """Response model for task submission."""

    task_id: str = Field(..., description="Unique task identifier")
    status: str = Field(default="pending", description="Initial task status")
    message: str = Field(default="Task submitted successfully")


class TaskStatusResponse(BaseModel):
    """Response model for task status."""

    task_id: str
    status: str
    created_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration: Optional[float] = None


class TaskResultResponse(BaseModel):
    """Response model for task result."""

    task_id: str
    status: str
    result: Optional[Any] = None
    error: Optional[str] = None
    created_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration: Optional[float] = None


class TaskListResponse(BaseModel):
    """Response model for task list."""

    tasks: list[str]
    count: int


class TaskCancelResponse(BaseModel):
    """Response model for task cancellation."""

    task_id: str
    cancelled: bool
    message: str


def mk_http_interface(
    functions: Optional[list[Callable]] = None,
    backend: Optional[ComputationBackend] = None,
    store: Optional[ComputationStore] = None,
    title: str = "AU Task API",
    description: str = "Async task management API",
    version: str = "0.1.0",
) -> FastAPI:
    """Create a FastAPI application for task management.

    Args:
        functions: Optional list of functions to register
        backend: Optional backend for task execution
        store: Optional store for results
        title: API title
        description: API description
        version: API version

    Returns:
        FastAPI application instance

    Raises:
        ImportError: If FastAPI is not installed

    Example:
        >>> @async_compute
        >>> def my_func(n: int) -> int:
        >>>     return n * 2
        >>>
        >>> app = mk_http_interface([my_func])
        >>> # Run with: uvicorn main:app --reload
    """
    if not HAS_FASTAPI:
        raise ImportError(
            "FastAPI is required for HTTP interface. "
            "Install with: pip install au[http]"
        )

    app = FastAPI(
        title=title,
        description=description,
        version=version,
    )

    # Store registered functions
    registered_functions: dict[str, Callable] = {}
    if functions:
        for func in functions:
            registered_functions[func.__name__] = func

    # Get backend and store
    _backend = backend or _get_default_backend()
    _store = store or _get_default_store()

    # Root endpoint
    @app.get("/")
    def root():
        """Get API information."""
        return {
            "name": title,
            "version": version,
            "endpoints": {
                "submit": "POST /tasks",
                "status": "GET /tasks/{task_id}/status",
                "result": "GET /tasks/{task_id}/result",
                "list": "GET /tasks",
                "cancel": "DELETE /tasks/{task_id}",
            },
            "registered_functions": list(registered_functions.keys()),
        }

    # Submit task endpoint
    @app.post("/tasks", response_model=TaskSubmitResponse, status_code=202)
    def submit_task_endpoint(request: TaskSubmitRequest = Body(...)):
        """Submit a new task for execution.

        Returns HTTP 202 Accepted with task ID.
        """
        # Check if function is registered
        if request.function_name not in registered_functions:
            raise HTTPException(
                status_code=404,
                detail=f"Function '{request.function_name}' not registered. "
                       f"Available functions: {list(registered_functions.keys())}"
            )

        func = registered_functions[request.function_name]

        try:
            task_id = submit_task(
                func,
                *request.args,
                backend=_backend,
                store=_store,
                **request.kwargs
            )

            return TaskSubmitResponse(
                task_id=task_id,
                status="pending",
                message="Task submitted successfully"
            )

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # Get task status endpoint
    @app.get("/tasks/{task_id}/status", response_model=TaskStatusResponse)
    def get_task_status(task_id: str):
        """Get the status of a task."""
        try:
            status = get_status(task_id, store=_store)

            # Get additional metadata if available
            handle = ComputationHandle(task_id, _store)
            metadata = handle.metadata

            response = TaskStatusResponse(
                task_id=task_id,
                status=status.value,
            )

            if metadata:
                if metadata.created_at:
                    response.created_at = metadata.created_at.isoformat()
                if metadata.completed_at:
                    response.completed_at = metadata.completed_at.isoformat()
                if metadata.duration is not None:
                    response.duration = metadata.duration

            return response

        except KeyError:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # Get task result endpoint
    @app.get("/tasks/{task_id}/result", response_model=TaskResultResponse)
    def get_task_result(
        task_id: str,
        wait: bool = Query(False, description="Wait for task to complete"),
        timeout: Optional[float] = Query(None, description="Timeout in seconds"),
    ):
        """Get the result of a task.

        If wait=true, blocks until task completes or timeout.
        Otherwise, returns current status immediately.
        """
        try:
            handle = ComputationHandle(task_id, _store)

            if wait:
                # Block until result is ready
                try:
                    result_value = handle.get_result(timeout=timeout)
                    status = ComputationStatus.COMPLETED
                    error = None
                except TimeoutError:
                    # Timeout while waiting
                    status = handle.get_status()
                    result_value = None
                    error = "Timeout while waiting for result"
                except Exception as e:
                    status = ComputationStatus.FAILED
                    result_value = None
                    error = str(e)
            else:
                # Get current status without waiting
                status = handle.get_status()

                if status == ComputationStatus.COMPLETED:
                    result_value = handle.get_result(timeout=0)
                    error = None
                elif status == ComputationStatus.FAILED:
                    try:
                        handle.get_result(timeout=0)
                        result_value = None
                        error = None
                    except Exception as e:
                        result_value = None
                        error = str(e)
                else:
                    result_value = None
                    error = None

            # Get metadata
            metadata = handle.metadata
            response = TaskResultResponse(
                task_id=task_id,
                status=status.value,
                result=result_value,
                error=error,
            )

            if metadata:
                if metadata.created_at:
                    response.created_at = metadata.created_at.isoformat()
                if metadata.completed_at:
                    response.completed_at = metadata.completed_at.isoformat()
                if metadata.duration is not None:
                    response.duration = metadata.duration

            return response

        except KeyError:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # List tasks endpoint
    @app.get("/tasks", response_model=TaskListResponse)
    def list_tasks():
        """List all task IDs in the store."""
        try:
            task_ids = list(_store)
            return TaskListResponse(
                tasks=task_ids,
                count=len(task_ids)
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # Cancel task endpoint
    @app.delete("/tasks/{task_id}", response_model=TaskCancelResponse)
    def cancel_task_endpoint(task_id: str):
        """Cancel a running task."""
        try:
            # Check if task exists
            if task_id not in _store:
                raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

            cancelled = cancel_task(task_id, backend=_backend, store=_store)

            return TaskCancelResponse(
                task_id=task_id,
                cancelled=cancelled,
                message="Cancellation attempted" if cancelled else "Task not cancellable"
            )

        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # Health check endpoint
    @app.get("/health")
    def health_check():
        """Health check endpoint."""
        return {
            "status": "healthy",
            "backend": type(_backend).__name__,
            "store": type(_store).__name__,
        }

    return app


def create_app_from_decorator(
    title: str = "AU Task API",
    description: str = "Async task management API",
) -> FastAPI:
    """Create a FastAPI app that auto-discovers @async_compute decorated functions.

    Note: This requires functions to be imported/registered before app creation.

    Args:
        title: API title
        description: API description

    Returns:
        FastAPI application instance
    """
    if not HAS_FASTAPI:
        raise ImportError(
            "FastAPI is required for HTTP interface. "
            "Install with: pip install au[http]"
        )

    # For now, create empty app
    # In future, could use registry pattern to auto-discover decorated functions
    return mk_http_interface(
        functions=[],
        title=title,
        description=description,
    )


# Flask support (if available)
try:
    from flask import Flask, request, jsonify
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False


def mk_flask_interface(
    functions: Optional[list[Callable]] = None,
    backend: Optional[ComputationBackend] = None,
    store: Optional[ComputationStore] = None,
) -> 'Flask':
    """Create a Flask application for task management.

    Args:
        functions: Optional list of functions to register
        backend: Optional backend
        store: Optional store

    Returns:
        Flask application instance

    Raises:
        ImportError: If Flask is not installed
    """
    if not HAS_FLASK:
        raise ImportError(
            "Flask is required for Flask interface. "
            "Install with: pip install au[flask]"
        )

    from flask import Flask, request, jsonify

    app = Flask(__name__)

    # Store registered functions
    registered_functions: dict[str, Callable] = {}
    if functions:
        for func in functions:
            registered_functions[func.__name__] = func

    _backend = backend or _get_default_backend()
    _store = store or _get_default_store()

    @app.route('/')
    def root():
        return jsonify({
            "name": "AU Task API (Flask)",
            "registered_functions": list(registered_functions.keys()),
        })

    @app.route('/tasks', methods=['POST'])
    def submit():
        data = request.get_json()
        func_name = data.get('function_name')

        if func_name not in registered_functions:
            return jsonify({"error": "Function not registered"}), 404

        func = registered_functions[func_name]
        args = data.get('args', [])
        kwargs = data.get('kwargs', {})

        try:
            task_id = submit_task(func, *args, backend=_backend, store=_store, **kwargs)
            return jsonify({"task_id": task_id, "status": "pending"}), 202
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/tasks/<task_id>/status')
    def status(task_id):
        try:
            status = get_status(task_id, store=_store)
            return jsonify({"task_id": task_id, "status": status.value})
        except KeyError:
            return jsonify({"error": "Task not found"}), 404

    @app.route('/tasks/<task_id>/result')
    def result(task_id):
        wait = request.args.get('wait', 'false').lower() == 'true'
        timeout = request.args.get('timeout', type=float)

        try:
            if wait:
                result_value = get_result(task_id, timeout=timeout, store=_store)
                return jsonify({
                    "task_id": task_id,
                    "status": "completed",
                    "result": result_value
                })
            else:
                status = get_status(task_id, store=_store)
                if status == ComputationStatus.COMPLETED:
                    result_value = get_result(task_id, store=_store)
                    return jsonify({
                        "task_id": task_id,
                        "status": status.value,
                        "result": result_value
                    })
                else:
                    return jsonify({
                        "task_id": task_id,
                        "status": status.value
                    })
        except KeyError:
            return jsonify({"error": "Task not found"}), 404
        except TimeoutError:
            return jsonify({"error": "Timeout"}), 408

    return app
