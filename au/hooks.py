"""
Enhanced hooks and observability for AU.

Provides lifecycle hooks, event callbacks, and improved middleware.
"""

from typing import Callable, Optional, Any, Protocol
from dataclasses import dataclass, field
from datetime import datetime
import time
import logging

from au.base import Middleware


class TaskEventHandler(Protocol):
    """Protocol for task event handlers."""

    def __call__(self, task_id: str, **kwargs) -> None:
        """Handle a task event.

        Args:
            task_id: Task identifier
            **kwargs: Event-specific data
        """
        ...


@dataclass
class TaskEvent:
    """Event fired during task lifecycle.

    Attributes:
        task_id: Task identifier
        event_type: Type of event (start, complete, error, retry)
        timestamp: When event occurred
        data: Event-specific data
    """

    task_id: str
    event_type: str
    timestamp: datetime = field(default_factory=datetime.now)
    data: dict[str, Any] = field(default_factory=dict)


class HooksMiddleware(Middleware):
    """Middleware that provides lifecycle hooks.

    Allows registering callbacks for different task events.
    """

    def __init__(
        self,
        on_start: Optional[TaskEventHandler] = None,
        on_complete: Optional[TaskEventHandler] = None,
        on_error: Optional[TaskEventHandler] = None,
        on_retry: Optional[TaskEventHandler] = None,
    ):
        """Initialize hooks middleware.

        Args:
            on_start: Callback when task starts
            on_complete: Callback when task completes successfully
            on_error: Callback when task fails
            on_retry: Callback when task is retried
        """
        self.on_start = on_start
        self.on_complete = on_complete
        self.on_error = on_error
        self.on_retry = on_retry
        self._events: list[TaskEvent] = []

    def before_compute(self, func_name: str, args: tuple, kwargs: dict):
        """Called before computation starts."""
        task_id = kwargs.get('__task_id', 'unknown')

        event = TaskEvent(
            task_id=task_id,
            event_type='start',
            data={
                'func_name': func_name,
                'args': args,
                'kwargs': kwargs,
            }
        )
        self._events.append(event)

        if self.on_start:
            self.on_start(
                task_id,
                func_name=func_name,
                args=args,
                kwargs=kwargs,
                timestamp=event.timestamp,
            )

    def after_compute(self, func_name: str, result: Any, duration: Optional[float]):
        """Called after successful computation."""
        task_id = 'unknown'  # Would need to be passed in

        event = TaskEvent(
            task_id=task_id,
            event_type='complete',
            data={
                'func_name': func_name,
                'result': result,
                'duration': duration,
            }
        )
        self._events.append(event)

        if self.on_complete:
            self.on_complete(
                task_id,
                func_name=func_name,
                result=result,
                duration=duration,
                timestamp=event.timestamp,
            )

    def on_error_hook(self, func_name: str, error: Exception):
        """Called when computation fails."""
        task_id = 'unknown'

        event = TaskEvent(
            task_id=task_id,
            event_type='error',
            data={
                'func_name': func_name,
                'error': str(error),
                'error_type': type(error).__name__,
            }
        )
        self._events.append(event)

        if self.on_error:
            self.on_error(
                task_id,
                func_name=func_name,
                error=error,
                timestamp=event.timestamp,
            )

    def get_events(self) -> list[TaskEvent]:
        """Get all recorded events.

        Returns:
            List of task events
        """
        return self._events.copy()


class TracingMiddleware(Middleware):
    """Middleware for distributed tracing (OpenTelemetry compatible).

    Provides trace IDs and span information for task execution.
    """

    def __init__(
        self,
        service_name: str = "au-tasks",
        trace_backend: Optional[str] = None,
    ):
        """Initialize tracing middleware.

        Args:
            service_name: Name of the service for tracing
            trace_backend: Optional tracing backend (opentelemetry, jaeger, zipkin)
        """
        self.service_name = service_name
        self.trace_backend = trace_backend
        self._spans: dict[str, dict[str, Any]] = {}

    def before_compute(self, func_name: str, args: tuple, kwargs: dict):
        """Start a new trace span."""
        import uuid
        trace_id = str(uuid.uuid4())
        span_id = str(uuid.uuid4())

        self._spans[func_name] = {
            'trace_id': trace_id,
            'span_id': span_id,
            'start_time': time.time(),
            'func_name': func_name,
        }

        logging.debug(f"[TRACE] Started span {span_id} for {func_name}")

    def after_compute(self, func_name: str, result: Any, duration: Optional[float]):
        """Complete the trace span."""
        if func_name in self._spans:
            span = self._spans[func_name]
            span['end_time'] = time.time()
            span['duration'] = duration
            span['status'] = 'success'

            logging.debug(
                f"[TRACE] Completed span {span['span_id']} "
                f"for {func_name} in {duration:.3f}s"
            )

    def on_error_hook(self, func_name: str, error: Exception):
        """Mark span as failed."""
        if func_name in self._spans:
            span = self._spans[func_name]
            span['end_time'] = time.time()
            span['status'] = 'error'
            span['error'] = str(error)

            logging.debug(
                f"[TRACE] Span {span['span_id']} failed "
                f"for {func_name}: {error}"
            )


class MetricsCollectorMiddleware(Middleware):
    """Enhanced metrics middleware with histogram support.

    Collects detailed metrics about task execution.
    """

    def __init__(self, metrics_backend: Optional[str] = None):
        """Initialize metrics collector.

        Args:
            metrics_backend: Optional metrics backend (prometheus, statsd, datadog)
        """
        self.metrics_backend = metrics_backend
        self._durations: list[float] = []
        self._status_counts: dict[str, int] = {
            'success': 0,
            'error': 0,
        }
        self._function_counts: dict[str, int] = {}

    def before_compute(self, func_name: str, args: tuple, kwargs: dict):
        """Track function invocation."""
        if func_name not in self._function_counts:
            self._function_counts[func_name] = 0
        self._function_counts[func_name] += 1

    def after_compute(self, func_name: str, result: Any, duration: Optional[float]):
        """Record successful completion metrics."""
        self._status_counts['success'] += 1
        if duration is not None:
            self._durations.append(duration)

    def on_error_hook(self, func_name: str, error: Exception):
        """Record error metrics."""
        self._status_counts['error'] += 1

    def get_metrics(self) -> dict[str, Any]:
        """Get collected metrics.

        Returns:
            Dictionary of metrics
        """
        metrics = {
            'total_tasks': sum(self._status_counts.values()),
            'successful_tasks': self._status_counts['success'],
            'failed_tasks': self._status_counts['error'],
            'function_counts': self._function_counts.copy(),
        }

        if self._durations:
            metrics['duration'] = {
                'count': len(self._durations),
                'min': min(self._durations),
                'max': max(self._durations),
                'avg': sum(self._durations) / len(self._durations),
                'total': sum(self._durations),
            }

        return metrics


class CompositeMiddleware(Middleware):
    """Combines multiple middleware into one.

    Allows using multiple middleware together.
    """

    def __init__(self, middlewares: list[Middleware]):
        """Initialize composite middleware.

        Args:
            middlewares: List of middleware to combine
        """
        self.middlewares = middlewares

    def before_compute(self, func_name: str, args: tuple, kwargs: dict):
        """Call before_compute on all middleware."""
        for middleware in self.middlewares:
            middleware.before_compute(func_name, args, kwargs)

    def after_compute(self, func_name: str, result: Any, duration: Optional[float]):
        """Call after_compute on all middleware."""
        for middleware in self.middlewares:
            middleware.after_compute(func_name, result, duration)

    def on_error_hook(self, func_name: str, error: Exception):
        """Call on_error on all middleware."""
        for middleware in self.middlewares:
            middleware.on_error_hook(func_name, error)


# Pre-configured middleware combinations


def create_observability_middleware(
    logging_level: str = "INFO",
    enable_metrics: bool = True,
    enable_tracing: bool = False,
    on_start: Optional[TaskEventHandler] = None,
    on_complete: Optional[TaskEventHandler] = None,
    on_error: Optional[TaskEventHandler] = None,
) -> Middleware:
    """Create a complete observability middleware stack.

    Args:
        logging_level: Logging level
        enable_metrics: Enable metrics collection
        enable_tracing: Enable distributed tracing
        on_start: Optional start hook
        on_complete: Optional completion hook
        on_error: Optional error hook

    Returns:
        Composite middleware with all observability features
    """
    from au.base import LoggingMiddleware

    middlewares = []

    # Add logging
    middlewares.append(LoggingMiddleware(level=logging_level))

    # Add hooks if provided
    if on_start or on_complete or on_error:
        middlewares.append(HooksMiddleware(
            on_start=on_start,
            on_complete=on_complete,
            on_error=on_error,
        ))

    # Add metrics
    if enable_metrics:
        middlewares.append(MetricsCollectorMiddleware())

    # Add tracing
    if enable_tracing:
        middlewares.append(TracingMiddleware())

    if len(middlewares) == 1:
        return middlewares[0]
    else:
        return CompositeMiddleware(middlewares)
