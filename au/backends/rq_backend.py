"""
RQ (Redis Queue) backend for au framework.

This module provides RQBackend for distributed task queuing using Redis.
It requires the 'rq' and 'redis' packages to be installed.
"""

import contextlib
import pickle
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Callable, Tuple

# Import for type checking only
if TYPE_CHECKING:
    import redis
    from rq import Queue
    from au.base import ComputationStore, Middleware, ComputationBackend

# Check if RQ dependencies are available
_HAS_RQ = False
with contextlib.suppress(ImportError):
    import redis
    from rq import Queue

    _HAS_RQ = True

# Import base classes
try:
    from au.base import ComputationBackend, _au_worker_entrypoint
except ImportError:
    # Handle case where we're importing this before base is fully loaded
    pass


class RQBackend(ComputationBackend):
    """
    Backend that uses RQ (Redis Queue) for distributed task processing.

    This backend enqueues tasks to Redis and relies on RQ workers to process them.

    Example:
        >>> import redis  # doctest: +SKIP
        >>> from rq import Queue  # doctest: +SKIP
        >>> redis_conn = redis.Redis()  # doctest: +SKIP
        >>> queue = Queue(connection=redis_conn)  # doctest: +SKIP
        >>> backend = RQBackend(store, queue)  # doctest: +SKIP
    """

    def __init__(
        self,
        store: "ComputationStore",
        rq_queue: "Queue",
        middleware: Optional[List["Middleware"]] = None,
    ):
        super().__init__(middleware)
        if not _HAS_RQ:
            raise ImportError(
                "RQBackend requires 'rq' and 'redis' packages. "
                "Please install them with: pip install rq redis"
            )
        self.store = store
        self.rq_queue = rq_queue

    def launch(self, func: Callable, args: tuple, kwargs: dict, key: str) -> None:
        """Enqueue computation task to RQ."""
        # Serialize task data
        task_data = {
            "func": func,
            "args": args,
            "kwargs": kwargs,
            "key": key,
            "store_reconstruction_info": self.store.get_reconstruction_info(),
            "middleware_configs": self._serialize_middleware(),
        }
        serialized_data = pickle.dumps(task_data)

        # Enqueue to RQ
        self.rq_queue.enqueue(
            _au_worker_entrypoint,
            serialized_data,
            job_id=key,  # Use our key as RQ job ID for easier tracking
            job_timeout="30m",  # Default timeout
        )

    def terminate(self, key: str) -> None:
        """Attempt to cancel RQ job by key."""
        try:
            job = self.rq_queue.job_class.fetch(
                key, connection=self.rq_queue.connection
            )
            if job:
                job.cancel()
        except Exception as e:
            # Job might not exist or already completed
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(f"Could not terminate RQ job {key}: {e}")

    def _serialize_middleware(self) -> List[Dict[str, Any]]:
        """Serialize middleware for worker process."""
        configs = []
        for mw in self.middleware:
            config = {"class": type(mw)}
            # Add specific configs for known middleware types
            if hasattr(mw, 'level') and hasattr(mw, 'logger'):  # LoggingMiddleware
                config["kwargs"] = {
                    "level": mw.level,
                    "logger_name": (
                        mw.logger.name if mw.logger.name != __name__ else None
                    ),
                }
            elif hasattr(mw, 'get_shared_state'):  # SharedMetricsMiddleware
                config["kwargs"] = mw.get_shared_state()
            else:  # Basic middleware like MetricsMiddleware
                config["kwargs"] = {}
            configs.append(config)
        return configs
