"""
Supabase PostgreSQL queue backend for au framework.

This module provides SupabaseQueueBackend for distributed task queuing using
Supabase PostgreSQL as a simulated queue. It requires the 'supabase' package.
"""

import contextlib
import json
import pickle
import time
import threading
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Callable, Tuple
import logging

# Import for type checking only
if TYPE_CHECKING:
    from supabase import Client as SupabaseClient
    from au.base import ComputationStore, Middleware, ComputationBackend

# Check if Supabase dependencies are available
_HAS_SUPABASE = False
with contextlib.suppress(ImportError):
    from supabase import Client as SupabaseClient

    _HAS_SUPABASE = True

# Import base classes
try:
    from au.base import ComputationBackend, _au_worker_entrypoint
except ImportError:
    # Handle case where we're importing this before base is fully loaded
    pass

logger = logging.getLogger(__name__)


class SupabaseQueueBackend(ComputationBackend):
    """
    Backend that uses Supabase PostgreSQL as a simulated task queue.

    This backend stores tasks in a PostgreSQL table and uses an internal polling
    worker to process them. This is a simulated queue and may have different
    performance characteristics than dedicated message brokers.

    Required table schema:
        CREATE TABLE au_task_queue (
            task_id UUID PRIMARY KEY,
            func_data BYTEA NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            started_at TIMESTAMP WITH TIME ZONE,
            completed_at TIMESTAMP WITH TIME ZONE,
            worker_id TEXT
        );

    Example:
        >>> from supabase import create_client  # doctest: +SKIP
        >>> supabase = create_client(url, key)  # doctest: +SKIP
        >>> backend = SupabaseQueueBackend(store, supabase)  # doctest: +SKIP
    """

    def __init__(
        self,
        store: "ComputationStore",
        supabase_client: "SupabaseClient",
        queue_table_name: str = "au_task_queue",
        max_concurrent_tasks: int = 2,
        polling_interval_seconds: float = 1.0,
        middleware: Optional[List["Middleware"]] = None,
    ):
        super().__init__(middleware)
        if not _HAS_SUPABASE:
            raise ImportError(
                "SupabaseQueueBackend requires 'supabase' package. "
                "Please install it with: pip install supabase"
            )

        self.store = store
        self.supabase = supabase_client
        self.queue_table_name = queue_table_name
        self.max_concurrent_tasks = max_concurrent_tasks
        self.polling_interval_seconds = polling_interval_seconds

        # Internal worker management
        self._worker_threads: List[threading.Thread] = []
        self._stop_polling_event = threading.Event()
        self._started = False
        self._worker_id = str(uuid.uuid4())[:8]  # Short worker ID

        # Ensure table exists (basic check)
        self._ensure_queue_table_exists()

    def _ensure_queue_table_exists(self) -> None:
        """
        Check if the queue table exists.
        Note: This is a basic check - in production, you should create
        the table via Supabase migrations or SQL scripts.
        """
        try:
            # Try a simple query to check if table exists
            self.supabase.table(self.queue_table_name).select("task_id").limit(
                1
            ).execute()
        except Exception as e:
            logger.warning(
                f"Could not verify {self.queue_table_name} table exists. "
                f"Please ensure it's created with the required schema. Error: {e}"
            )

    def launch(self, func: Callable, args: tuple, kwargs: dict, key: str) -> None:
        """Enqueue task to Supabase PostgreSQL table."""
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

        # Insert into queue table
        try:
            self.supabase.table(self.queue_table_name).insert(
                {
                    "task_id": key,
                    "func_data": serialized_data.hex(),  # Store as hex string
                    "status": "pending",
                }
            ).execute()
        except Exception as e:
            logger.error(f"Failed to enqueue task {key}: {e}")
            raise

        # Start polling workers if not already started
        self._ensure_workers_started()

    def _ensure_workers_started(self) -> None:
        """Start polling worker threads if not already started."""
        if not self._started:
            self._started = True
            for i in range(self.max_concurrent_tasks):
                worker_thread = threading.Thread(
                    target=self._polling_worker,
                    name=f"SupabaseWorker-{self._worker_id}-{i}",
                    daemon=True,
                )
                worker_thread.start()
                self._worker_threads.append(worker_thread)
            logger.info(f"Started {self.max_concurrent_tasks} Supabase polling workers")

    def _polling_worker(self) -> None:
        """Main polling loop for processing tasks."""
        while not self._stop_polling_event.is_set():
            try:
                # Try to claim a pending task
                task = self._claim_next_task()
                if task:
                    self._process_task(task)
                else:
                    # No tasks available, sleep
                    time.sleep(self.polling_interval_seconds)
            except Exception as e:
                logger.error(f"Error in polling worker: {e}", exc_info=True)
                time.sleep(self.polling_interval_seconds)

    def _claim_next_task(self) -> Optional[Dict[str, Any]]:
        """Atomically claim the next pending task."""
        try:
            # Use a transaction-like approach to claim a task
            # First, get a pending task
            response = (
                self.supabase.table(self.queue_table_name)
                .select("*")
                .eq("status", "pending")
                .order("created_at", desc=False)
                .limit(1)
                .execute()
            )

            if not response.data:
                return None

            task = response.data[0]
            task_id = task["task_id"]

            # Try to atomically update it to running
            update_response = (
                self.supabase.table(self.queue_table_name)
                .update(
                    {
                        "status": "running",
                        "started_at": "NOW()",
                        "worker_id": self._worker_id,
                    }
                )
                .eq("task_id", task_id)
                .eq("status", "pending")
                .execute()
            )

            if update_response.data:
                # Successfully claimed the task
                task["status"] = "running"
                return task
            else:
                # Task was claimed by another worker
                return None

        except Exception as e:
            logger.error(f"Error claiming task: {e}")
            return None

    def _process_task(self, task: Dict[str, Any]) -> None:
        """Process a claimed task."""
        task_id = task["task_id"]
        try:
            # Deserialize func_data
            func_data_hex = task["func_data"]
            serialized_data = bytes.fromhex(func_data_hex)

            # Execute the task using the worker entrypoint
            _au_worker_entrypoint(serialized_data)

            # Mark task as completed
            self.supabase.table(self.queue_table_name).update(
                {"status": "completed", "completed_at": "NOW()"}
            ).eq("task_id", task_id).execute()

        except Exception as e:
            logger.error(f"Error processing task {task_id}: {e}", exc_info=True)
            # Mark task as failed
            try:
                self.supabase.table(self.queue_table_name).update(
                    {"status": "failed", "completed_at": "NOW()"}
                ).eq("task_id", task_id).execute()
            except Exception as update_error:
                logger.error(f"Error updating failed task status: {update_error}")

    def terminate(self, key: str) -> None:
        """Attempt to terminate a task by marking it as failed."""
        try:
            self.supabase.table(self.queue_table_name).update(
                {"status": "failed", "completed_at": "NOW()"}
            ).eq("task_id", key).in_("status", ["pending", "running"]).execute()
        except Exception as e:
            logger.warning(f"Could not terminate task {key}: {e}")

    def shutdown(self) -> None:
        """Shutdown the polling workers."""
        if self._started:
            self._stop_polling_event.set()
            for thread in self._worker_threads:
                thread.join(timeout=5.0)
            self._started = False
            logger.info("Supabase polling workers shut down")

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
            else:  # Basic middleware
                config["kwargs"] = {}
            configs.append(config)
        return configs

    def __enter__(self):
        """Context manager entry."""
        self._ensure_workers_started()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.shutdown()
