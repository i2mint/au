"""
Examples demonstrating the new queue backends for au framework.

This module shows how to use the different backends:
1. StdLibQueueBackend - Using Python's standard library
2. RQBackend - Using Redis Queue (requires rq and redis packages)
3. SupabaseQueueBackend - Using Supabase PostgreSQL (requires supabase package)
"""

import tempfile
import time
from au.base import (
    async_compute,
    FileSystemStore,
    StdLibQueueBackend,
    LoggingMiddleware,
    MetricsMiddleware,
    SerializationFormat,
)


def example_stdlib_backend():
    """Example using StdLibQueueBackend with ThreadPoolExecutor."""
    print("=== StdLibQueueBackend Example ===")

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create store and backend
        store = FileSystemStore(tmpdir, ttl_seconds=300)
        middleware = [LoggingMiddleware(), MetricsMiddleware()]

        # Use context manager for proper cleanup
        with StdLibQueueBackend(
            store,
            max_workers=3,
            use_processes=False,  # Use threads for I/O-bound tasks
            middleware=middleware,
        ) as backend:

            # Define async functions
            @async_compute(backend=backend, store=store)
            def fetch_data(url_id: int):
                """Simulate fetching data from an API."""
                time.sleep(0.5)  # Simulate network delay
                return f"data_from_url_{url_id}"

            @async_compute(backend=backend, store=store)
            def process_text(text: str):
                """Simulate text processing."""
                time.sleep(0.2)  # Simulate processing time
                return text.upper()

            # Launch multiple tasks
            print("Launching tasks...")
            fetch_handles = [fetch_data(i) for i in range(5)]

            # Wait for fetch tasks and process results
            process_handles = []
            for handle in fetch_handles:
                data = handle.get_result(timeout=10)
                process_handle = process_text(data)
                process_handles.append(process_handle)

            # Get final results
            results = [h.get_result(timeout=10) for h in process_handles]
            print(f"Final results: {results}")


def example_rq_backend():
    """Example using RQBackend (requires redis and rq packages)."""
    print("=== RQBackend Example ===")

    try:
        import redis
        from rq import Queue
        from au.backends.rq_backend import RQBackend

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create Redis connection and RQ queue
            redis_conn = redis.Redis(host="localhost", port=6379, db=0)
            rq_queue = Queue("au_tasks", connection=redis_conn)

            # Create store and backend
            store = FileSystemStore(tmpdir, ttl_seconds=300)
            backend = RQBackend(store, rq_queue)

            @async_compute(backend=backend, store=store)
            def cpu_intensive_task(n: int):
                """Simulate CPU-intensive work."""
                result = 0
                for i in range(n * 1000000):
                    result += i % 7
                return result

            print("Launching CPU-intensive task...")
            handle = cpu_intensive_task(10)

            print("Task enqueued. Start RQ worker with: rq worker au_tasks")
            print(f"Task key: {handle.key}")
            print(
                "Check status with handle.get_status() and get result with handle.get_result()"
            )

    except ImportError:
        print("RQ example requires 'redis' and 'rq' packages:")
        print("pip install redis rq")


def example_supabase_backend():
    """Example using SupabaseQueueBackend (requires supabase package)."""
    print("=== SupabaseQueueBackend Example ===")

    try:
        from supabase import create_client
        from au.backends.supabase_backend import SupabaseQueueBackend

        # Note: You need to provide your own Supabase URL and key
        SUPABASE_URL = "your-supabase-url"
        SUPABASE_KEY = "your-supabase-anon-key"

        if SUPABASE_URL == "your-supabase-url":
            print("Please configure your Supabase URL and key in the example")
            return

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create Supabase client
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

            # Create store and backend
            store = FileSystemStore(tmpdir, ttl_seconds=300)

            with SupabaseQueueBackend(
                store, supabase, max_concurrent_tasks=2, polling_interval_seconds=2.0
            ) as backend:

                @async_compute(backend=backend, store=store)
                def analyze_data(data_id: str):
                    """Simulate data analysis."""
                    time.sleep(1.0)  # Simulate analysis time
                    return {
                        "data_id": data_id,
                        "analysis_result": f"processed_{data_id}",
                        "confidence": 0.95,
                    }

                print("Launching analysis tasks...")
                handles = [analyze_data(f"dataset_{i}") for i in range(3)]

                # Wait for results
                results = []
                for handle in handles:
                    result = handle.get_result(timeout=30)
                    results.append(result)

                print(f"Analysis results: {results}")

    except ImportError:
        print("Supabase example requires 'supabase' package:")
        print("pip install supabase")


def example_backend_comparison():
    """Compare different backends for the same task."""
    print("=== Backend Comparison ===")

    def cpu_task(n: int) -> int:
        """Simple CPU task for comparison."""
        return sum(i * i for i in range(n))

    with tempfile.TemporaryDirectory() as tmpdir:
        store = FileSystemStore(tmpdir, ttl_seconds=300)

        # Test StdLib backend with processes (good for CPU-bound)
        print("Testing StdLibQueueBackend with processes...")
        start_time = time.time()

        with StdLibQueueBackend(store, max_workers=2, use_processes=True) as backend:

            @async_compute(backend=backend, store=store)
            def stdlib_cpu_task(n: int) -> int:
                return cpu_task(n)

            handles = [stdlib_cpu_task(1000) for _ in range(4)]
            results = [h.get_result(timeout=30) for h in handles]

        stdlib_time = time.time() - start_time
        print(f"StdLib backend time: {stdlib_time:.2f}s")

        # Test StdLib backend with threads (comparison)
        print("Testing StdLibQueueBackend with threads...")
        start_time = time.time()

        with StdLibQueueBackend(store, max_workers=2, use_processes=False) as backend:

            @async_compute(backend=backend, store=store)
            def stdlib_thread_task(n: int) -> int:
                return cpu_task(n)

            handles = [stdlib_thread_task(1000) for _ in range(4)]
            results = [h.get_result(timeout=30) for h in handles]

        thread_time = time.time() - start_time
        print(f"Thread backend time: {thread_time:.2f}s")

        print(
            f"Process backend was {thread_time/stdlib_time:.1f}x faster for CPU-bound tasks"
        )


if __name__ == "__main__":
    example_stdlib_backend()
    print()
    example_rq_backend()
    print()
    example_supabase_backend()
    print()
    example_backend_comparison()
