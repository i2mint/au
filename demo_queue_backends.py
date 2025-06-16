#!/usr/bin/env python3
"""
Quick demo of the new AU queue backends
"""

import tempfile
import time
from au.base import (
    async_compute,
    FileSystemStore,
    StdLibQueueBackend,
    LoggingMiddleware,
)


def demo_function(x, y):
    """Simple demo function that can be pickled."""
    time.sleep(0.1)  # Simulate some work
    return x * y + 1


def main():
    print("=== AU Framework Queue Backends Demo ===")

    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"Using temp directory: {tmpdir}")

        # Create store
        store = FileSystemStore(tmpdir, ttl_seconds=300)
        middleware = [LoggingMiddleware()]

        # Demo StdLibQueueBackend with threads
        print("\n1. Testing StdLibQueueBackend with threads...")
        with StdLibQueueBackend(
            store, max_workers=2, use_processes=False, middleware=middleware
        ) as backend:
            # Create async version of our function
            async_demo = async_compute(backend=backend, store=store)(demo_function)

            # Launch some tasks
            handles = []
            for i in range(3):
                handle = async_demo(i, i + 1)
                handles.append(handle)
                print(f"  Launched task {i}: {handle.key[:8]}...")

            # Collect results
            print("  Collecting results...")
            for i, handle in enumerate(handles):
                result = handle.get_result(timeout=10)
                expected = i * (i + 1) + 1
                print(f"  Task {i}: {result} (expected: {expected})")
                assert result == expected

        print("✅ StdLibQueueBackend test passed!")

        # Demo StdLibQueueBackend with processes
        print("\n2. Testing StdLibQueueBackend with processes...")
        with StdLibQueueBackend(store, max_workers=2, use_processes=True) as backend:
            async_demo_proc = async_compute(backend=backend, store=store)(demo_function)

            handle = async_demo_proc(5, 6)
            result = handle.get_result(timeout=10)
            expected = 5 * 6 + 1
            print(f"  Process result: {result} (expected: {expected})")
            assert result == expected

        print("✅ Process-based test passed!")

        print(f"\n🎉 All tests passed! Queue backends are working correctly.")


if __name__ == "__main__":
    main()
