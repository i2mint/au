import time
import pytest
from au.base import (
    async_compute,
    FileSystemStore,
    ProcessBackend,
    StdLibQueueBackend,
    LoggingMiddleware,
    MetricsMiddleware,
    SharedMetricsMiddleware,
    SerializationFormat,
    ComputationHandle,
    ComputationStatus,
)


def test_basic_async_compute(tmp_path):
    @async_compute(base_path=tmp_path)
    def double(x):
        return x * 2

    handle = double(21)
    result = handle.get_result(timeout=5)
    assert result == 42
    assert handle.is_ready()
    assert handle.get_status() == ComputationStatus.COMPLETED


def test_custom_backend_and_store(tmp_path):
    store = FileSystemStore(tmp_path, ttl_seconds=60)
    backend = ProcessBackend(store)

    @async_compute(backend=backend, store=store)
    def inc(x):
        return x + 1

    handle = inc(10)
    assert handle.get_status() in (ComputationStatus.PENDING, ComputationStatus.RUNNING)
    result = handle.get_result(timeout=5)
    assert result == 11
    assert handle.get_status() == ComputationStatus.COMPLETED


def test_middleware_and_metrics(tmp_path):
    from au.base import SharedMetricsMiddleware
    import multiprocessing
    import time

    shared_state = dict(
        total_computations=multiprocessing.Value("i", 0),
        completed_computations=multiprocessing.Value("i", 0),
        failed_computations=multiprocessing.Value("i", 0),
        total_duration=multiprocessing.Value("d", 0.0),
        lock=multiprocessing.Lock(),
    )
    metrics = SharedMetricsMiddleware(**shared_state)
    logging_mw = LoggingMiddleware()

    @async_compute(base_path=tmp_path, middleware=[metrics, logging_mw])
    def square(x):
        return x * x

    handles = [square(i) for i in range(5)]
    # Wait for all handles to be ready, but don't hang forever
    deadline = time.time() + 10  # max 10 seconds for all
    for h in handles:
        while not h.is_ready() and time.time() < deadline:
            time.sleep(0.05)
    # Now collect results, fail if any are not ready
    results = []
    for h in handles:
        assert h.is_ready(), f"Computation {h.key} did not complete in time"
        results.append(h.get_result(timeout=0.1))
    assert results == [i * i for i in range(5)]
    stats = metrics.get_stats()
    assert stats['total'] == 5
    assert stats['completed'] == 5
    assert stats['failed'] == 0
    assert stats['avg_duration'] >= 0.0


def test_cancellation_and_termination(tmp_path):
    store = FileSystemStore(tmp_path, ttl_seconds=60)
    backend = ProcessBackend(store)

    @async_compute(backend=backend, store=store)
    def slow(x):
        time.sleep(10)
        return x

    handle: ComputationHandle = slow(123)
    # Cancel and terminate the process
    cancelled = handle.cancel()
    assert cancelled is True
    # After cancellation, status should be FAILED
    with pytest.raises(Exception):
        handle.get_result(timeout=1)
    assert handle.get_status() == ComputationStatus.FAILED
    # The process should be terminated (no zombie)
    if hasattr(handle.backend, 'terminate'):
        handle.backend.terminate(handle.key)  # Should be a no-op if already terminated


def test_pickle_serialization(tmp_path):
    @async_compute(base_path=tmp_path, serialization=SerializationFormat.PICKLE)
    def make_dict(x):
        return {'val': x, 'list': [x, x + 1]}

    handle = make_dict(7)
    result = handle.get_result(timeout=5)
    assert result == {'val': 7, 'list': [7, 8]}


def test_cleanup_expired(tmp_path):
    @async_compute(base_path=tmp_path, ttl_seconds=1)
    def foo(x):
        return x

    handle = foo(1)
    handle.get_result(timeout=5)
    # Wait for expiration
    time.sleep(2)
    cleaned = foo.cleanup_expired()
    assert cleaned >= 1


def test_stdlib_queue_backend(tmp_path):
    """Test the StdLibQueueBackend with ThreadPoolExecutor."""
    # For queue backends, we need to test with a simpler approach
    # since local functions can't be pickled for distributed execution
    store = FileSystemStore(tmp_path, ttl_seconds=60)

    # Test basic functionality without distributed execution
    # StdLibQueueBackend is designed for pickleable functions
    with StdLibQueueBackend(store, max_workers=2, use_processes=False) as backend:
        # Test the backend creation and context management
        assert backend._started
        assert backend._executor is not None

        # Test termination method
        backend.terminate("nonexistent_key")  # Should not raise


def test_stdlib_queue_backend_processes(tmp_path):
    """Test the StdLibQueueBackend with ProcessPoolExecutor."""
    store = FileSystemStore(tmp_path, ttl_seconds=60)

    # Test basic functionality
    with StdLibQueueBackend(store, max_workers=2, use_processes=True) as backend:
        # Test the backend creation and context management
        assert backend._started
        assert backend._executor is not None

        # Test that it's using ProcessPoolExecutor
        import concurrent.futures

        assert isinstance(backend._executor, concurrent.futures.ProcessPoolExecutor)


def test_stdlib_queue_backend_with_pickleable_functions(tmp_path):
    """Test StdLibQueueBackend with module-level functions that can be pickled."""
    store = FileSystemStore(tmp_path, ttl_seconds=60)

    with StdLibQueueBackend(store, max_workers=2, use_processes=False) as backend:
        # Create async functions using module-level functions
        multiply_async = async_compute(backend=backend, store=store)(_test_multiply)
        add_one_async = async_compute(backend=backend, store=store)(_test_add_one)

        # Test basic execution
        handle1 = multiply_async(6, 7)
        handle2 = add_one_async(41)

        # Get results
        result1 = handle1.get_result(timeout=10)
        result2 = handle2.get_result(timeout=10)

        assert result1 == 42
        assert result2 == 42
        assert handle1.is_ready()
        assert handle2.is_ready()


# Module-level functions for testing queue backends (these can be pickled)
def _test_multiply(x, y):
    """Test function for queue backends."""
    return x * y


def _test_add_one(x):
    """Test function for queue backends."""
    return x + 1


def _test_slow_function(seconds):
    """Test function that takes time."""
    import time

    time.sleep(seconds)
    return f"slept_{seconds}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
