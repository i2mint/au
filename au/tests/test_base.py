import sys
import time
from datetime import timedelta
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
    ComputationCancelledError,
    ComputationResult,
)

# Functions a worker process runs are defined at module level and wrapped inside
# the tests: where processes are spawned (Windows, macOS) the function is pickled
# by reference, which a local function or a name rebound by a decorator cannot be.


def _double(x):
    return x * 2


def _inc(x):
    return x + 1


def _square(x):
    return x * x


def _slow(x):
    time.sleep(10)
    return x


def _make_dict(x):
    return {"val": x, "list": [x, x + 1]}


def _identity(x):
    return x


def test_basic_async_compute(tmp_path):
    double = async_compute(base_path=tmp_path)(_double)

    handle = double(21)
    result = handle.get_result(timeout=5)
    assert result == 42
    assert handle.is_ready()
    assert handle.get_status() == ComputationStatus.COMPLETED


def test_custom_backend_and_store(tmp_path):
    store = FileSystemStore(tmp_path, ttl_seconds=60)
    backend = ProcessBackend(store)

    inc = async_compute(backend=backend, store=store)(_inc)

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

    square = async_compute(base_path=tmp_path, middleware=[metrics, logging_mw])(
        _square
    )

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
    assert stats["total"] == 5
    assert stats["completed"] == 5
    assert stats["failed"] == 0
    assert stats["avg_duration"] >= 0.0


def test_cancellation_and_termination(tmp_path):
    store = FileSystemStore(tmp_path, ttl_seconds=60)
    backend = ProcessBackend(store)

    slow = async_compute(backend=backend, store=store)(_slow)

    handle: ComputationHandle = slow(123)
    # Cancel and terminate the process
    cancelled = handle.cancel()
    assert cancelled is True
    # After cancellation, status should be CANCELLED (a first-class terminal
    # state, distinct from FAILED) and get_result should raise.
    with pytest.raises(ComputationCancelledError):
        handle.get_result(timeout=1)
    assert handle.get_status() == ComputationStatus.CANCELLED
    # The process should be terminated (no zombie)
    if hasattr(handle.backend, "terminate"):
        handle.backend.terminate(handle.key)  # Should be a no-op if already terminated


def test_pickle_serialization(tmp_path):
    make_dict = async_compute(
        base_path=tmp_path, serialization=SerializationFormat.PICKLE
    )(_make_dict)

    handle = make_dict(7)
    result = handle.get_result(timeout=5)
    assert result == {"val": 7, "list": [7, 8]}


def test_cleanup_expired(tmp_path):
    foo = async_compute(base_path=tmp_path, ttl_seconds=1)(_identity)

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


# --- races between writers and readers of one store (i2mint/au#6) -------------


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows refuses to replace a file a reader has open (PermissionError); "
    "see au#10 (retry the rename)",
)
def test_concurrent_writers_of_one_key_never_leave_a_torn_file(tmp_path):
    """The worker's RUNNING and the caller's CANCELLED share a key; two writers
    must not interleave into one file (a reader saw ``Extra data``)."""
    import threading

    store = FileSystemStore(tmp_path, auto_cleanup=False)
    key = store.create_key()
    big = ComputationResult("x" * 200_000, ComputationStatus.COMPLETED)
    small = ComputationResult(None, ComputationStatus.RUNNING)
    stop = threading.Event()
    torn = []

    def write(result):
        while not stop.is_set():
            store[key] = result

    def read(_=None):
        while not stop.is_set():
            got = store[key]
            if got.status == ComputationStatus.FAILED:
                torn.append(got.error)

    threads = [
        threading.Thread(target=f, args=(a,))
        for f, a in [(write, big), (write, small), (read, None)]
    ]
    for t in threads:
        t.start()
    time.sleep(1.0)
    stop.set()
    for t in threads:
        t.join()
    assert not torn, torn[:3]
    assert not list(tmp_path.glob("*.tmp"))  # every temp file was renamed or removed


def test_cleanup_expired_counts_what_it_removed_even_with_automatic_cleanup(tmp_path):
    store = FileSystemStore(tmp_path, ttl_seconds=1, cleanup_probability=1.0)
    key = store.create_key()
    done = ComputationResult(1, ComputationStatus.COMPLETED)
    done.created_at = done.created_at - timedelta(seconds=10)
    store[key] = done
    # The nested read inside cleanup used to start its own cleanup, which removed
    # the item first and left this call counting zero.
    for _ in range(20):
        store[key] = done
        assert store.cleanup_expired() == 1


def test_a_worker_starting_late_does_not_undo_a_cancellation(tmp_path):
    from au.base import _mark_running

    store = FileSystemStore(tmp_path, auto_cleanup=False)
    key = store.create_key()
    store[key] = ComputationResult(
        None, ComputationStatus.CANCELLED, terminal_reason="stop"
    )
    _mark_running(store, key)
    assert store[key].status == ComputationStatus.CANCELLED
    fresh = store.create_key()
    _mark_running(store, fresh)
    assert store[fresh].status == ComputationStatus.RUNNING


# --- review of au#8 -----------------------------------------------------------


def test_a_late_running_mark_works_on_a_store_that_raises_for_a_missing_key():
    """``InMemoryStore`` raises KeyError for a key nobody wrote yet."""
    from au.base import ThreadBackend, _mark_running
    from au.testing import InMemoryStore

    store = InMemoryStore()
    _mark_running(store, "never-written")
    assert store["never-written"].status == ComputationStatus.RUNNING

    @async_compute(backend=ThreadBackend(store), store=store)
    def double(x):
        return x * 2

    assert double(21).get_result(timeout=5) == 42


def _quick(x):
    time.sleep(0.2)
    return x


def test_a_cancel_just_after_launch_always_ends_cancelled(tmp_path):
    """A worker starting between cancel's write and the terminate used to leave
    RUNNING behind for good."""
    store = FileSystemStore(tmp_path, auto_cleanup=False)
    backend = ProcessBackend(store)
    wrapped = async_compute(backend=backend, store=store)(_quick)
    stuck, cancelled = 0, 0
    for i in range(80):
        handle = wrapped(i)
        time.sleep(0.001 * (1 + i % 2))
        if handle.cancel():
            cancelled += 1
            if handle.get_status() != ComputationStatus.CANCELLED:
                stuck += 1
    assert cancelled >= 70 and stuck == 0


def test_temp_files_never_look_like_keys_whatever_the_suffix(tmp_path):
    store = FileSystemStore(tmp_path, suffix=".tmp", auto_cleanup=False)
    key = store.create_key()
    seen_mid_write = []
    real = store._serialize

    def serialize_and_look(result):
        seen_mid_write.append(list(store))  # the temp file exists by now
        return real(result)

    store._serialize = serialize_and_look
    store[key] = ComputationResult(1, ComputationStatus.COMPLETED)
    assert seen_mid_write == [[]]
    assert list(store) == [key] and len(store) == 1


# --- platforms without fork (i2mint/au#7) --------------------------------------


def test_process_backend_builds_where_fork_does_not_exist(monkeypatch):
    """Windows: ``set_start_method('fork')`` raises ValueError, so it is not asked."""
    import multiprocessing

    def no_fork(method, force=False):
        raise ValueError("cannot find context for 'fork'")

    monkeypatch.setattr(multiprocessing, "get_all_start_methods", lambda: ["spawn"])
    monkeypatch.setattr(multiprocessing, "set_start_method", no_fork)
    monkeypatch.setattr(multiprocessing, "get_start_method", lambda *a, **k: "spawn")
    backend = ProcessBackend()
    assert backend.store is None


def _put_double(queue, x):
    queue.put(x * 2)


@pytest.fixture
def spawn_start_method():
    """Run with the ``spawn`` start method (what Windows has), then put ``fork``
    back for the rest of the session."""
    import multiprocessing

    previous = multiprocessing.get_start_method(allow_none=True)
    multiprocessing.set_start_method("spawn", force=True)
    try:
        yield
    finally:
        multiprocessing.set_start_method(previous or "fork", force=True)


def test_spawn_accepts_what_only_a_starting_process_can_receive(
    tmp_path, spawn_start_method
):
    """A Queue pickles only while a process is being started, so pickling the
    arguments up front (the first version of this change) refused it."""
    import multiprocessing

    store = FileSystemStore(tmp_path)
    backend = ProcessBackend(store)
    multiprocessing.set_start_method("spawn", force=True)  # the backend forces fork
    queue = multiprocessing.Queue()
    backend.launch(_put_double, (queue, 21), {}, "k", store)
    assert queue.get(timeout=30) == 42
    backend._processes["k"].join(10)


def test_a_spawned_process_gets_a_clear_refusal_for_an_unpicklable_function(
    tmp_path, spawn_start_method
):
    import multiprocessing

    store = FileSystemStore(tmp_path)
    backend = ProcessBackend(store)
    multiprocessing.set_start_method("spawn", force=True)
    with pytest.raises(ValueError, match="module-level function"):
        backend.launch(lambda x: x, (1,), {}, "k", store)
    assert "k" not in backend._processes


def test_a_launch_that_raises_does_not_leave_the_key_running(tmp_path):
    from au.api import submit_task
    from au.base import ComputationBackend

    class Refusing(ComputationBackend):
        def launch(self, func, args, kwargs, key, store=None):
            raise ValueError("cannot launch")

    store = FileSystemStore(tmp_path)
    with pytest.raises(ValueError, match="cannot launch"):
        submit_task(_identity, 1, key="job", backend=Refusing(), store=store)
    assert store["job"].status == ComputationStatus.FAILED
    # ... so a resubmit under the same key is not taken for a live duplicate.
    backend = ProcessBackend(store)
    assert submit_task(_identity, 1, key="job", backend=backend, store=store) == "job"


def test_a_retry_policy_wrapper_pickles_by_reference():
    import pickle

    from au.api import _WithRetryPolicy
    from au.retry import RetryPolicy

    wrapped = pickle.loads(pickle.dumps(_WithRetryPolicy(_identity, RetryPolicy())))
    assert wrapped(5) == 5
