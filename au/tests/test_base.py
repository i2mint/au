import contextlib
import functools
import json
import os
import pickle
import threading
import logging
import time
from pathlib import Path
from datetime import datetime, timedelta
import pytest
from au.testing import InMemoryStore
from au.base import (
    async_compute,
    FileSystemStore,
    ProcessBackend,
    StdLibQueueBackend,
    Middleware,
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
    # A fast child can already be done here: only a failure would be wrong.
    assert handle.get_status() != ComputationStatus.FAILED
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
    # after_compute runs once COMPLETED is written: let the children finish.
    while metrics.get_stats()["completed"] < 5 and time.time() < deadline:
        time.sleep(0.05)
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


def test_a_busy_file_is_retried_on_windows_and_not_elsewhere(tmp_path, monkeypatch):
    import au.base as base

    store = FileSystemStore(tmp_path, auto_cleanup=False)
    key = store.create_key()
    real_replace = Path.replace
    refusals = []

    def busy_twice(self, target):
        if len(refusals) < 2:
            refusals.append(1)
            raise PermissionError(13, "Access is denied")
        return real_replace(self, target)

    monkeypatch.setattr(Path, "replace", busy_twice)
    monkeypatch.setattr(base, "_RETRY_PERMISSION_ERRORS", True)
    monkeypatch.setattr(base, "_PERMISSION_RETRY_SECONDS", 0.0)
    store[key] = ComputationResult(1, ComputationStatus.COMPLETED)
    assert len(refusals) == 2 and store[key].value == 1

    # Off Windows a PermissionError is a real one: no retries, the write is lost
    # (and logged), exactly as before.
    refusals.clear()
    other = store.create_key()
    monkeypatch.setattr(base, "_RETRY_PERMISSION_ERRORS", False)
    store[other] = ComputationResult(2, ComputationStatus.COMPLETED)
    assert len(refusals) == 1 and store[other].status == ComputationStatus.PENDING


# --- i2mint/au#12: every failure is recorded and the release hook always runs --


class _Recorder(MetricsMiddleware):
    """Middleware that tracks in-flight keys, as nw's job manager does."""

    def __init__(self):
        super().__init__()
        self.in_flight, self.calls, self.results = set(), [], []

    def before_compute(self, func, args, kwargs, key):
        self.in_flight.add(key)

    def after_compute(self, key, result):
        self.calls.append(("after", key))
        self.results.append(result.status)
        self.in_flight.discard(key)

    def on_error(self, key, error):
        self.calls.append(("error", key, type(error)))
        self.in_flight.discard(key)


class _Refusing(dict):
    """A store that raises when asked to write any of the ``refused`` statuses."""

    def __init__(self, *refused):
        super().__init__()
        self.refused = refused

    def __setitem__(self, key, result):
        if result.status in self.refused:
            raise OSError(f"cannot write {result.status}")
        super().__setitem__(key, result)


class _Interrupt(BaseException):
    pass


def _interrupted():
    raise _Interrupt("stop")


def test_a_base_exception_is_recorded_as_failed_then_re_raised():
    from au.base import compute_and_record

    store, mw = {}, _Recorder()
    with pytest.raises(_Interrupt):
        compute_and_record(_interrupted, (), {}, key="k", store=store, middleware=[mw])
    assert store["k"].status == ComputationStatus.FAILED
    assert isinstance(store["k"].error, _Interrupt)
    assert mw.calls == [("error", "k", _Interrupt)] and not mw.in_flight


@pytest.mark.parametrize(
    "refused, recorded",
    [
        ((ComputationStatus.RUNNING,), ComputationStatus.FAILED),
        ((ComputationStatus.COMPLETED,), ComputationStatus.FAILED),
        # Nothing more can be written: RUNNING stays, but the hook still runs.
        (
            (ComputationStatus.COMPLETED, ComputationStatus.FAILED),
            ComputationStatus.RUNNING,
        ),
        (
            (
                ComputationStatus.RUNNING,
                ComputationStatus.COMPLETED,
                ComputationStatus.FAILED,
            ),
            None,
        ),
    ],
)
def test_a_store_that_fails_to_write_still_gets_the_release_hook(refused, recorded):
    from au.base import compute_and_record

    store, mw = _Refusing(*refused), _Recorder()
    # A failure the store will not record is raised (au#20), after the hook.
    unrecordable = ComputationStatus.FAILED in refused
    with pytest.raises(OSError) if unrecordable else contextlib.nullcontext():
        compute_and_record(_double, (2,), {}, key="k", store=store, middleware=[mw])
    assert mw.calls == [("error", "k", OSError)] and not mw.in_flight
    assert (store["k"].status if "k" in store else None) == recorded


def test_a_thread_backend_job_raising_a_base_exception_ends_failed(monkeypatch):
    import threading

    from au.base import ThreadBackend
    from au.testing import InMemoryStore

    escaped = threading.Event()
    monkeypatch.setattr(
        threading,
        "excepthook",
        lambda a: escaped.set() if a.exc_type is _Interrupt else None,
    )
    store, mw = InMemoryStore(), _Recorder()
    handle = async_compute(backend=ThreadBackend(store, middleware=[mw]), store=store)(
        _interrupted
    )()
    with pytest.raises(RuntimeError, match="stop"):
        handle.get_result(timeout=5)
    assert handle.get_status() == ComputationStatus.FAILED
    assert escaped.wait(5), "the BaseException was swallowed, not re-raised"
    assert mw.calls == [("error", handle.key, _Interrupt)] and not mw.in_flight


def test_a_cancellation_is_final_whatever_the_cancelled_job_then_does():
    from au.base import compute_and_record

    def cancelled_then(outcome):
        def job():
            store["k"] = ComputationResult(None, ComputationStatus.CANCELLED)
            return outcome()

        return job

    for outcome, raised, hook in [
        (lambda: 1, None, "after"),
        (_interrupted, _Interrupt, "error"),
    ]:
        store, mw = {}, _Recorder()
        with pytest.raises(raised) if raised else contextlib.nullcontext():
            compute_and_record(
                cancelled_then(outcome), (), {}, key="k", store=store, middleware=[mw]
            )
        assert store["k"].status == ComputationStatus.CANCELLED
        assert not mw.in_flight and [c[0] for c in mw.calls] == [hook]
        if hook == "after":
            assert mw.results == [ComputationStatus.CANCELLED]


class _ForgetfulStore(dict):
    """A store whose reads fail once the job has started (a dropped connection)."""

    def get(self, key, default=None):
        if key in self:
            raise ConnectionError("store unreachable")
        return default


def test_a_store_read_that_fails_does_not_lose_a_finished_job():
    from au.base import compute_and_record

    store, mw = _ForgetfulStore(), _Recorder()
    compute_and_record(_double, (21,), {}, key="k", store=store, middleware=[mw])
    assert dict.get(store, "k").value == 42
    assert [c[0] for c in mw.calls] == ["after"]


def _interrupted_hook(*args):
    raise _Interrupt("hook")


@pytest.mark.parametrize("hook", ["before_compute", "after_compute"])
def test_a_hook_raising_a_base_exception_does_not_strand_the_others(hook):
    from au.base import compute_and_record

    store, mw, interrupting = {}, _Recorder(), MetricsMiddleware()
    setattr(interrupting, hook, _interrupted_hook)
    with pytest.raises(_Interrupt):
        compute_and_record(
            _double,
            (2,),
            {},
            key="k",
            store=store,
            middleware=[interrupting, mw],
        )
    assert not mw.in_flight and len(mw.calls) == 1
    expected = {"before_compute": ComputationStatus.FAILED}
    assert store["k"].status == expected.get(hook, ComputationStatus.COMPLETED)


def _interrupted_without_a_message():
    raise _Interrupt()


def test_a_queue_job_raising_a_base_exception_ends_failed_and_says_what(tmp_path):
    store = FileSystemStore(tmp_path, auto_cleanup=False)
    with StdLibQueueBackend(store, use_processes=False) as backend:
        handle = async_compute(backend=backend, store=store)(
            _interrupted_without_a_message
        )()
        with pytest.raises(RuntimeError, match="_Interrupt"):
            handle.get_result(timeout=5)
        assert handle.get_status() == ComputationStatus.FAILED
        assert (
            async_compute(backend=backend, store=store)(_double)(4).get_result(
                timeout=5
            )
            == 8
        ), "the queue worker did not survive"


# --- i2mint/au#14: one middleware lifecycle; middleware crosses by pickle -----


def test_process_backend_runs_the_middleware_in_the_child_only(tmp_path, seen_dir):
    """The parent used to run ``before_compute`` and never release it."""
    store, mw = FileSystemStore(tmp_path, auto_cleanup=False), _Recorder()
    backend = ProcessBackend(store, middleware=[mw, _Tagged("child", seen_dir)])
    assert async_compute(backend=backend, store=store)(_inc)(1).get_result(timeout=5)
    assert not mw.in_flight and mw.calls == []
    deadline = time.time() + 5  # the child writes its note after the result
    while not (seen_dir / "child.json").exists() and time.time() < deadline:
        time.sleep(0.05)
    assert _seen(seen_dir, "child")["pid"] != os.getpid()


class _Tagged(Middleware):
    """Carries constructor state, and writes what it saw to a file any process
    can read: a worker runs an unpickled copy, not this instance."""

    def __init__(self, tag, out_dir):
        self.tag, self.out_dir = tag, out_dir

    def before_compute(self, func, args, kwargs, key):
        pass

    def after_compute(self, key, result):
        note = {"tag": self.tag, "id": id(self), "pid": os.getpid()}
        (Path(self.out_dir) / f"{self.tag}.json").write_text(json.dumps(note))

    def on_error(self, key, error):
        pass


class _HoldsAThreadLock(Middleware):
    def __init__(self):
        self.lock = threading.Lock()

    def before_compute(self, func, args, kwargs, key):
        pass

    def after_compute(self, key, result):
        pass

    def on_error(self, key, error):
        pass


def _seen(out_dir, tag):
    return json.loads((Path(out_dir) / f"{tag}.json").read_text())


def _note_completion(out_dir, task_id, **event):
    (Path(out_dir) / "completed").write_text(task_id)


@pytest.fixture
def seen_dir(tmp_path):
    """Where middleware notes what it saw, apart from the store's own files."""
    path = tmp_path / "seen"
    path.mkdir()
    return path


@pytest.mark.parametrize("use_processes", [False, True])
def test_a_queue_worker_runs_the_middleware_it_was_given(
    tmp_path, seen_dir, use_processes
):
    """A class outside au's three used to be dropped without a word."""
    store, mw = FileSystemStore(tmp_path, auto_cleanup=False), _Tagged("mine", seen_dir)
    backend = StdLibQueueBackend(
        store, use_processes=use_processes, middleware=[mw, MetricsMiddleware()]
    )
    with backend:
        handle = async_compute(backend=backend, store=store)(_inc)(1)
        assert handle.get_result(timeout=10) == 2
    seen = _seen(seen_dir, "mine")
    assert seen["tag"] == "mine"
    if use_processes:
        assert seen["pid"] != os.getpid()
    else:
        assert seen["id"] != id(mw), "the worker ran this instance, not a copy"


def test_au_observability_middleware_runs_on_a_queue(tmp_path, seen_dir):
    """Composite, hooks and tracing used to be refused or rebuilt without state."""
    from au.hooks import create_observability_middleware

    on_complete = functools.partial(_note_completion, seen_dir)
    mw = create_observability_middleware(enable_tracing=True, on_complete=on_complete)
    store = FileSystemStore(tmp_path, auto_cleanup=False)
    backend = StdLibQueueBackend(store, use_processes=True, middleware=[mw])
    with backend:
        handle = async_compute(backend=backend, store=store)(_inc)(1)
        assert handle.get_result(timeout=10) == 2
    assert (seen_dir / "completed").read_text() == handle.key


@pytest.mark.parametrize("make", [_HoldsAThreadLock, SharedMetricsMiddleware])
def test_a_middleware_that_does_not_pickle_is_refused_at_launch_by_name(tmp_path, make):
    """SharedMetricsMiddleware's counters cross only by inheritance (ProcessBackend)."""
    store = FileSystemStore(tmp_path, auto_cleanup=False)
    backend = StdLibQueueBackend(store, use_processes=False, middleware=[make()])
    with backend, pytest.raises(TypeError, match=f"{make.__name__} .*picklable"):
        async_compute(backend=backend, store=store)(_inc)(1)
    assert list(store) == []


def test_metrics_middleware_pickles_with_its_counts_and_a_lock_of_its_own():
    metrics = MetricsMiddleware()
    metrics.before_compute(_inc, (1,), {}, "k")
    copy = pickle.loads(pickle.dumps(metrics))
    copy.before_compute(_inc, (1,), {}, "k")
    assert (metrics.get_stats()["total"], copy.get_stats()["total"]) == (1, 2)


def test_a_job_queued_with_middleware_configs_still_runs(tmp_path, seen_dir):
    """Payloads queued by au <= 0.1.4: configs, not instances, and a class it
    could not rebuild (a required argument, no kwargs) is skipped, not fatal."""
    from au.base import _au_worker_entrypoint
    from au.hooks import CompositeMiddleware

    store = FileSystemStore(tmp_path, auto_cleanup=False)
    key = store.create_key()
    logging_config = {"class": LoggingMiddleware, "kwargs": {"level": 10}}
    task_data = dict(
        func=_inc,
        args=(1,),
        kwargs={},
        key=key,
        store_reconstruction_info=store.get_reconstruction_info(),
        middleware_configs=[
            logging_config,
            {"class": MetricsMiddleware, "kwargs": {}},
            {"class": CompositeMiddleware},
            {"class": _Tagged},
        ],
    )
    _au_worker_entrypoint(pickle.dumps(task_data))
    assert store[key].value == 2


def test_spawned_process_backend_runs_au_and_custom_middleware(
    tmp_path, seen_dir, spawn_start_method
):
    import multiprocessing

    store = FileSystemStore(tmp_path, auto_cleanup=False)
    middleware = [LoggingMiddleware(), MetricsMiddleware(), _Tagged("spawn", seen_dir)]
    backend = ProcessBackend(store, middleware=middleware)
    multiprocessing.set_start_method("spawn", force=True)  # the backend forces fork
    handle = async_compute(backend=backend, store=store)(_inc)(1)
    assert handle.get_result(timeout=30) == 2
    backend._processes[handle.key].join(30)
    assert _seen(seen_dir, "spawn")["tag"] == "spawn"


def test_a_spawned_process_says_when_the_middleware_does_not_pickle(
    tmp_path, spawn_start_method
):
    import multiprocessing

    store = FileSystemStore(tmp_path)
    backend = ProcessBackend(store, middleware=[_HoldsAThreadLock()])
    multiprocessing.set_start_method("spawn", force=True)
    with pytest.raises(ValueError, match="middleware that pickles"):
        backend.launch(_inc, (1,), {}, "k", store)
    assert "k" not in backend._processes


# --- i2mint/au#17: level names, and SharedMetricsMiddleware at the top level ---


@pytest.mark.parametrize("level", ["INFO", "info", logging.INFO])
def test_logging_middleware_takes_a_level_name_or_number(level, caplog):
    mw = LoggingMiddleware(level=level, logger_name="au.test17")
    with caplog.at_level(logging.INFO, logger="au.test17"):
        mw.before_compute(_inc, (1,), {}, "k")
    assert mw.level == logging.INFO and "Starting computation k" in caplog.text


def test_a_custom_level_name_is_found_as_registered():
    logging.addLevelName(5, "trace17")
    assert LoggingMiddleware(level="trace17").level == 5


@pytest.mark.parametrize(
    "level, error, says", [("noisy", ValueError, "'noisy'"), (20.0, TypeError, "20.0")]
)
def test_a_level_that_is_not_one_is_refused_at_construction(level, error, says):
    with pytest.raises(error, match=says):
        LoggingMiddleware(level=level)


def test_observability_middleware_hands_its_level_name_to_logging_middleware():
    from au.hooks import create_observability_middleware

    stack = create_observability_middleware(logging_level="DEBUG")
    (logging_mw,) = [m for m in stack.middlewares if isinstance(m, LoggingMiddleware)]
    assert logging_mw.level == logging.DEBUG


def test_shared_metrics_middleware_is_exported_from_au():
    import au

    assert au.SharedMetricsMiddleware is SharedMetricsMiddleware


# --- i2mint/au#20 ---------------------------------------------------------------


def _naps(seconds):
    time.sleep(seconds)
    return seconds


def test_a_result_duration_is_the_time_the_run_took(tmp_path):
    """The result used to be made after the call returned: duration ~0."""
    from au.base import compute_and_record

    store = FileSystemStore(tmp_path, auto_cleanup=False)
    compute_and_record(_naps, (0.2,), {}, key="k", store=store, middleware=[])
    assert store["k"].duration.total_seconds() >= 0.2  # through a JSON round trip


def test_a_long_job_is_not_expired_the_moment_it_finishes(tmp_path):
    """TTL still counts from the record's creation, not the run's start."""
    from au.base import compute_and_record

    store = FileSystemStore(tmp_path, ttl_seconds=0.1, auto_cleanup=False)
    compute_and_record(_naps, (0.15,), {}, key="k", store=store, middleware=[])
    assert not store.is_expired(store["k"])


def _fails():
    raise KeyError("the cause")


def test_get_result_chains_the_original_error():
    from au.base import ThreadBackend
    from au.testing import InMemoryStore

    store = InMemoryStore()
    handle = async_compute(backend=ThreadBackend(store), store=store)(_fails)()
    with pytest.raises(RuntimeError) as raised:
        handle.get_result(timeout=5)
    assert isinstance(raised.value.__cause__, KeyError)


@async_compute(store=InMemoryStore())
def _decorated_at_import(x):
    return x + 100


def test_mock_async_runs_every_default_backed_task_synchronously():
    import au.api
    import au.base
    from au.testing import InMemoryStore, mock_async

    before = (au.base._default_backend, au.api._default_backend)
    with mock_async() as mock:

        @async_compute(store=InMemoryStore())
        def task(x):
            return x * 2

        assert task(5).get_result() == 10
        # Decorated before the block, as module code is: looked up per call.
        assert _decorated_at_import(1).get_result(timeout=1) == 101
        assert au.api.submit_task(_double, 4, store=InMemoryStore())
        assert mock.task_count == 3
    assert (au.base._default_backend, au.api._default_backend) == before


def test_a_backend_that_is_callable_is_not_taken_for_the_function():
    from unittest.mock import MagicMock

    from au.testing import InMemoryStore

    backend = MagicMock()  # callable, and not a ComputationBackend
    async_compute(backend=backend, store=InMemoryStore())(_double)(3)
    assert backend.launch.call_args.args[:3] == (_double, (3,), {})


def test_a_queue_job_whose_failure_cannot_be_recorded_fails_its_queue_job(
    monkeypatch,
):
    """The entrypoint used to swallow it, and overwrite the cause."""
    import au.base as base

    store = _Refusing(ComputationStatus.FAILED)
    monkeypatch.setattr(base, "_reconstruct_store", lambda info: store)
    payload = pickle.dumps(
        dict(
            func=_fails,
            args=(),
            kwargs={},
            key="k",
            store_reconstruction_info={},
            middleware=[],
        )
    )
    with pytest.raises(OSError) as raised:
        base._au_worker_entrypoint(payload)
    assert isinstance(raised.value.__cause__, KeyError)


@pytest.mark.parametrize(
    "payload",
    [b"not a pickle", pickle.dumps({"key": "k", "store_reconstruction_info": {}})],
)
def test_a_queue_job_that_cannot_be_set_up_fails_its_queue_job(payload):
    """It used to return normally: the queue recorded it as done."""
    from au.base import _au_worker_entrypoint

    with pytest.raises(Exception):
        _au_worker_entrypoint(payload)


def test_a_running_record_says_when_the_run_began(tmp_path):
    from au.base import _mark_running

    store = FileSystemStore(tmp_path, auto_cleanup=False)
    began = datetime.now()
    _mark_running(store, "k", started_at=began)
    assert store["k"].started_at == began


def test_mock_async_keeps_the_simple_apis_results_in_memory():
    import au.api
    from au.testing import mock_async

    before = au.api._default_store
    with mock_async():
        key = au.api.submit_task(_double, 4)
        assert isinstance(au.api._default_store, InMemoryStore)
        assert au.api.get_result(key, timeout=1) == 8
    assert au.api._default_store is before
