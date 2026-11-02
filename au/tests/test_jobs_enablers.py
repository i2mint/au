"""Tests for the three foundation fixes that enable the ``nw.jobs`` consumer.

Grouped by fix:

* **au-1** — signature reconciliation between the ergonomic ``au.api`` surface,
  the ``au.hooks`` middleware layer, and the real ``base`` backends (end-to-end
  ``submit_task`` -> ``get_result`` on several backends, and hooks middleware
  actually receiving before/after/on_error callbacks with the right arguments).
* **au-2** — first-class ``ComputationStatus.CANCELLED``.
* **au-3** — caller-supplied idempotency ``key=`` on ``submit_task``.
"""

import math
import threading
from datetime import datetime, timedelta, timezone

import pytest

from au.api import submit_task, get_result, get_handle, cancel_task
from au.base import (
    ThreadBackend,
    StdLibQueueBackend,
    ProcessBackend,
    FileSystemStore,
    ComputationHandle,
    ComputationResult,
    ComputationStatus,
    ComputationCancelledError,
    Middleware,
)
from au.testing import SyncTestBackend, InMemoryStore
from au.hooks import (
    HooksMiddleware,
    TracingMiddleware,
    MetricsCollectorMiddleware,
    CompositeMiddleware,
    create_observability_middleware,
)


# ---- Module-level functions (picklable, for process/stdlib backends) --------


def _mul(x, y):
    """Multiply two numbers."""
    return x * y


def _boom():
    """Always raise."""
    raise ValueError("boom")


# ============================================================================
# au-1 — end-to-end submit/get across backends
# ============================================================================


def _make_thread_backend():
    return ThreadBackend()


def _make_stdlib_thread_backend():
    return StdLibQueueBackend(use_processes=False)


# NOTE on backend coverage: the fork-based backends (ProcessBackend and
# StdLibQueueBackend(use_processes=True)) are covered in a *separate* test
# (test_submit_task_get_result_process_backend) rather than parametrized
# alongside the in-process ones. ProcessBackend forces the multiprocessing
# 'fork' start method, and forking after thread-based backends have run in the
# same interpreter is unsafe on macOS (fork-after-threads deadlock). Keeping
# the fork path in its own test avoids interleaving it with the thread pools.
@pytest.mark.parametrize(
    "make_backend",
    [
        _make_thread_backend,
        _make_stdlib_thread_backend,
    ],
)
def test_submit_task_get_result_end_to_end(tmp_path, make_backend):
    """submit_task -> get_result works end-to-end on the in-process backends.

    Exercises the au-1 launch-signature fix: ``api.submit_task`` calls
    ``backend.launch(func, args, kwargs, key, store)`` and every backend now
    accepts the per-launch store.
    """
    store = FileSystemStore(tmp_path, ttl_seconds=60)
    backend = make_backend()
    try:
        task_id = submit_task(_mul, 6, 7, backend=backend, store=store)
        assert isinstance(task_id, str)
        assert get_result(task_id, timeout=15, store=store) == 42
    finally:
        if hasattr(backend, "shutdown"):
            backend.shutdown()


@pytest.mark.parametrize(
    "make_backend",
    [
        lambda: ProcessBackend(),
        lambda: StdLibQueueBackend(),
        lambda: ThreadBackend(),
    ],
)
def test_backend_constructs_without_store_and_resolves_per_launch(make_backend):
    """au-1: backends build with no store and resolve the store per-launch.

    Before the fix, ``ProcessBackend()`` / ``StdLibQueueBackend()`` /
    ``ThreadBackend()`` required a store positionally, so ``_get_default_backend``
    (which builds them store-less and lets ``submit_task`` pass the store to
    ``launch``) could not construct them. This unit-checks the store-resolution
    contract without forking a real worker (the fork path is covered end-to-end
    for ProcessBackend by test_base.py).
    """
    backend = make_backend()
    assert backend.store is None
    per_launch = InMemoryStore()
    # Per-launch store is used when the backend has none of its own.
    assert backend._resolve_store(per_launch) is per_launch
    # And a clear error when neither is available.
    with pytest.raises(ValueError):
        backend._resolve_store(None)


def test_submit_task_get_result_default_backend(tmp_path):
    """submit_task -> get_result works with the module-level *default* backend."""
    import au.api as api

    prev_backend, prev_store = api._default_backend, api._default_store
    try:
        api.set_default_backend(ThreadBackend())
        api.set_default_store(FileSystemStore(tmp_path, ttl_seconds=60))
        task_id = submit_task(_mul, 6, 7)  # no explicit backend/store
        assert get_result(task_id, timeout=15) == 42
    finally:
        api._default_backend, api._default_store = prev_backend, prev_store


def test_submit_honors_explicit_empty_store_over_global_default():
    """An explicit (empty) store must win over the global default.

    Regression guard for the au-1 api fix: stores are ``MutableMapping``s, so an
    *empty* one is falsy — the old ``store = store or _get_default_store()``
    silently swapped a caller's empty store for the global default. The task
    must land in the caller's store, not the global one.
    """
    import au.api as api

    prev_backend, prev_store = api._default_backend, api._default_store
    try:
        global_store = InMemoryStore()
        api.set_default_store(global_store)
        api.set_default_backend(SyncTestBackend())

        explicit = InMemoryStore()  # empty -> falsy as a Mapping
        task_id = submit_task(_mul, 6, 7, backend=SyncTestBackend(), store=explicit)

        assert get_result(task_id, store=explicit) == 42
        assert task_id in explicit  # landed in the caller's store
        assert task_id not in global_store  # NOT the global default
    finally:
        api._default_backend, api._default_store = prev_backend, prev_store


# ============================================================================
# au-1 — the au.hooks middleware layer actually drives the compute path
# ============================================================================


def test_hooks_layer_classes_conform_to_middleware_protocol():
    """Every au.hooks middleware instantiates and implements the base protocol.

    Regression guard: before au-1 these classes defined ``on_error_hook`` (not
    ``on_error``) and so could not even be instantiated (abstract ``on_error``).
    """
    middlewares = [
        HooksMiddleware(),
        TracingMiddleware(),
        MetricsCollectorMiddleware(),
        CompositeMiddleware([MetricsCollectorMiddleware()]),
        create_observability_middleware(),
    ]
    for mw in middlewares:
        assert isinstance(mw, Middleware)
        # The exact protocol the backends invoke — not the old *_hook name.
        assert hasattr(mw, "before_compute")
        assert hasattr(mw, "after_compute")
        assert hasattr(mw, "on_error")
        assert not hasattr(mw, "on_error_hook")


def test_hooks_middleware_receives_before_and_after(tmp_path):
    """A HooksMiddleware attached to a backend gets before/after with right args."""
    calls = {"start": [], "complete": [], "error": []}
    mw = HooksMiddleware(
        on_start=lambda task_id, **kw: calls["start"].append((task_id, kw)),
        on_complete=lambda task_id, **kw: calls["complete"].append((task_id, kw)),
        on_error=lambda task_id, **kw: calls["error"].append((task_id, kw)),
    )
    backend = SyncTestBackend(middleware=[mw])
    store = InMemoryStore()
    key = store.create_key()

    backend.launch(_mul, (3, 4), {}, key, store)

    assert store[key].value == 12

    # before_compute -> on_start(task_id=key, func_name, args, kwargs, timestamp)
    assert len(calls["start"]) == 1
    start_tid, start_kw = calls["start"][0]
    assert start_tid == key
    assert start_kw["func_name"] == "_mul"
    assert start_kw["args"] == (3, 4)
    assert start_kw["kwargs"] == {}

    # after_compute -> on_complete(task_id=key, result=<value>, duration, timestamp)
    assert len(calls["complete"]) == 1
    complete_tid, complete_kw = calls["complete"][0]
    assert complete_tid == key
    assert complete_kw["result"] == 12
    assert "duration" in complete_kw

    # No error on the success path.
    assert calls["error"] == []

    # The middleware's own event log carries the real task_id (the key).
    events = mw.get_events()
    assert [e.event_type for e in events] == ["start", "complete"]
    assert all(e.task_id == key for e in events)


def test_hooks_middleware_receives_on_error():
    """A HooksMiddleware attached to a backend gets on_error(key, exception)."""
    calls = {"start": [], "complete": [], "error": []}
    mw = HooksMiddleware(
        on_start=lambda task_id, **kw: calls["start"].append((task_id, kw)),
        on_complete=lambda task_id, **kw: calls["complete"].append((task_id, kw)),
        on_error=lambda task_id, **kw: calls["error"].append((task_id, kw)),
    )
    backend = SyncTestBackend(middleware=[mw])
    store = InMemoryStore()
    key = store.create_key()

    backend.launch(_boom, (), {}, key, store)

    assert store[key].status == ComputationStatus.FAILED
    assert len(calls["start"]) == 1
    assert calls["complete"] == []

    # on_error -> callback(task_id=key, error=<exception>, timestamp)
    assert len(calls["error"]) == 1
    err_tid, err_kw = calls["error"][0]
    assert err_tid == key
    assert isinstance(err_kw["error"], Exception)
    assert str(err_kw["error"]) == "boom"


def test_hooks_middleware_wired_on_real_async_backend(tmp_path):
    """The hooks layer also drives a real (threaded) compute path."""
    completed = threading.Event()
    seen = {}

    def _on_complete(task_id, **kw):
        seen["task_id"] = task_id
        seen["result"] = kw.get("result")
        completed.set()

    mw = HooksMiddleware(on_complete=_on_complete)
    store = FileSystemStore(tmp_path, ttl_seconds=60)
    backend = ThreadBackend(store, middleware=[mw])

    task_id = submit_task(_mul, 5, 5, backend=backend, store=store)
    assert get_result(task_id, timeout=10, store=store) == 25
    assert completed.wait(timeout=5), "after_compute hook never fired"
    assert seen["task_id"] == task_id
    assert seen["result"] == 25


# ============================================================================
# au-2 — first-class ComputationStatus.CANCELLED
# ============================================================================


def test_cancel_sets_cancelled_status_not_failed():
    """Cancelling an in-flight computation yields CANCELLED (a distinct terminal)."""
    store = InMemoryStore()
    key = store.create_key()
    store[key] = ComputationResult(None, ComputationStatus.RUNNING)
    handle = ComputationHandle(key, store)

    assert handle.cancel() is True

    result = store[key]
    assert result.status == ComputationStatus.CANCELLED
    assert result.status != ComputationStatus.FAILED
    assert result.is_ready  # CANCELLED counts as terminal
    assert result.terminal_reason  # a reason was recorded


def test_get_result_on_cancelled_raises_cancelled_error():
    """get_result on a cancelled computation raises ComputationCancelledError."""
    store = InMemoryStore()
    key = store.create_key()
    store[key] = ComputationResult(None, ComputationStatus.RUNNING)
    handle = ComputationHandle(key, store)
    handle.cancel(reason="stopped by user")

    with pytest.raises(ComputationCancelledError, match="stopped by user"):
        handle.get_result(timeout=0.1)


def test_cancel_task_api_routes_to_cancelled():
    """The api.cancel_task helper also routes to CANCELLED."""
    store = InMemoryStore()
    key = store.create_key()
    store[key] = ComputationResult(None, ComputationStatus.RUNNING)
    # cancel_task builds its own handle over the given store.
    assert cancel_task(key, backend=SyncTestBackend(), store=store) is True
    assert get_handle(key, store=store).get_status() == ComputationStatus.CANCELLED


def test_genuine_failure_is_still_failed():
    """A real failure remains FAILED (not conflated with cancellation)."""
    backend = SyncTestBackend()
    store = InMemoryStore()
    key = store.create_key()
    backend.launch(_boom, (), {}, key, store)
    assert store[key].status == ComputationStatus.FAILED


def test_cancel_on_terminal_returns_false():
    """Cancelling an already-terminal computation is a no-op returning False."""
    store = InMemoryStore()
    key = store.create_key()
    store[key] = ComputationResult(42, ComputationStatus.COMPLETED)
    handle = ComputationHandle(key, store)
    assert handle.cancel() is False
    assert store[key].status == ComputationStatus.COMPLETED


def test_cancelled_roundtrips_through_filesystem_store(tmp_path):
    """CANCELLED + terminal_reason survive filesystem (de)serialization."""
    store = FileSystemStore(tmp_path)
    key = store.create_key()
    store[key] = ComputationResult(
        None, ComputationStatus.CANCELLED, terminal_reason="stopped by user"
    )
    got = store[key]
    assert got.status == ComputationStatus.CANCELLED
    assert got.terminal_reason == "stopped by user"
    assert got.is_ready


# ============================================================================
# au-3 — caller-supplied idempotency key
# ============================================================================


def test_idempotency_key_dedups_in_flight(tmp_path):
    """Two submit_task(key=...) while in flight run the work once, share the id."""
    calls = {"n": 0}
    started = threading.Event()
    release = threading.Event()

    def work(x):
        calls["n"] += 1
        started.set()
        release.wait(timeout=5)
        return x * 2

    store = FileSystemStore(tmp_path, ttl_seconds=60)
    backend = ThreadBackend(store)

    k1 = submit_task(work, 5, key="job-x", backend=backend, store=store)
    assert started.wait(timeout=5), "first submission never started"

    # Second submission with the same key must NOT launch a duplicate.
    k2 = submit_task(work, 5, key="job-x", backend=backend, store=store)
    assert k1 == k2 == "job-x"

    release.set()
    assert get_result(k1, timeout=5, store=store) == 10
    assert calls["n"] == 1  # the work ran exactly once


def test_idempotency_key_reruns_when_terminal(tmp_path):
    """A terminal (completed) idempotency key re-runs on resubmission."""
    calls = {"n": 0}

    def work(x):
        calls["n"] += 1
        return x * 2

    store = InMemoryStore()
    backend = SyncTestBackend()

    k1 = submit_task(work, 5, key="job-y", backend=backend, store=store)
    assert get_result(k1, store=store) == 10
    assert calls["n"] == 1

    # The first run completed (terminal) -> resubmit re-runs the work.
    k2 = submit_task(work, 5, key="job-y", backend=backend, store=store)
    assert k2 == "job-y"
    assert get_result(k2, store=store) == 10
    assert calls["n"] == 2


def test_default_key_mints_unique_ids(tmp_path):
    """Without an explicit key, each submission gets a fresh unique id."""
    store = InMemoryStore()
    backend = SyncTestBackend()
    ids = {submit_task(_mul, i, 2, backend=backend, store=store) for i in range(3)}
    assert len(ids) == 3


def test_an_idempotency_key_dedups_only_a_running_record():
    """Only a RUNNING record is in flight; a missing, terminal or expired one
    re-runs (what ``_inflight_record`` classified, before the reservation
    became one compare-and-set)."""
    store, backend = InMemoryStore(ttl_seconds=1), SyncTestBackend()
    old = ComputationResult(1, ComputationStatus.COMPLETED)
    old.created_at = datetime.now() - timedelta(seconds=10)
    records = {
        "run": ComputationResult(None, ComputationStatus.RUNNING),
        "done": ComputationResult(1, ComputationStatus.COMPLETED),
        "old": old,
    }
    for key, record in records.items():
        store[key] = record
    for key in ["nope", "run", "done", "old"]:
        submit_task(_mul, 3, 2, key=key, backend=backend, store=store)
    assert store["run"].status == ComputationStatus.RUNNING  # deduped, not run
    for key in ["nope", "done", "old"]:  # "old": a guard, as a fresh one re-runs
        assert store[key].value == 6  # (re-)ran
    plain = {}  # any mapping, as for the workers' writes
    submit_task(_mul, 3, 2, key="k", backend=backend, store=plain)
    assert plain["k"].value == 6


def test_concurrent_submits_of_one_key_launch_once(tmp_path):
    """i2mint/au#31: both submits read "not in flight", then both reserved and
    launched. The reservation is now a compare-and-set."""
    import threading

    store = FileSystemStore(tmp_path, auto_cleanup=False)
    launched = []

    class _Counting(SyncTestBackend):
        def launch(self, func, args, kwargs, key, store=None):
            launched.append(key)  # leaves the reservation RUNNING: in flight

    backend, start = _Counting(), threading.Barrier(8)

    def submit():
        start.wait()
        submit_task(_mul, 1, 2, key="once", backend=backend, store=store)

    threads = [threading.Thread(target=submit) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)
    assert not any(thread.is_alive() for thread in threads)
    assert launched == ["once"]


# --- i2mint/au#36: a reservation lease ----------------------------------------


@pytest.fixture
def lease():
    """Set ``reservation_lease_seconds`` for the test, then restore the default."""
    from au.config import AUConfig, reset_global_config, set_global_config

    yield lambda seconds: set_global_config(AUConfig(reservation_lease_seconds=seconds))
    reset_global_config()


def _running(*, began_ago, started_ago=None):
    record = ComputationResult(None, ComputationStatus.RUNNING)
    record.created_at = datetime.now() - timedelta(seconds=began_ago)
    if started_ago is not None:
        record.started_at = datetime.now() - timedelta(seconds=started_ago)
    return record


def test_without_a_lease_a_stale_reservation_holds_its_key(lease):
    lease(None)
    store, backend = InMemoryStore(), SyncTestBackend()
    store["k"] = _running(began_ago=10**6)
    submit_task(_mul, 3, 2, key="k", backend=backend, store=store)
    assert store["k"].status == ComputationStatus.RUNNING  # deduped, as before


def test_a_reservation_older_than_its_lease_no_longer_holds_its_key(lease):
    lease(60)
    store, backend = InMemoryStore(), SyncTestBackend()
    store["stale"] = _running(began_ago=120)
    store["fresh"] = _running(began_ago=1)
    store["started"] = _running(began_ago=120, started_ago=1)  # began: started_at
    for key in store.keys():
        submit_task(_mul, 3, 2, key=key, backend=backend, store=store)
    assert store["stale"].value == 6  # re-ran
    assert store["fresh"].status == store["started"].status == ComputationStatus.RUNNING


def test_a_lease_compares_aware_and_naive_times_as_instants(lease):
    """A naive time is local, an aware one any zone: each is compared in UTC."""
    lease(60)
    local = datetime.now().astimezone().utcoffset()
    behind = timezone(local - timedelta(hours=10))  # wall clock behind local
    fresh = datetime.now(timezone.utc) - timedelta(seconds=1)
    store, backend = InMemoryStore(), SyncTestBackend()
    for key, began in {
        "naive": fresh.astimezone().replace(tzinfo=None),
        "aware": fresh.astimezone(behind),  # same instant: read naive, 10 h old
        "aware stale": (fresh - timedelta(seconds=120)).astimezone(behind),
    }.items():
        store[key] = ComputationResult(None, ComputationStatus.RUNNING)
        store[key].created_at = began
    for key in list(store.keys()):
        submit_task(_mul, 3, 2, key=key, backend=backend, store=store)
    assert store["naive"].status == store["aware"].status == ComputationStatus.RUNNING
    assert store["aware stale"].value == 6  # re-ran, no TypeError


def test_concurrent_resubmits_of_a_stale_key_launch_once(lease, tmp_path):
    lease(60)
    store = FileSystemStore(tmp_path, auto_cleanup=False)
    store["k"] = _running(began_ago=120)
    launched = []

    class _Counting(SyncTestBackend):
        def launch(self, func, args, kwargs, key, store=None):
            launched.append(key)

    backend, start = _Counting(), threading.Barrier(8)

    def submit():
        start.wait()
        submit_task(_mul, 1, 2, key="k", backend=backend, store=store)

    threads = [threading.Thread(target=submit) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)
    assert not any(thread.is_alive() for thread in threads)
    assert launched == ["k"]


@pytest.mark.parametrize("seconds", [0, -5, 1e-9, 1e20, math.nan, math.inf, "60", True])
def test_a_lease_that_is_not_a_positive_finite_number_is_refused_by_name(
    lease, seconds
):
    lease(seconds)
    store = InMemoryStore()
    with pytest.raises(ValueError, match="AU_RESERVATION_LEASE_SECONDS"):
        submit_task(_mul, 1, 2, key="k", backend=SyncTestBackend(), store=store)
    assert "k" not in store


def test_an_unparseable_lease_in_the_environment_is_refused_not_dropped(
    monkeypatch,
):
    from au.config import get_config, reset_global_config, set_global_config

    monkeypatch.setenv("AU_RESERVATION_LEASE_SECONDS", "1h")
    set_global_config(get_config())
    try:
        with pytest.raises(ValueError, match="AU_RESERVATION_LEASE_SECONDS"):
            submit_task(_mul, 1, 2, key="k", backend=SyncTestBackend(), store={})
    finally:
        reset_global_config()


def test_the_lease_applies_to_a_plain_mapping_store(lease):
    lease(60)
    store = {"stale": _running(began_ago=120)}
    submit_task(_mul, 3, 2, key="stale", backend=SyncTestBackend(), store=store)
    assert store["stale"].value == 6


def test_a_store_overriding_write_unless_without_stale_after_still_works():
    """``write_unless`` was public before ``stale_after``: without a lease, an
    override with the old signature is called as before."""

    class _OldOverride(InMemoryStore):
        def write_unless(self, key, result, *, keep):
            return super().write_unless(key, result, keep=keep)

    store = _OldOverride()
    submit_task(_mul, 3, 2, key="k", backend=SyncTestBackend(), store=store)
    assert store["k"].value == 6


def test_the_lease_is_read_from_the_environment(monkeypatch):
    from au.config import load_config_from_env

    monkeypatch.setenv("AU_RESERVATION_LEASE_SECONDS", "90.5")
    assert load_config_from_env()["reservation_lease_seconds"] == 90.5


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
