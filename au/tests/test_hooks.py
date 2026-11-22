"""Tests for the lifecycle middleware in ``au.hooks`` (events, metrics, tracing, composition)."""

import logging
from datetime import datetime, timedelta

from au.base import ComputationResult, ComputationStatus, LoggingMiddleware
from au.hooks import (
    CompositeMiddleware,
    HooksMiddleware,
    MetricsCollectorMiddleware,
    TaskEvent,
    TracingMiddleware,
    create_observability_middleware,
)


def work(x):
    return x


def _result(value=1, *, seconds=None):
    start = datetime(2026, 1, 1)
    r = ComputationResult(value, ComputationStatus.COMPLETED, started_at=start)
    if seconds is not None:
        r.completed_at = start + timedelta(seconds=seconds)
    return r


def test_task_event_defaults():
    e = TaskEvent("t", "start")
    assert e.data == {} and isinstance(e.timestamp, datetime)


def test_hooks_record_events_and_call_callbacks_in_order():
    calls = []
    hooks = HooksMiddleware(
        on_start=lambda tid, **kw: calls.append(
            ("start", tid, kw["func_name"], kw["args"])
        ),
        on_complete=lambda tid, **kw: calls.append(("complete", tid, kw["result"])),
        on_error=lambda tid, **kw: calls.append(("error", tid, str(kw["error"]))),
    )
    hooks.before_compute(work, (3,), {}, "k")
    hooks.after_compute("k", _result(3, seconds=2))
    hooks.on_error("k2", ValueError("nope"))
    assert calls == [
        ("start", "k", "work", (3,)),
        ("complete", "k", 3),
        ("error", "k2", "nope"),
    ]
    events = hooks.get_events()
    assert [e.event_type for e in events] == ["start", "complete", "error"]
    assert events[1].data["duration"] == 2.0
    assert events[2].data == {"error": "nope", "error_type": "ValueError"}
    events.clear()  # a copy: the middleware's own record is untouched
    assert len(hooks.get_events()) == 3


def test_hooks_without_callbacks_still_record_events():
    hooks = HooksMiddleware()
    hooks.before_compute(work, (), {}, "k")
    hooks.after_compute("k", _result())
    hooks.on_error("k", RuntimeError("x"))
    assert len(hooks.get_events()) == 3


def test_metrics_count_functions_statuses_and_durations():
    m = MetricsCollectorMiddleware()
    assert m.get_metrics()["total_tasks"] == 0 and "duration" not in m.get_metrics()
    for key, secs in [("a", 1), ("b", 3)]:
        m.before_compute(work, (), {}, key)
        m.after_compute(key, _result(seconds=secs))
    m.before_compute(work, (), {}, "c")
    m.on_error("c", ValueError())
    got = m.get_metrics()
    assert (
        got["total_tasks"] == 3
        and got["successful_tasks"] == 2
        and got["failed_tasks"] == 1
    )
    assert got["function_counts"] == {"work": 3}
    assert got["duration"] == {
        "count": 2,
        "min": 1.0,
        "max": 3.0,
        "avg": 2.0,
        "total": 4.0,
    }


def test_metrics_ignore_results_without_a_duration():
    m = MetricsCollectorMiddleware()
    m.after_compute("a", ComputationResult(1, ComputationStatus.COMPLETED))
    assert m.get_metrics()["successful_tasks"] == 1


def test_tracing_spans_succeed_or_fail_and_unknown_keys_are_ignored(caplog):
    t = TracingMiddleware()
    with caplog.at_level(logging.DEBUG):
        t.before_compute(work, (), {}, "ok")
        t.after_compute("ok", _result(seconds=1))
        t.before_compute(work, (), {}, "bad")
        t.on_error("bad", ValueError("boom"))
        t.after_compute("unknown", _result())
        t.on_error("unknown", ValueError())
    ok, bad = t._spans["ok"], t._spans["bad"]
    assert (
        ok["status"] == "success"
        and ok["duration"] == 1.0
        and ok["func_name"] == "work"
    )
    assert bad["status"] == "error" and bad["error"] == "boom"
    assert ok["trace_id"] != bad["trace_id"] and "unknown" not in t._spans


def test_composite_forwards_every_stage_to_every_middleware():
    a, b = HooksMiddleware(), MetricsCollectorMiddleware()
    c = CompositeMiddleware([a, b])
    c.before_compute(work, (), {}, "k")
    c.after_compute("k", _result())
    c.on_error("k", ValueError())
    assert len(a.get_events()) == 3
    assert b.get_metrics()["total_tasks"] == 2


def test_observability_stack_composition():
    only_logging = create_observability_middleware(enable_metrics=False)
    assert isinstance(only_logging, LoggingMiddleware)

    default = create_observability_middleware()
    assert isinstance(default, CompositeMiddleware)
    assert [type(m) for m in default.middlewares] == [
        LoggingMiddleware,
        MetricsCollectorMiddleware,
    ]

    full = create_observability_middleware(
        enable_tracing=True, on_start=lambda tid, **kw: None
    )
    assert [type(m) for m in full.middlewares] == [
        LoggingMiddleware,
        HooksMiddleware,
        MetricsCollectorMiddleware,
        TracingMiddleware,
    ]
