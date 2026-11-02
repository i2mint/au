"""Tests for configuration module."""

import logging
import os
import pytest
from pathlib import Path
import tempfile

from au.testing import InMemoryStore
from au.config import (
    AUConfig,
    get_config,
    load_config_from_env,
    load_config_from_file,
    get_global_config,
    set_global_config,
    reset_global_config,
)


def test_default_config():
    """Test default configuration."""
    config = AUConfig()
    assert config.backend == "thread"
    assert config.storage == "filesystem"
    assert config.ttl_seconds == 3600
    assert config.max_workers == 4


def test_env_config(monkeypatch):
    """Test loading configuration from environment variables."""
    monkeypatch.setenv("AU_BACKEND", "redis")
    monkeypatch.setenv("AU_REDIS_URL", "redis://localhost:6379")
    monkeypatch.setenv("AU_MAX_WORKERS", "8")
    monkeypatch.setenv("AU_TTL_SECONDS", "7200")
    monkeypatch.setenv("AU_RETRY_ENABLED", "true")

    env_config = load_config_from_env()

    assert env_config["backend"] == "redis"
    assert env_config["redis_url"] == "redis://localhost:6379"
    assert env_config["max_workers"] == 8
    assert env_config["ttl_seconds"] == 7200
    assert env_config["retry_enabled"] is True


def test_get_config_with_overrides():
    """Test getting config with explicit overrides."""
    config = get_config(backend="process", max_workers=16)

    assert config.backend == "process"
    assert config.max_workers == 16


def test_toml_config_file():
    """Test loading configuration from TOML file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        f.write("""
[au]
backend = "redis"
max_workers = 10
ttl_seconds = 1800
retry_enabled = true
""")
        toml_path = Path(f.name)

    try:
        file_config = load_config_from_file(toml_path)

        assert file_config["backend"] == "redis"
        assert file_config["max_workers"] == 10
        assert file_config["ttl_seconds"] == 1800
        assert file_config["retry_enabled"] is True

    finally:
        toml_path.unlink()


def test_json_config_file():
    """Test loading configuration from JSON file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write("""
{
    "au": {
        "backend": "process",
        "max_workers": 12
    }
}
""")
        json_path = Path(f.name)

    try:
        file_config = load_config_from_file(json_path)

        assert file_config["backend"] == "process"
        assert file_config["max_workers"] == 12

    finally:
        json_path.unlink()


def test_global_config():
    """Test global configuration management."""
    reset_global_config()

    # Get global config (creates default)
    config1 = get_global_config()
    assert isinstance(config1, AUConfig)

    # Should return same instance
    config2 = get_global_config()
    assert config1 is config2

    # Set custom global config
    custom_config = AUConfig(backend="redis", max_workers=20)
    set_global_config(custom_config)

    config3 = get_global_config()
    assert config3 is custom_config
    assert config3.backend == "redis"
    assert config3.max_workers == 20

    # Reset
    reset_global_config()


# --- i2mint/au#22: the observability settings reach the simple API ------------


@pytest.fixture
def isolated_config(monkeypatch, tmp_path):
    """Only the environment the test sets: no ambient AU_* nor config file."""
    import au.api

    for name in [n for n in os.environ if n.startswith("AU_")]:
        monkeypatch.delenv(name)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))  # Path.home() on Windows
    monkeypatch.setenv("AU_BACKEND", "thread")
    monkeypatch.setattr(au.api, "_default_backend", None)
    reset_global_config()
    yield monkeypatch
    reset_global_config()


def test_au_logging_level_sets_the_level_tasks_are_logged_at(isolated_config, caplog):
    import au.api

    isolated_config.setenv("AU_LOGGING_LEVEL", "DEBUG")
    with caplog.at_level(logging.DEBUG, logger="au.tasks"):
        store = InMemoryStore()
        key = au.api.submit_task(abs, -3, store=store)
        assert au.api.get_result(key, timeout=5, store=store) == 3
    starts = [r for r in caplog.records if "Starting computation" in r.message]
    assert [r.levelno for r in starts] == [logging.DEBUG]


def test_au_logging_enabled_false_logs_nothing(isolated_config):
    import au.api

    isolated_config.setenv("AU_LOGGING_ENABLED", "false")
    assert au.api._get_default_backend().middleware == []


def test_a_bad_logging_level_is_refused_by_its_setting_name(isolated_config):
    import au.api

    isolated_config.setenv("AU_LOGGING_LEVEL", "loud")
    with pytest.raises(ValueError, match="AU_LOGGING_LEVEL.*'loud'"):
        au.api._get_default_backend()


def test_task_logs_stay_quiet_unless_configured_but_au_errors_do_not():
    import au.api

    def has_null_handler(name):
        return any(
            isinstance(h, logging.NullHandler) for h in logging.getLogger(name).handlers
        )

    assert has_null_handler(au.api.TASKS_LOGGER)
    assert not has_null_handler("au") and not has_null_handler("au.base")


# --- i2mint/au#25: every setting does something, or is gone -------------------


@pytest.fixture
def configured(monkeypatch):
    """Install a global config for the test, then restore the default, and
    keep the default backend and store it may build from leaking out."""
    import au.api

    monkeypatch.setattr(au.api, "_default_backend", None)
    monkeypatch.setattr(au.api, "_default_store", None)

    def install(**settings):
        set_global_config(AUConfig(**settings))

    yield install
    reset_global_config()


def _flaky_until(attempts):
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) < attempts:
            raise ConnectionError("not yet")
        return len(calls)

    return flaky, calls


def _submit_and_wait(func, **kwargs):
    from au.api import get_result, submit_task
    from au.base import ThreadBackend

    store = InMemoryStore()
    task_id = submit_task(func, backend=ThreadBackend(), store=store, **kwargs)
    return lambda: get_result(task_id, timeout=5, store=store)


def test_retry_settings_make_submit_task_retry(configured):
    configured(retry_enabled=True, retry_max_attempts=3, retry_initial_delay=0)
    flaky, calls = _flaky_until(3)
    assert _submit_and_wait(flaky)() == 3


def test_without_retry_settings_submit_task_runs_once(configured):
    configured()
    flaky, calls = _flaky_until(2)
    with pytest.raises(Exception, match="not yet"):
        _submit_and_wait(flaky)()
    assert len(calls) == 1


def test_a_retry_policy_passed_overrides_the_settings(configured):
    from au.retry import RetryPolicy

    configured(retry_enabled=True, retry_max_attempts=5, retry_initial_delay=0)
    flaky, calls = _flaky_until(3)
    with pytest.raises(Exception, match="not yet"):
        _submit_and_wait(flaky, retry_policy=RetryPolicy(max_attempts=1))()
    assert len(calls) == 1


def test_a_bad_backoff_setting_is_refused_by_name_before_the_key_is_reserved(
    configured,
):
    from au.api import submit_task
    from au.base import ThreadBackend

    configured(retry_enabled=True, retry_backoff="fibonacci")
    store = InMemoryStore()
    with pytest.raises(ValueError, match="AU_RETRY_BACKOFF"):
        submit_task(int, key="job", backend=ThreadBackend(), store=store)
    assert "job" not in store


@pytest.mark.parametrize("framework", ["fastapi", "flask"])
def test_serve_defaults_to_the_http_settings(configured, framework):
    pytest.importorskip(framework)
    from au.http import serve

    configured(http_host="0.0.0.0", http_port=9123, http_framework=framework)
    served = {}

    def run(app, *, host, port):
        served.update(app=app, host=host, port=port)

    from au.base import ThreadBackend

    serve([int], run=run, backend=ThreadBackend(), store=InMemoryStore())
    assert (served["host"], served["port"]) == ("0.0.0.0", 9123)
    module = type(served["app"]).__module__.split(".")[0]
    assert module == framework
    serve([int], port=80, run=run, backend=ThreadBackend(), store=InMemoryStore())
    assert served["port"] == 80  # an argument beats the setting


def test_serve_refuses_an_unknown_framework_by_its_setting(configured):
    from au.http import serve

    configured(http_framework="starlette")
    with pytest.raises(ValueError, match="AU_HTTP_FRAMEWORK"):
        serve([int], run=lambda app, **kw: None)


def test_the_removed_metrics_setting_warns_instead_of_breaking(monkeypatch, tmp_path):
    monkeypatch.setenv("AU_METRICS_ENABLED", "true")
    with pytest.warns(FutureWarning, match="metrics_enabled"):
        config = get_config(config_file=tmp_path / "absent.toml")
    assert not hasattr(config, "metrics_enabled")
    monkeypatch.delenv("AU_METRICS_ENABLED")
    file = tmp_path / "au.toml"
    file.write_text("[au]\nmetrics_enabled = true\nmax_workers = 2\n")
    with pytest.warns(FutureWarning, match="metrics_enabled"):
        assert get_config(config_file=file).max_workers == 2


def test_retry_settings_keep_the_task_logging_its_name(configured, caplog):
    """The retried function kept no ``__name__``: LoggingMiddleware failed on
    every task once retry was enabled."""
    from au.api import get_result, submit_task
    from au.base import LoggingMiddleware, ThreadBackend

    configured(retry_enabled=True, retry_initial_delay=0)
    store = InMemoryStore()
    backend = ThreadBackend(middleware=[LoggingMiddleware(level=logging.INFO)])
    with caplog.at_level(logging.INFO):
        task_id = submit_task(_flaky_until(1)[0], backend=backend, store=store)
        assert get_result(task_id, timeout=5, store=store) == 1
    assert "flaky" in caplog.text
    assert "failed" not in caplog.text


@pytest.mark.parametrize(
    "setting, value",
    [
        ("retry_max_attempts", 0),
        ("retry_max_attempts", -1),
        ("retry_initial_delay", -1),
    ],
)
def test_a_retry_setting_that_would_skip_or_break_the_task_is_refused(
    configured, setting, value
):
    configured(retry_enabled=True, **{setting: value})
    with pytest.raises(ValueError, match=f"AU_{setting.upper()}"):
        _submit_and_wait(int)


def test_serve_blames_an_argument_not_the_setting(configured):
    from au.http import serve

    configured()
    with pytest.raises(ValueError, match="^framework is 'bottle'"):
        serve([int], framework="bottle", run=lambda app, **kw: None)
