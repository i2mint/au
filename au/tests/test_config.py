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
