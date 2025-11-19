"""Tests for configuration module."""

import os
import pytest
from pathlib import Path
import tempfile

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
    with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
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
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
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
