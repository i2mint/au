"""
Configuration management for AU with convention-over-configuration support.

Supports configuration cascade:
1. Environment variables (AU_*)
2. Config file (au.toml, au.yaml, .au.toml)
3. Explicit parameters
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Any
import json


@dataclass
class AUConfig:
    """Configuration for AU with smart defaults."""

    # Backend configuration
    backend: str = "thread"  # thread, process, stdlib, redis, supabase
    redis_url: Optional[str] = None
    supabase_url: Optional[str] = None
    supabase_key: Optional[str] = None
    max_workers: int = 4

    # Storage configuration
    storage: str = "filesystem"  # filesystem, memory
    storage_path: str = "/tmp/au_tasks"
    ttl_seconds: int = 3600
    serialization: str = "json"  # json, pickle

    # Retry configuration
    retry_enabled: bool = False
    retry_max_attempts: int = 3
    retry_backoff: str = "exponential"  # exponential, linear, constant
    retry_initial_delay: float = 1.0

    # Observability
    logging_enabled: bool = True
    logging_level: str = "INFO"
    metrics_enabled: bool = False

    # HTTP configuration (for HTTP module)
    http_host: str = "127.0.0.1"
    http_port: int = 8000
    http_framework: str = "fastapi"  # fastapi, flask, starlette


def load_config_from_env() -> dict[str, Any]:
    """Load configuration from environment variables.

    Environment variables:
    - AU_BACKEND: Backend type (thread, process, stdlib, redis, supabase)
    - AU_REDIS_URL: Redis connection URL
    - AU_SUPABASE_URL: Supabase project URL
    - AU_SUPABASE_KEY: Supabase API key
    - AU_MAX_WORKERS: Maximum number of workers
    - AU_STORAGE: Storage type (filesystem, memory)
    - AU_STORAGE_PATH: Path for filesystem storage
    - AU_TTL_SECONDS: Time-to-live for results
    - AU_SERIALIZATION: Serialization format (json, pickle)
    - AU_RETRY_ENABLED: Enable retry (true/false)
    - AU_RETRY_MAX_ATTEMPTS: Maximum retry attempts
    - AU_LOGGING_LEVEL: Logging level
    - AU_METRICS_ENABLED: Enable metrics (true/false)

    Returns:
        Dictionary of configuration values from environment
    """
    config = {}

    # Simple string mappings
    env_mappings = {
        'AU_BACKEND': 'backend',
        'AU_REDIS_URL': 'redis_url',
        'AU_SUPABASE_URL': 'supabase_url',
        'AU_SUPABASE_KEY': 'supabase_key',
        'AU_STORAGE': 'storage',
        'AU_STORAGE_PATH': 'storage_path',
        'AU_SERIALIZATION': 'serialization',
        'AU_RETRY_BACKOFF': 'retry_backoff',
        'AU_LOGGING_LEVEL': 'logging_level',
        'AU_HTTP_HOST': 'http_host',
        'AU_HTTP_FRAMEWORK': 'http_framework',
    }

    for env_var, config_key in env_mappings.items():
        value = os.environ.get(env_var)
        if value is not None:
            config[config_key] = value

    # Integer mappings
    int_mappings = {
        'AU_MAX_WORKERS': 'max_workers',
        'AU_TTL_SECONDS': 'ttl_seconds',
        'AU_RETRY_MAX_ATTEMPTS': 'retry_max_attempts',
        'AU_HTTP_PORT': 'http_port',
    }

    for env_var, config_key in int_mappings.items():
        value = os.environ.get(env_var)
        if value is not None:
            try:
                config[config_key] = int(value)
            except ValueError:
                pass  # Ignore invalid values

    # Float mappings
    float_mappings = {
        'AU_RETRY_INITIAL_DELAY': 'retry_initial_delay',
    }

    for env_var, config_key in float_mappings.items():
        value = os.environ.get(env_var)
        if value is not None:
            try:
                config[config_key] = float(value)
            except ValueError:
                pass

    # Boolean mappings
    bool_mappings = {
        'AU_RETRY_ENABLED': 'retry_enabled',
        'AU_LOGGING_ENABLED': 'logging_enabled',
        'AU_METRICS_ENABLED': 'metrics_enabled',
    }

    for env_var, config_key in bool_mappings.items():
        value = os.environ.get(env_var)
        if value is not None:
            config[config_key] = value.lower() in ('true', '1', 'yes', 'on')

    return config


def load_config_from_file(path: Optional[Path] = None) -> dict[str, Any]:
    """Load configuration from file.

    Searches for configuration files in order:
    1. Explicit path if provided
    2. au.toml in current directory
    3. .au.toml in current directory
    4. au.yaml in current directory
    5. .au.yaml in current directory
    6. ~/.au/config.toml

    Args:
        path: Optional explicit path to config file

    Returns:
        Dictionary of configuration values from file
    """
    if path and path.exists():
        return _load_config_file(path)

    # Search common locations
    search_paths = [
        Path.cwd() / "au.toml",
        Path.cwd() / ".au.toml",
        Path.cwd() / "au.yaml",
        Path.cwd() / ".au.yaml",
        Path.home() / ".au" / "config.toml",
    ]

    for config_path in search_paths:
        if config_path.exists():
            return _load_config_file(config_path)

    return {}


def _load_config_file(path: Path) -> dict[str, Any]:
    """Load configuration from a specific file.

    Args:
        path: Path to configuration file

    Returns:
        Dictionary of configuration values
    """
    suffix = path.suffix.lower()

    try:
        if suffix == '.toml':
            # Try to use tomllib (Python 3.11+) or tomli
            try:
                import tomllib
                with open(path, 'rb') as f:
                    data = tomllib.load(f)
            except ImportError:
                try:
                    import tomli
                    with open(path, 'rb') as f:
                        data = tomli.load(f)
                except ImportError:
                    # Fallback to simple parsing for basic TOML
                    data = _simple_toml_parse(path)

            # Extract au section
            return data.get('au', data)

        elif suffix in ('.yaml', '.yml'):
            try:
                import yaml
                with open(path) as f:
                    data = yaml.safe_load(f)
                return data.get('au', data)
            except ImportError:
                # YAML requires external library
                return {}

        elif suffix == '.json':
            with open(path) as f:
                data = json.load(f)
            return data.get('au', data)

    except Exception:
        # If we can't load the file, return empty config
        return {}

    return {}


def _simple_toml_parse(path: Path) -> dict[str, Any]:
    """Simple TOML parser for basic key=value pairs.

    This is a fallback when tomllib/tomli are not available.
    Only handles simple [au] section with key=value pairs.

    Args:
        path: Path to TOML file

    Returns:
        Dictionary with 'au' section
    """
    result = {}
    current_section = None

    with open(path) as f:
        for line in f:
            line = line.strip()

            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue

            # Section header
            if line.startswith('[') and line.endswith(']'):
                current_section = line[1:-1].strip()
                if current_section not in result:
                    result[current_section] = {}
                continue

            # Key-value pair
            if '=' in line and current_section:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")

                # Try to parse value type
                if value.lower() in ('true', 'false'):
                    value = value.lower() == 'true'
                elif value.isdigit():
                    value = int(value)
                elif value.replace('.', '', 1).isdigit():
                    value = float(value)

                result[current_section][key] = value

    return result


def get_config(
    config_file: Optional[Path] = None,
    **overrides
) -> AUConfig:
    """Get configuration with cascade: env vars → config file → defaults → overrides.

    Args:
        config_file: Optional path to configuration file
        **overrides: Explicit configuration overrides

    Returns:
        AUConfig instance with resolved configuration

    Example:
        >>> config = get_config()  # Uses environment and defaults
        >>> config = get_config(backend='redis', redis_url='redis://localhost')
        >>> config = get_config(config_file=Path('custom.toml'))
    """
    # Start with defaults
    config_dict = {}

    # Layer 1: Environment variables
    config_dict.update(load_config_from_env())

    # Layer 2: Config file
    config_dict.update(load_config_from_file(config_file))

    # Layer 3: Explicit overrides
    config_dict.update({k: v for k, v in overrides.items() if v is not None})

    return AUConfig(**config_dict)


# Global configuration instance (can be modified)
_global_config: Optional[AUConfig] = None


def set_global_config(config: AUConfig) -> None:
    """Set the global configuration instance.

    Args:
        config: AUConfig instance to use globally
    """
    global _global_config
    _global_config = config


def get_global_config() -> AUConfig:
    """Get the global configuration instance.

    If not set, creates one from environment and config files.

    Returns:
        Global AUConfig instance
    """
    global _global_config
    if _global_config is None:
        _global_config = get_config()
    return _global_config


def reset_global_config() -> None:
    """Reset the global configuration to None."""
    global _global_config
    _global_config = None
