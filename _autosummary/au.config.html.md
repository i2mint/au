# au.config

Configuration management for AU with convention-over-configuration support.

Supports configuration cascade:

1. Environment variables (AU_\*)
2. Config file (au.toml, au.yaml, .au.toml)
3. Explicit parameters

### Functions

| [`get_config`](#au.config.get_config)([config_file])     | Get configuration with cascade: env vars → config file → defaults → overrides.   |
|--------------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| [`get_global_config`](#au.config.get_global_config)()           | Get the global configuration instance.                                           |
| [`load_config_from_env`](#au.config.load_config_from_env)()        | Load configuration from environment variables.                                   |
| [`load_config_from_file`](#au.config.load_config_from_file)([path]) | Load configuration from file.                                                    |
| [`reset_global_config`](#au.config.reset_global_config)()         | Reset the global configuration to None.                                          |
| [`set_global_config`](#au.config.set_global_config)(config)     | Set the global configuration instance.                                           |

### Classes

| [`AUConfig`](#au.config.AUConfig)([backend, redis_url, supabase_url, ...])   | Configuration for AU with smart defaults.   |
|------------------------------------------------------------------------------------------------------|---------------------------------------------|

### *class* au.config.AUConfig(backend='thread', redis_url=None, supabase_url=None, supabase_key=None, max_workers=4, storage='filesystem', storage_path='/tmp/au_tasks', ttl_seconds=3600, serialization='json', retry_enabled=False, retry_max_attempts=3, retry_backoff='exponential', retry_initial_delay=1.0, logging_enabled=True, logging_level='INFO', metrics_enabled=False, http_host='127.0.0.1', http_port=8000, http_framework='fastapi')

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Configuration for AU with smart defaults.

### au.config.get_config(config_file=None, \*\*overrides)

Get configuration with cascade: env vars → config file → defaults → overrides.

* **Parameters:**
  * **config_file** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`Path`](https://docs.python.org/3/library/pathlib.html#pathlib.Path)]) – Optional path to configuration file
  * **\*\*overrides** – Explicit configuration overrides
* **Return type:**
  [`AUConfig`](#au.config.AUConfig)
* **Returns:**
  AUConfig instance with resolved configuration

### Example

```pycon
>>> config = get_config()  # Uses environment and defaults
>>> config = get_config(backend='redis', redis_url='redis://localhost')
>>> config = get_config(config_file=Path('custom.toml'))
```

### au.config.get_global_config()

Get the global configuration instance.

If not set, creates one from environment and config files.

* **Return type:**
  [`AUConfig`](#au.config.AUConfig)
* **Returns:**
  Global AUConfig instance

### au.config.load_config_from_env()

Load configuration from environment variables.

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
- AU_LOGGING_LEVEL: Level the simple API logs task start/completion at
  (a level name or number; not a filter: configure logging for that)
- AU_METRICS_ENABLED: Enable metrics (true/false)

* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]
* **Returns:**
  Dictionary of configuration values from environment

### au.config.load_config_from_file(path=None)

Load configuration from file.

Searches for configuration files in order:

1. Explicit path if provided
2. au.toml in current directory
3. .au.toml in current directory
4. au.yaml in current directory
5. .au.yaml in current directory
6. ~/.au/config.toml

* **Parameters:**
  **path** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`Path`](https://docs.python.org/3/library/pathlib.html#pathlib.Path)]) – Optional explicit path to config file
* **Return type:**
  [`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)[[`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)]
* **Returns:**
  Dictionary of configuration values from file

### au.config.reset_global_config()

Reset the global configuration to None.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

### au.config.set_global_config(config)

Set the global configuration instance.

* **Parameters:**
  **config** ([`AUConfig`](#au.config.AUConfig)) – AUConfig instance to use globally
* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)
