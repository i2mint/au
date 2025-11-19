# AU - Async Utils

**A lightweight, convention-over-configuration async framework for Python.**

AU makes async task execution simple and powerful. Transform any Python function into an async task with a single decorator, or use the simple API for even more flexibility.

## ✨ Features

- 🎯 **Convention Over Configuration** - Works out of the box with smart defaults
- 🚀 **Simple APIs** - Decorator pattern or direct function calls
- 💾 **Pluggable Storage** - Filesystem, in-memory, Redis, databases
- 🔄 **Multiple Backends** - Threads, processes, Redis Queue, Supabase
- 🌐 **HTTP Interface** - Built-in REST API with FastAPI or Flask
- 🔁 **Retry Logic** - Configurable retry policies with backoff
- 🔗 **Task Dependencies** - DAG-based workflow orchestration
- 🧪 **Testing Utilities** - Synchronous test backends and mocks
- 📊 **Observability** - Logging, metrics, tracing, and hooks
- 🛡️ **Zero Dependencies** - Core uses only Python stdlib

## 📦 Installation

```bash
# Core (no dependencies)
pip install au

# With HTTP support (FastAPI)
pip install au[http]

# With Redis backend
pip install au[redis]

# All features
pip install au[all]
```

## 🚀 Quick Start

### Simple Decorator Pattern

```python
from au import async_compute

@async_compute
def expensive_task(n: int) -> int:
    """This runs asynchronously!"""
    return sum(i * i for i in range(n))

# Launch task (returns immediately)
handle = expensive_task(1000000)

# Get result (blocks until complete)
result = handle.get_result(timeout=30)
print(f"Result: {result}")
```

### Simplified API (No Decorator Required)

```python
from au import submit_task, get_result

def my_function(x, y):
    return x + y

# Submit task
task_id = submit_task(my_function, 10, y=20)

# Get result
result = get_result(task_id, timeout=10)
print(f"Result: {result}")  # 30
```

### Context Manager Pattern

```python
from au import async_task

with async_task(expensive_task, 1000000) as handle:
    # Do other work while task runs
    print("Working...")

# Result ready here
print(f"Result: {handle.result}")
```

## 🎛️ Configuration

AU supports multiple configuration layers:

### 1. Environment Variables

```bash
export AU_BACKEND=redis
export AU_REDIS_URL=redis://localhost:6379
export AU_STORAGE_PATH=/var/au/tasks
export AU_TTL_SECONDS=7200
export AU_MAX_WORKERS=8
```

### 2. Config File (au.toml)

```toml
[au]
backend = "redis"
redis_url = "redis://localhost:6379"
storage_path = "/var/au/tasks"
ttl_seconds = 7200
max_workers = 8
```

### 3. Explicit Configuration

```python
from au import get_config, set_global_config

config = get_config(
    backend='redis',
    redis_url='redis://localhost:6379',
    max_workers=16
)
set_global_config(config)
```

## 🔄 Backends

### Thread Backend (Default)

```python
from au import async_compute

@async_compute  # Uses ThreadBackend by default
def io_bound_task(url):
    return requests.get(url).text
```

### Process Backend

```python
from au import async_compute, ProcessBackend

@async_compute(backend=ProcessBackend())
def cpu_bound_task(n):
    return sum(i * i for i in range(n))
```

### Redis Queue Backend

```python
from au import async_compute
from au.backends.rq_backend import RQBackend

backend = RQBackend(redis_url='redis://localhost:6379')

@async_compute(backend=backend)
def distributed_task(data):
    return process(data)
```

### Standard Library Queue Backend

```python
from au import StdLibQueueBackend, async_compute

backend = StdLibQueueBackend(max_workers=4, executor_type='thread')

@async_compute(backend=backend)
def task(x):
    return x * 2
```

## 🌐 HTTP Interface

Create a REST API for your tasks with one line:

```python
from au import async_compute
from au.http import mk_http_interface

@async_compute
def process_data(data: dict) -> dict:
    # Process data
    return {"result": data["value"] * 2}

# Create FastAPI app
app = mk_http_interface([process_data])

# Run with: uvicorn main:app
```

### API Endpoints

```bash
# Submit task
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"function_name": "process_data", "args": [], "kwargs": {"data": {"value": 5}}}'

# Get status
curl http://localhost:8000/tasks/{task_id}/status

# Get result (wait for completion)
curl http://localhost:8000/tasks/{task_id}/result?wait=true&timeout=30

# List all tasks
curl http://localhost:8000/tasks

# Cancel task
curl -X DELETE http://localhost:8000/tasks/{task_id}
```

## 🔁 Retry Logic

Add automatic retry with backoff:

```python
from au import async_compute, RetryPolicy, BackoffStrategy

retry_policy = RetryPolicy(
    max_attempts=5,
    backoff=BackoffStrategy.EXPONENTIAL,
    initial_delay=1.0,
    retry_on=[ConnectionError, TimeoutError],
)

from au.api import submit_task

task_id = submit_task(
    flaky_function,
    retry_policy=retry_policy
)
```

### Predefined Policies

```python
from au.retry import (
    DEFAULT_RETRY_POLICY,       # 3 attempts, exponential
    AGGRESSIVE_RETRY_POLICY,    # 5 attempts, fast
    CONSERVATIVE_RETRY_POLICY,  # 2 attempts, slow
    NETWORK_RETRY_POLICY,       # Retries network errors only
)
```

## 🔗 Task Dependencies & Workflows

Build complex workflows with dependencies:

```python
from au import TaskGraph

graph = TaskGraph()

# Define tasks
t1 = graph.add_task(fetch_data, 'source1')
t2 = graph.add_task(fetch_data, 'source2')
t3 = graph.add_task(merge_data, depends_on=[t1, t2])
t4 = graph.add_task(analyze, depends_on=[t3])

# Execute workflow
results = graph.execute()
print(results[t4])
```

### Fluent Builder API

```python
from au import WorkflowBuilder

workflow = (
    WorkflowBuilder()
    .add_task('fetch1', fetch_data, 'source1')
    .add_task('fetch2', fetch_data, 'source2')
    .add_task('merge', merge_data, depends_on=['fetch1', 'fetch2'])
    .add_task('analyze', analyze, depends_on=['merge'])
    .build()
)

results = workflow.execute()
```

## 📊 Observability

### Logging & Metrics

```python
from au import async_compute, LoggingMiddleware, MetricsMiddleware

@async_compute(middleware=[
    LoggingMiddleware(level='INFO'),
    MetricsMiddleware(),
])
def monitored_task(x):
    return x * 2
```

### Custom Hooks

```python
from au.hooks import create_observability_middleware

middleware = create_observability_middleware(
    logging_level='INFO',
    enable_metrics=True,
    enable_tracing=True,
    on_start=lambda task_id, **kw: print(f"Task {task_id} started"),
    on_complete=lambda task_id, **kw: print(f"Task {task_id} completed"),
    on_error=lambda task_id, error, **kw: print(f"Task {task_id} failed: {error}"),
)
```

## 🧪 Testing

AU provides synchronous test backends for easy testing:

```python
from au.testing import SyncTestBackend, InMemoryStore, mock_async

def test_my_task():
    backend = SyncTestBackend()
    store = InMemoryStore()

    # Tasks execute synchronously for testing
    from au import async_compute

    @async_compute(backend=backend, store=store)
    def task(x):
        return x * 2

    handle = task(5)
    assert handle.get_result() == 10
```

### Mocking Context Manager

```python
from au.testing import mock_async

def test_with_mock():
    with mock_async() as mock:
        @async_compute
        def task(x):
            return x * 2

        handle = task(5)

        assert mock.task_count == 1
        assert handle.get_result() == 10
```

## 📚 API Reference

### Core Functions

- `async_compute(backend=None, store=None, ...)` - Decorator for async tasks
- `submit_task(func, *args, **kwargs)` - Submit task without decorator
- `get_result(task_id, timeout=None)` - Get task result
- `get_status(task_id)` - Get task status
- `is_ready(task_id)` - Check if task is complete
- `cancel_task(task_id)` - Cancel running task
- `async_task(func, *args, **kwargs)` - Context manager for tasks

### Configuration

- `get_config(**overrides)` - Get configuration
- `get_global_config()` - Get global configuration
- `set_global_config(config)` - Set global configuration
- `AUConfig` - Configuration dataclass

### Retry

- `RetryPolicy(max_attempts, backoff, ...)` - Retry configuration
- `retry_with_policy(func, args, kwargs, policy)` - Execute with retry
- `BackoffStrategy` - EXPONENTIAL, LINEAR, CONSTANT

### Workflow

- `TaskGraph()` - Create task graph
- `WorkflowBuilder()` - Fluent workflow builder
- `depends_on(*funcs)` - Decorator for dependencies

### Testing

- `SyncTestBackend()` - Synchronous test backend
- `InMemoryStore()` - In-memory result store
- `mock_async()` - Context manager for testing
- `create_test_backend()` - Create test backend
- `create_test_store()` - Create test store

### HTTP

- `mk_http_interface(functions, ...)` - Create FastAPI app
- `mk_flask_interface(functions, ...)` - Create Flask app

## 🎯 Use Cases

### Web Application Background Tasks

```python
from flask import Flask, request, jsonify
from au import async_compute, submit_task, get_status, get_result

app = Flask(__name__)

@async_compute
def process_upload(file_path):
    # Heavy processing
    return analyze_file(file_path)

@app.route('/upload', methods=['POST'])
def upload():
    file_path = save_uploaded_file(request.files['file'])
    handle = process_upload(file_path)
    return jsonify({'task_id': handle.key})

@app.route('/status/<task_id>')
def status(task_id):
    return jsonify({'status': get_status(task_id).value})

@app.route('/result/<task_id>')
def result(task_id):
    try:
        result = get_result(task_id, timeout=0.1)
        return jsonify({'result': result})
    except TimeoutError:
        return jsonify({'status': 'pending'}), 202
```

### Distributed Data Processing

```python
from au import async_compute
from au.backends.rq_backend import RQBackend

backend = RQBackend(redis_url='redis://queue:6379')

@async_compute(backend=backend)
def process_chunk(data_chunk):
    return [transform(item) for item in data_chunk]

# Submit many tasks
chunks = split_data(large_dataset, chunk_size=1000)
handles = [process_chunk(chunk) for chunk in chunks]

# Collect results
results = [h.get_result() for h in handles]
final_result = merge_results(results)
```

### ML Model Training Pipeline

```python
from au import TaskGraph

def load_data():
    return load_dataset()

def preprocess(data):
    return clean_and_transform(data)

def train_model(data):
    return fit_model(data)

def evaluate(model):
    return compute_metrics(model)

# Build pipeline
graph = TaskGraph()
t1 = graph.add_task(load_data)
t2 = graph.add_task(preprocess, depends_on=[t1])
t3 = graph.add_task(train_model, depends_on=[t2])
t4 = graph.add_task(evaluate, depends_on=[t3])

results = graph.execute(timeout=3600)
metrics = results[t4]
```

## 🏗️ Architecture

AU follows a clean, modular architecture:

```
┌─────────────────────────────────────────┐
│          User Application               │
└─────────────────┬───────────────────────┘
                  │
      ┌───────────┴──────────┐
      │   Decorator/API       │
      │   (async_compute)     │
      └───────────┬───────────┘
                  │
      ┌───────────┴──────────┐
      │   ComputationHandle   │
      │   (Result tracking)   │
      └───────────┬───────────┘
                  │
      ┌───────────┴──────────┐
      │   Backend Layer       │
      │   (Execution)         │
      └───────────┬───────────┘
                  │
      ┌───────────┴──────────┐
      │   Storage Layer       │
      │   (Persistence)       │
      └───────────────────────┘
```

## 🤝 Contributing

Contributions are welcome! Please check out the [GitHub repository](https://github.com/i2mint/au).

## 📄 License

MIT License - see LICENSE file for details.

## 🔗 Related Projects

- **[qh](https://github.com/i2mint/qh)** - HTTP services built on AU
- **[i2mint](https://github.com/i2mint)** - Ecosystem of Python tools

## 📖 Documentation

For more detailed documentation, see the [docs folder](./docs) or visit our [documentation site](https://github.com/i2mint/au).

## 🎓 Examples

Check out the [examples folder](./examples) for more use cases:

- Simple tasks
- Web API integration
- Distributed processing
- Workflow orchestration
- Testing strategies

---

**Made with ❤️ by the i2mint team**
