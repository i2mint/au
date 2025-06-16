# au - Asynchronous Computation Framework

A Python framework for transforming synchronous functions into asynchronous ones with status tracking, result persistence, and pluggable backends.

## Features

- 🚀 **Simple decorator-based API** - Transform any function into an async computation
- 💾 **Pluggable storage backends** - File system, Redis, databases, etc.
- 🔄 **Multiple execution backends** - Processes, threads, distributed queues (RQ, Supabase)
- 🌐 **Queue backends** - Standard library, Redis Queue, Supabase PostgreSQL
- 🛡️ **Middleware system** - Logging, metrics, authentication, rate limiting
- 🧹 **Automatic cleanup** - TTL-based expiration of old results
- 📦 **Flexible serialization** - JSON, Pickle, or custom formats
- 🔍 **Status tracking** - Monitor computation state and progress
- ❌ **Cancellation support** - Stop long-running computations
- 🏭 **Distributed processing** - Scale across multiple machines

## Installation

```bash
pip install au
```

## Quick Start

```python
from au import async_compute
# For queue backends:
# from au import StdLibQueueBackend
# from au.backends.rq_backend import RQBackend
# from au.backends.supabase_backend import SupabaseQueueBackend

@async_compute()
def expensive_computation(n: int) -> int:
    """Calculate factorial."""
    result = 1
    for i in range(1, n + 1):
        result *= i
    return result

# Launch computation (returns immediately)
handle = expensive_computation(100)

# Check status
print(handle.get_status())  # ComputationStatus.RUNNING

# Get result (blocks with timeout)
result = handle.get_result(timeout=30)
print(f"100! = {result}")
```

## Use Cases

### 1. **Long-Running Computations**
Perfect for computations that take minutes or hours:
- Machine learning model training
- Data processing pipelines
- Scientific simulations
- Report generation

### 2. **Web Application Background Tasks**
Offload heavy work from request handlers:
```python
@app.route('/analyze')
def analyze_data():
    handle = analyze_large_dataset(request.files['data'])
    return {'job_id': handle.key}

@app.route('/status/<job_id>')
def check_status(job_id):
    handle = ComputationHandle(job_id, store)
    return {'status': handle.get_status().value}
```

### 3. **Distributed Computing**
Use queue backends to distribute work across multiple machines:
```python
# Using Redis Queue backend
import redis
from rq import Queue
from au.backends.rq_backend import RQBackend

redis_conn = redis.Redis()
rq_queue = Queue('tasks', connection=redis_conn)
backend = RQBackend(store, rq_queue)

@async_compute(backend=backend, store=store)
def distributed_task(data):
    return complex_analysis(data)

# Task will be processed by RQ workers on any machine
handle = distributed_task(large_dataset)
```

### 4. **Batch Processing**
Process multiple items with shared infrastructure:
```python
store = FileSystemStore("/var/computations", ttl_seconds=3600)
backend = ProcessBackend(store)

@async_compute(backend=backend, store=store)
def process_item(item_id):
    return transform_item(item_id)

# Launch multiple computations
handles = [process_item(i) for i in range(1000)]
```

## Usage Patterns

### Basic Usage

```python
from au import async_compute

# Simple async function with default settings
@async_compute()
def my_function(x):
    return x * 2

handle = my_function(21)
result = handle.get_result(timeout=10)  # Returns 42
```

### Custom Configuration

```python
from au import async_compute, FileSystemStore, ProcessBackend
from au import LoggingMiddleware, MetricsMiddleware, SerializationFormat

# Configure store with TTL and serialization
store = FileSystemStore(
    "/var/computations",
    ttl_seconds=3600,  # 1 hour TTL
    serialization=SerializationFormat.PICKLE  # For complex objects
)

# Add middleware
middleware = [
    LoggingMiddleware(level=logging.INFO),
    MetricsMiddleware()
]

# Create backend with middleware
backend = ProcessBackend(store, middleware=middleware)

# Apply to function
@async_compute(backend=backend, store=store)
def complex_computation(data):
    return analyze(data)
```

### Shared Infrastructure

```python
# Create shared components
store = FileSystemStore("/var/shared", ttl_seconds=7200)
backend = ProcessBackend(store)

# Multiple functions share the same infrastructure
@async_compute(backend=backend, store=store)
def step1(x):
    return preprocess(x)

@async_compute(backend=backend, store=store)
def step2(x):
    return transform(x)

# Chain computations
data = load_data()
h1 = step1(data)
preprocessed = h1.get_result(timeout=60)
h2 = step2(preprocessed)
final_result = h2.get_result(timeout=60)
```

### Temporary Computations

```python
from au import temporary_async_compute

# Automatic cleanup when context exits
with temporary_async_compute(ttl_seconds=60) as async_func:
    @async_func
    def quick_job(x):
        return x ** 2
    
    handle = quick_job(10)
    result = handle.get_result(timeout=5)
    # Temporary directory cleaned up automatically
```

### Thread Backend for I/O-Bound Tasks

```python
from au import ThreadBackend

# Use threads for I/O-bound operations
store = FileSystemStore("/tmp/io_tasks")
backend = ThreadBackend(store)

@async_compute(backend=backend, store=store)
def fetch_data(url):
    return requests.get(url).json()

# Launch multiple I/O operations
handles = [fetch_data(url) for url in urls]
```

## Queue Backends

The AU framework supports multiple queue backends for different distributed computing scenarios:

### Standard Library Queue Backend

Uses Python's `concurrent.futures` for in-memory task processing with no external dependencies.

```python
from au import StdLibQueueBackend

store = FileSystemStore("/tmp/computations")

# Use ThreadPoolExecutor for I/O-bound tasks
with StdLibQueueBackend(store, max_workers=4, use_processes=False) as backend:
    @async_compute(backend=backend, store=store)
    def fetch_data(url):
        return requests.get(url).text

# Use ProcessPoolExecutor for CPU-bound tasks  
with StdLibQueueBackend(store, max_workers=4, use_processes=True) as backend:
    @async_compute(backend=backend, store=store)
    def cpu_intensive(n):
        return sum(i * i for i in range(n))
```

**Features:**
- No external dependencies
- Context manager support for clean shutdown
- Choice between threads and processes
- In-memory queuing (not persistent)

### Redis Queue (RQ) Backend

Distributed task processing using Redis and RQ workers.

**Installation:**
```bash
pip install redis rq
```

**Usage:**
```python
import redis
from rq import Queue
from au.backends.rq_backend import RQBackend

# Setup Redis and RQ
redis_conn = redis.Redis(host='localhost', port=6379, db=0)
rq_queue = Queue('au_tasks', connection=redis_conn)

# Create backend
store = FileSystemStore("/tmp/computations")
backend = RQBackend(store, rq_queue)

@async_compute(backend=backend, store=store)
def heavy_computation(data):
    # This will be processed by RQ workers
    return process_data(data)

# Launch task (enqueued to Redis)
handle = heavy_computation(my_data)

# Start RQ worker in separate process/machine:
# rq worker au_tasks
```

**Features:**
- Distributed processing across multiple machines
- Persistent task queue (survives restarts)
- Built-in job monitoring and management
- Fault tolerance and retry mechanisms

### Supabase Queue Backend

PostgreSQL-based task queue using Supabase with internal polling workers.

**Installation:**
```bash
pip install supabase
```

**Database Setup:**
```sql
CREATE TABLE au_task_queue (
    task_id UUID PRIMARY KEY,
    func_data BYTEA NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    worker_id TEXT
);
```

**Usage:**
```python
from supabase import create_client
from au.backends.supabase_backend import SupabaseQueueBackend

# Setup Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Create backend with internal polling workers
store = FileSystemStore("/tmp/computations")
with SupabaseQueueBackend(
    store, 
    supabase, 
    max_concurrent_tasks=3,
    polling_interval_seconds=2.0
) as backend:
    
    @async_compute(backend=backend, store=store)
    def analyze_data(dataset_id):
        return run_analysis(dataset_id)
    
    handle = analyze_data("dataset_123")
    result = handle.get_result(timeout=60)
```

**Features:**
- PostgreSQL-based persistence
- Internal polling workers (no separate worker processes needed)
- SQL-based task management and monitoring
- Integration with Supabase ecosystem

### Backend Comparison

| Backend | Persistence | Distribution | Setup Complexity | Best For |
|---------|-------------|--------------|------------------|----------|
| ProcessBackend | No | Single machine | Low | Development, single-machine processing |
| StdLibQueueBackend | No | Single machine | Low | Simple queuing, testing |
| RQBackend | Yes | Multi-machine | Medium | Production distributed systems |
| SupabaseQueueBackend | Yes | Multi-machine | Medium | PostgreSQL-based architectures |

### Function Serialization Requirements

Queue backends require functions to be **pickleable**:

✅ **Good:**
```python
# Module-level function
def my_task(x):
    return x * 2

@async_compute(backend=queue_backend)
def another_task(data):
    return process(data)
```

❌ **Bad:**
```python
def test_function():
    # Local function - can't be pickled!
    @async_compute(backend=queue_backend)
    def local_task(x):
        return x * 2
```

## Architecture & Design

### Core Components

1. **Storage Abstraction (`ComputationStore`)**
   - Implements Python's `MutableMapping` interface
   - Handles result persistence and retrieval
   - Supports TTL-based expiration
   - Extensible for any storage backend

2. **Execution Abstraction (`ComputationBackend`)**
   - Defines how computations are launched
   - Supports different execution models
   - Integrates middleware for cross-cutting concerns

3. **Result Handling (`ComputationHandle`)**
   - Clean API for checking status and retrieving results
   - Supports timeouts and cancellation
   - Provides access to metadata

4. **Middleware System**
   - Lifecycle hooks: before, after, error
   - Composable and reusable
   - Examples: logging, metrics, auth, rate limiting

### Design Principles

- **Separation of Concerns**: Storage, execution, and result handling are independent
- **Dependency Injection**: All components are injected, avoiding hardcoded dependencies
- **Open/Closed Principle**: Extend functionality without modifying core code
- **Standard Interfaces**: Uses Python's `collections.abc` interfaces
- **Functional Approach**: Decorator-based API preserves function signatures

### Trade-offs & Considerations

#### Pros
- ✅ Clean abstraction allows easy swapping of implementations
- ✅ Type hints and dataclasses provide excellent IDE support
- ✅ Follows SOLID principles for maintainability
- ✅ Minimal dependencies (uses only Python stdlib)
- ✅ Flexible serialization supports complex objects
- ✅ Middleware enables cross-cutting concerns

#### Cons
- ❌ Process-based backend has overhead for small computations
- ❌ File-based storage might not scale for high throughput
- ❌ Metrics middleware doesn't share state across processes by default
- ❌ No built-in distributed coordination
- ❌ Fork method required for ProcessBackend (platform-specific)

#### When to Use
- ✅ Long-running computations (minutes to hours)
- ✅ Need to persist results across restarts
- ✅ Want to separate computation from result retrieval
- ✅ Building async APIs or job queues
- ✅ Need cancellation or timeout support

#### When NOT to Use
- ❌ Sub-second computations (overhead too high)
- ❌ Need distributed coordination (use Celery/Dask)
- ❌ Require complex workflow orchestration
- ❌ Need real-time streaming results

## Advanced Features

### Custom Middleware

```python
from au import Middleware

class RateLimitMiddleware(Middleware):
    def __init__(self, max_per_minute: int = 60):
        self.max_per_minute = max_per_minute
        self.requests = []
    
    def before_compute(self, func, args, kwargs, key):
        now = time.time()
        self.requests = [t for t in self.requests if now - t < 60]
        
        if len(self.requests) >= self.max_per_minute:
            raise Exception("Rate limit exceeded")
        
        self.requests.append(now)
    
    def after_compute(self, key, result):
        pass
    
    def on_error(self, key, error):
        pass

# Use the middleware
@async_compute(middleware=[RateLimitMiddleware(max_per_minute=10)])
def rate_limited_function(x):
    return expensive_api_call(x)
```

### Custom Storage Backend

```python
from au import ComputationStore, ComputationResult
import redis

class RedisStore(ComputationStore):
    def __init__(self, redis_client, *, ttl_seconds=None):
        super().__init__(ttl_seconds=ttl_seconds)
        self.redis = redis_client
    
    def create_key(self):
        return f"computation:{uuid.uuid4()}"
    
    def __getitem__(self, key):
        data = self.redis.get(key)
        if data is None:
            return ComputationResult(None, ComputationStatus.PENDING)
        return pickle.loads(data)
    
    def __setitem__(self, key, result):
        data = pickle.dumps(result)
        if self.ttl_seconds:
            self.redis.setex(key, self.ttl_seconds, data)
        else:
            self.redis.set(key, data)
    
    def __delitem__(self, key):
        self.redis.delete(key)
    
    def __iter__(self):
        return iter(self.redis.scan_iter("computation:*"))
    
    def __len__(self):
        return len(list(self))
    
    def cleanup_expired(self):
        # Redis handles expiration automatically
        return 0

# Use Redis backend
redis_client = redis.Redis(host='localhost', port=6379)
store = RedisStore(redis_client, ttl_seconds=3600)

@async_compute(store=store)
def distributed_computation(x):
    return process(x)
```

### Monitoring & Metrics

```python
from au import MetricsMiddleware

# Create shared metrics
metrics = MetricsMiddleware()

@async_compute(middleware=[metrics])
def monitored_function(x):
    return compute(x)

# Launch several computations
for i in range(10):
    monitored_function(i)

# Check metrics
stats = metrics.get_stats()
print(f"Total: {stats['total']}")
print(f"Completed: {stats['completed']}")
print(f"Failed: {stats['failed']}")
print(f"Avg Duration: {stats['avg_duration']:.2f}s")
```

## Error Handling

```python
@async_compute()
def may_fail(x):
    if x < 0:
        raise ValueError("x must be positive")
    return x ** 2

handle = may_fail(-5)

try:
    result = handle.get_result(timeout=5)
except Exception as e:
    print(f"Computation failed: {e}")
    print(f"Status: {handle.get_status()}")  # ComputationStatus.FAILED
```

## Cleanup Strategies

```python
# Manual cleanup
@async_compute(ttl_seconds=3600)
def my_func(x):
    return x * 2

# Clean up expired results
removed = my_func.cleanup_expired()
print(f"Removed {removed} expired results")

# Automatic cleanup with probability
store = FileSystemStore(
    "/tmp/computations",
    ttl_seconds=3600,
    auto_cleanup=True,
    cleanup_probability=0.1  # 10% chance on each access
)
```

## API Reference

### Main Decorator

```python
@async_compute(
    backend=None,           # Execution backend (default: ProcessBackend)
    store=None,            # Storage backend (default: FileSystemStore)
    base_path="/tmp/computations",  # Path for default file store
    ttl_seconds=3600,      # Time-to-live for results
    serialization=SerializationFormat.JSON,  # JSON or PICKLE
    middleware=None        # List of middleware components
)
```

### ComputationHandle Methods

- `is_ready() -> bool`: Check if computation is complete
- `get_status() -> ComputationStatus`: Get current status
- `get_result(timeout=None) -> T`: Get result, optionally wait
- `cancel() -> bool`: Attempt to cancel computation
- `metadata -> Dict[str, Any]`: Access computation metadata

### ComputationStatus Enum

- `PENDING`: Not started yet
- `RUNNING`: Currently executing
- `COMPLETED`: Successfully finished
- `FAILED`: Failed with error

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details.