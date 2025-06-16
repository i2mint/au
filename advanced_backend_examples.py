#!/usr/bin/env python3
"""
Advanced examples showing RQ and Supabase backends (require external dependencies)
"""


def demo_rq_usage():
    """Show how to use RQ backend (requires: pip install redis rq)"""
    print("=== RQ Backend Usage Example ===")
    print("To use this backend:")
    print("1. Install dependencies: pip install redis rq")
    print("2. Start Redis server: redis-server")
    print("3. Start RQ worker: rq worker au_tasks")
    print()

    code_example = '''
import redis
from rq import Queue
from au.backends.rq_backend import RQBackend
from au.base import async_compute, FileSystemStore

# Setup
redis_conn = redis.Redis(host='localhost', port=6379, db=0)
rq_queue = Queue('au_tasks', connection=redis_conn)
store = FileSystemStore("/tmp/au_computations")
backend = RQBackend(store, rq_queue)

# Define work function (must be at module level for pickling)
def expensive_computation(n):
    import time
    time.sleep(n)  # Simulate work
    return f"Processed {n} items"

# Use it
compute_async = async_compute(backend=backend, store=store)(expensive_computation)
handle = compute_async(5)

print(f"Task queued: {handle.key}")
# Worker will process this task
result = handle.get_result(timeout=60)
print(f"Result: {result}")
'''
    print(code_example)


def demo_supabase_usage():
    """Show how to use Supabase backend (requires: pip install supabase)"""
    print("=== Supabase Backend Usage Example ===")
    print("To use this backend:")
    print("1. Install dependency: pip install supabase")
    print("2. Create Supabase project and get URL/key")
    print("3. Create the au_task_queue table (see README)")
    print()

    sql_schema = '''
-- Required SQL schema for Supabase:
CREATE TABLE au_task_queue (
    task_id UUID PRIMARY KEY,
    func_data BYTEA NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    worker_id TEXT
);
'''

    code_example = '''
from supabase import create_client
from au.backends.supabase_backend import SupabaseQueueBackend
from au.base import async_compute, FileSystemStore

# Setup Supabase
SUPABASE_URL = "your-project-url"
SUPABASE_KEY = "your-anon-key"
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Create backend with internal workers
store = FileSystemStore("/tmp/au_computations")
with SupabaseQueueBackend(
    store, 
    supabase,
    max_concurrent_tasks=2,
    polling_interval_seconds=1.0
) as backend:
    
    def data_analysis(dataset_path):
        import pandas as pd
        df = pd.read_csv(dataset_path)
        return {"rows": len(df), "columns": len(df.columns)}
    
    # Use it
    analyze_async = async_compute(backend=backend, store=store)(data_analysis)
    handle = analyze_async("/path/to/data.csv")
    
    # Backend's internal workers will process this
    result = handle.get_result(timeout=300)
    print(f"Analysis result: {result}")
'''

    print("SQL Schema:")
    print(sql_schema)
    print("Python Code:")
    print(code_example)


def demo_backend_selection():
    """Guide for choosing the right backend"""
    print("=== Backend Selection Guide ===")

    guide = '''
Choose your backend based on your needs:

1. **ProcessBackend (default)**
   - Good for: Single-machine, development, simple cases
   - Pros: No setup, works out of box
   - Cons: Not persistent, single machine only

2. **StdLibQueueBackend** 
   - Good for: Better resource management, testing queuing
   - Pros: No external dependencies, threads or processes
   - Cons: Not persistent, single machine only

3. **RQBackend**
   - Good for: Production distributed systems, high throughput
   - Pros: Battle-tested, great tooling, Redis ecosystem
   - Cons: Requires Redis, separate worker processes

4. **SupabaseQueueBackend**
   - Good for: PostgreSQL-based apps, simple distributed setup
   - Pros: SQL-based monitoring, integrated with Supabase
   - Cons: Polling-based (not as efficient as push-based)

Performance recommendations:
- CPU-bound tasks: ProcessBackend or StdLibQueueBackend(use_processes=True)
- I/O-bound tasks: StdLibQueueBackend(use_processes=False) 
- Distributed: RQBackend for high throughput, SupabaseQueueBackend for simplicity
- Development: ProcessBackend (default)
- Testing: StdLibQueueBackend
'''
    print(guide)


if __name__ == "__main__":
    demo_rq_usage()
    print("\n" + "=" * 60 + "\n")
    demo_supabase_usage()
    print("\n" + "=" * 60 + "\n")
    demo_backend_selection()
