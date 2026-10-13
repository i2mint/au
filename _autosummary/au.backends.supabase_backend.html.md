# au.backends.supabase_backend

Supabase PostgreSQL queue backend for au framework.

This module provides SupabaseQueueBackend for distributed task queuing using
Supabase PostgreSQL as a simulated queue. It requires the ‘supabase’ package.

### Classes

| [`SupabaseQueueBackend`](#au.backends.supabase_backend.SupabaseQueueBackend)([store, ...])   | Backend that uses Supabase PostgreSQL as a simulated task queue.   |
|---------------------------------------------------------------------------------------|--------------------------------------------------------------------|

### *class* au.backends.supabase_backend.SupabaseQueueBackend(store=None, supabase_client=None, queue_table_name='au_task_queue', max_concurrent_tasks=2, polling_interval_seconds=1.0, middleware=None)

Bases: [`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)

Backend that uses Supabase PostgreSQL as a simulated task queue.

This backend stores tasks in a PostgreSQL table and uses an internal polling
worker to process them. This is a simulated queue and may have different
performance characteristics than dedicated message brokers.

Required table schema:
: CREATE TABLE au_task_queue (
  : task_id UUID PRIMARY KEY,
    func_data BYTEA NOT NULL,
    status TEXT NOT NULL DEFAULT ‘pending’,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    worker_id TEXT
  <br/>
  );

### Example

```pycon
>>> from supabase import create_client
>>> supabase = create_client(url, key)
>>> backend = SupabaseQueueBackend(store, supabase)
```

#### launch(func, args, kwargs, key, store=None)

Enqueue task to Supabase PostgreSQL table.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### shutdown()

Shutdown the polling workers.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### terminate(key)

Attempt to terminate a task by marking it as failed.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)
