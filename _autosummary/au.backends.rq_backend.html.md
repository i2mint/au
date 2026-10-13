# au.backends.rq_backend

RQ (Redis Queue) backend for au framework.

This module provides RQBackend for distributed task queuing using Redis.
It requires the ‘rq’ and ‘redis’ packages to be installed.

### Classes

| [`RQBackend`](#au.backends.rq_backend.RQBackend)([store, rq_queue, middleware])   | Backend that uses RQ (Redis Queue) for distributed task processing.   |
|---------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|

### *class* au.backends.rq_backend.RQBackend(store=None, rq_queue=None, middleware=None)

Bases: [`ComputationBackend`](au.base.html.md#au.base.ComputationBackend)

Backend that uses RQ (Redis Queue) for distributed task processing.

This backend enqueues tasks to Redis and relies on RQ workers to process them.

### Example

```pycon
>>> import redis
>>> from rq import Queue
>>> redis_conn = redis.Redis()
>>> queue = Queue(connection=redis_conn)
>>> backend = RQBackend(store, queue)
```

#### launch(func, args, kwargs, key, store=None)

Enqueue computation task to RQ.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)

#### terminate(key)

Attempt to cancel RQ job by key.

* **Return type:**
  [`None`](https://docs.python.org/3/builtins/constants.html#None)
