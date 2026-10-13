# au.retry

Retry policies and error handling for AU.

Provides configurable retry strategies with backoff policies.

### Functions

| [`retry_with_policy`](#au.retry.retry_with_policy)(func[, args, kwargs, policy])   | Execute a function with retry policy.   |
|----------------------------------------------------------------------------------------------------|-----------------------------------------|

### Classes

| [`BackoffStrategy`](#au.retry.BackoffStrategy)(\*values)                    | Backoff strategy for retries.     |
|-----------------------------------------------------------------------------------------------|-----------------------------------|
| [`RetryPolicy`](#au.retry.RetryPolicy)([max_attempts, backoff, ...])    | Configuration for retry behavior. |
| [`RetryState`](#au.retry.RetryState)([attempt_count, last_error, ...]) | State tracking for retries.       |

### Exceptions

| [`NonRetryableError`](#au.retry.NonRetryableError)   | Base class for errors that should not be retried.   |
|----------------------------------------------------------------------|-----------------------------------------------------|
| [`RetryableError`](#au.retry.RetryableError)      | Base class for errors that should be retried.       |

### *class* au.retry.BackoffStrategy(\*values)

Bases: [`str`](https://docs.python.org/3/builtins/stdtypes.html#str), [`Enum`](https://docs.python.org/3/library/enum.html#enum.Enum)

Backoff strategy for retries.

### *exception* au.retry.NonRetryableError

Bases: [`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)

Base class for errors that should not be retried.

### *class* au.retry.RetryPolicy(max_attempts=3, backoff=BackoffStrategy.EXPONENTIAL, initial_delay=1.0, max_delay=60.0, retry_on=<factory>, dont_retry_on=<factory>, on_retry=None)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

Configuration for retry behavior.

#### max_attempts

Maximum number of retry attempts (including initial try)

#### backoff

Backoff strategy (exponential, linear, constant)

#### initial_delay

Initial delay in seconds before first retry

#### max_delay

Maximum delay between retries

#### retry_on

List of exception types to retry on (empty means retry all)

#### dont_retry_on

List of exception types to never retry

#### on_retry

Optional callback called before each retry

#### get_delay(attempt)

Calculate delay before next retry.

* **Parameters:**
  **attempt** ([`int`](https://docs.python.org/3/builtins/functions.html#int)) – Current attempt number (1-indexed)
* **Return type:**
  [`float`](https://docs.python.org/3/builtins/functions.html#float)
* **Returns:**
  Delay in seconds

#### should_retry(attempt, error)

Determine if we should retry given an attempt number and error.

* **Parameters:**
  * **attempt** ([`int`](https://docs.python.org/3/builtins/functions.html#int)) – Current attempt number (1-indexed)
  * **error** ([`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)) – Exception that occurred
* **Return type:**
  [`bool`](https://docs.python.org/3/builtins/functions.html#bool)
* **Returns:**
  True if should retry, False otherwise

### *class* au.retry.RetryState(attempt_count=0, last_error=None, will_retry=False, next_retry_at=None, retry_history=<factory>)

Bases: [`object`](https://docs.python.org/3/builtins/functions.html#object)

State tracking for retries.

#### attempt_count

Number of attempts made

#### last_error

Last exception encountered

#### will_retry

Whether another retry will be attempted

#### next_retry_at

Timestamp of next retry attempt

#### retry_history

List of (timestamp, exception) tuples

#### add_attempt(error, will_retry, next_retry_at=None)

Record a retry attempt.

* **Parameters:**
  * **error** ([`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)) – Exception that occurred
  * **will_retry** ([`bool`](https://docs.python.org/3/builtins/functions.html#bool)) – Whether another retry will happen
  * **next_retry_at** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`float`](https://docs.python.org/3/builtins/functions.html#float)]) – Timestamp of next retry

### *exception* au.retry.RetryableError

Bases: [`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception)

Base class for errors that should be retried.

### au.retry.retry_with_policy(func, args=(), kwargs=None, policy=None)

Execute a function with retry policy.

* **Parameters:**
  * **func** ([`Callable`](https://docs.python.org/3/library/typing.html#typing.Callable)) – Function to execute
  * **args** ([`tuple`](https://docs.python.org/3/builtins/stdtypes.html#tuple)) – Positional arguments for function
  * **kwargs** ([`dict`](https://docs.python.org/3/builtins/stdtypes.html#dict)) – Keyword arguments for function
  * **policy** ([`Optional`](https://docs.python.org/3/library/typing.html#typing.Optional)[[`RetryPolicy`](#au.retry.RetryPolicy)]) – Retry policy (None means no retry)
* **Return type:**
  [`Any`](https://docs.python.org/3/library/typing.html#typing.Any)
* **Returns:**
  Function return value
* **Raises:**
  **Last exception if all retries exhausted** –
