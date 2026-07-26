"""
Retry policies and error handling for AU.

Provides configurable retry strategies with backoff policies.
"""

import time
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable, Any, Type
from datetime import datetime, timedelta


class BackoffStrategy(str, Enum):
    """Backoff strategy for retries."""

    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    CONSTANT = "constant"


@dataclass
class RetryPolicy:
    """Configuration for retry behavior.

    Attributes:
        max_attempts: Maximum number of retry attempts (including initial try)
        backoff: Backoff strategy (exponential, linear, constant)
        initial_delay: Initial delay in seconds before first retry
        max_delay: Maximum delay between retries
        retry_on: List of exception types to retry on (empty means retry all)
        dont_retry_on: List of exception types to never retry
        on_retry: Optional callback called before each retry
    """

    max_attempts: int = 3
    backoff: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    initial_delay: float = 1.0
    max_delay: float = 60.0
    retry_on: list[Type[Exception]] = field(default_factory=list)
    dont_retry_on: list[Type[Exception]] = field(default_factory=list)
    on_retry: Optional[Callable[[int, Exception], None]] = None

    def should_retry(self, attempt: int, error: Exception) -> bool:
        """Determine if we should retry given an attempt number and error.

        Args:
            attempt: Current attempt number (1-indexed)
            error: Exception that occurred

        Returns:
            True if should retry, False otherwise
        """
        # Check if we've exceeded max attempts
        if attempt >= self.max_attempts:
            return False

        # Check if error is in dont_retry list
        if self.dont_retry_on:
            for exc_type in self.dont_retry_on:
                if isinstance(error, exc_type):
                    return False

        # Check if error is in retry list (if specified)
        if self.retry_on:
            for exc_type in self.retry_on:
                if isinstance(error, exc_type):
                    return True
            # If retry_on is specified and error not in it, don't retry
            return False

        # Default: retry all errors
        return True

    def get_delay(self, attempt: int) -> float:
        """Calculate delay before next retry.

        Args:
            attempt: Current attempt number (1-indexed)

        Returns:
            Delay in seconds
        """
        if self.backoff == BackoffStrategy.CONSTANT:
            delay = self.initial_delay

        elif self.backoff == BackoffStrategy.LINEAR:
            delay = self.initial_delay * attempt

        else:  # EXPONENTIAL
            delay = self.initial_delay * (2 ** (attempt - 1))

        # Cap at max_delay
        return min(delay, self.max_delay)


@dataclass
class RetryState:
    """State tracking for retries.

    Attributes:
        attempt_count: Number of attempts made
        last_error: Last exception encountered
        will_retry: Whether another retry will be attempted
        next_retry_at: Timestamp of next retry attempt
        retry_history: List of (timestamp, exception) tuples
    """

    attempt_count: int = 0
    last_error: Optional[Exception] = None
    will_retry: bool = False
    next_retry_at: Optional[float] = None
    retry_history: list[tuple[float, str]] = field(default_factory=list)

    def add_attempt(
        self, error: Exception, will_retry: bool, next_retry_at: Optional[float] = None
    ):
        """Record a retry attempt.

        Args:
            error: Exception that occurred
            will_retry: Whether another retry will happen
            next_retry_at: Timestamp of next retry
        """
        self.attempt_count += 1
        self.last_error = error
        self.will_retry = will_retry
        self.next_retry_at = next_retry_at
        self.retry_history.append((time.time(), str(error)))


def retry_with_policy(
    func: Callable,
    args: tuple = (),
    kwargs: dict = None,
    policy: Optional[RetryPolicy] = None,
) -> Any:
    """Execute a function with retry policy.

    Args:
        func: Function to execute
        args: Positional arguments for function
        kwargs: Keyword arguments for function
        policy: Retry policy (None means no retry)

    Returns:
        Function return value

    Raises:
        Last exception if all retries exhausted
    """
    kwargs = kwargs or {}

    # No retry policy means single attempt
    if policy is None:
        return func(*args, **kwargs)

    attempt = 0
    last_error = None

    while attempt < policy.max_attempts:
        attempt += 1

        try:
            return func(*args, **kwargs)

        except Exception as e:
            last_error = e

            # Check if we should retry
            if not policy.should_retry(attempt, e):
                raise

            # If this was the last attempt, raise
            if attempt >= policy.max_attempts:
                raise

            # Calculate delay
            delay = policy.get_delay(attempt)

            # Call retry callback if provided
            if policy.on_retry:
                policy.on_retry(attempt, e)

            # Log retry
            logging.debug(
                f"Retry attempt {attempt}/{policy.max_attempts} "
                f"after {delay:.2f}s delay. Error: {e}"
            )

            # Wait before retry
            time.sleep(delay)

    # Should not reach here, but just in case
    if last_error:
        raise last_error


class RetryableError(Exception):
    """Base class for errors that should be retried."""

    pass


class NonRetryableError(Exception):
    """Base class for errors that should not be retried."""

    pass


# Common retry policies

DEFAULT_RETRY_POLICY = RetryPolicy(
    max_attempts=3,
    backoff=BackoffStrategy.EXPONENTIAL,
    initial_delay=1.0,
)

AGGRESSIVE_RETRY_POLICY = RetryPolicy(
    max_attempts=5,
    backoff=BackoffStrategy.EXPONENTIAL,
    initial_delay=0.5,
    max_delay=30.0,
)

CONSERVATIVE_RETRY_POLICY = RetryPolicy(
    max_attempts=2,
    backoff=BackoffStrategy.CONSTANT,
    initial_delay=2.0,
)

NETWORK_RETRY_POLICY = RetryPolicy(
    max_attempts=4,
    backoff=BackoffStrategy.EXPONENTIAL,
    initial_delay=1.0,
    max_delay=30.0,
    retry_on=[ConnectionError, TimeoutError, RetryableError],
    dont_retry_on=[ValueError, TypeError, NonRetryableError],
)
