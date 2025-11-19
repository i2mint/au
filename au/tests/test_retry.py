"""Tests for retry module."""

import pytest
import time

from au.retry import (
    RetryPolicy,
    BackoffStrategy,
    retry_with_policy,
    RetryableError,
    NonRetryableError,
    DEFAULT_RETRY_POLICY,
)


class TestRetryPolicy:
    """Tests for RetryPolicy class."""

    def test_default_policy(self):
        """Test default retry policy."""
        policy = RetryPolicy()
        assert policy.max_attempts == 3
        assert policy.backoff == BackoffStrategy.EXPONENTIAL
        assert policy.initial_delay == 1.0

    def test_should_retry_max_attempts(self):
        """Test max attempts limit."""
        policy = RetryPolicy(max_attempts=3)

        # Should retry on first and second attempts
        assert policy.should_retry(1, ValueError()) is True
        assert policy.should_retry(2, ValueError()) is True

        # Should not retry after max attempts
        assert policy.should_retry(3, ValueError()) is False

    def test_should_retry_on_specific_errors(self):
        """Test retry on specific error types."""
        policy = RetryPolicy(
            max_attempts=5,
            retry_on=[ConnectionError, TimeoutError],
        )

        # Should retry these errors
        assert policy.should_retry(1, ConnectionError()) is True
        assert policy.should_retry(1, TimeoutError()) is True

        # Should not retry other errors
        assert policy.should_retry(1, ValueError()) is False
        assert policy.should_retry(1, TypeError()) is False

    def test_dont_retry_on_specific_errors(self):
        """Test don't retry on specific error types."""
        policy = RetryPolicy(
            max_attempts=5,
            dont_retry_on=[ValueError, TypeError],
        )

        # Should not retry these errors
        assert policy.should_retry(1, ValueError()) is False
        assert policy.should_retry(1, TypeError()) is False

        # Should retry other errors
        assert policy.should_retry(1, ConnectionError()) is True

    def test_exponential_backoff(self):
        """Test exponential backoff calculation."""
        policy = RetryPolicy(
            backoff=BackoffStrategy.EXPONENTIAL,
            initial_delay=1.0,
        )

        assert policy.get_delay(1) == 1.0
        assert policy.get_delay(2) == 2.0
        assert policy.get_delay(3) == 4.0
        assert policy.get_delay(4) == 8.0

    def test_linear_backoff(self):
        """Test linear backoff calculation."""
        policy = RetryPolicy(
            backoff=BackoffStrategy.LINEAR,
            initial_delay=2.0,
        )

        assert policy.get_delay(1) == 2.0
        assert policy.get_delay(2) == 4.0
        assert policy.get_delay(3) == 6.0

    def test_constant_backoff(self):
        """Test constant backoff calculation."""
        policy = RetryPolicy(
            backoff=BackoffStrategy.CONSTANT,
            initial_delay=3.0,
        )

        assert policy.get_delay(1) == 3.0
        assert policy.get_delay(2) == 3.0
        assert policy.get_delay(3) == 3.0

    def test_max_delay(self):
        """Test max delay cap."""
        policy = RetryPolicy(
            backoff=BackoffStrategy.EXPONENTIAL,
            initial_delay=10.0,
            max_delay=15.0,
        )

        assert policy.get_delay(1) == 10.0
        assert policy.get_delay(2) == 15.0  # Capped at max_delay
        assert policy.get_delay(3) == 15.0


class TestRetryWithPolicy:
    """Tests for retry_with_policy function."""

    def test_successful_function_no_retry(self):
        """Test function that succeeds on first try."""
        call_count = 0

        def successful_func():
            nonlocal call_count
            call_count += 1
            return "success"

        policy = RetryPolicy(max_attempts=3)
        result = retry_with_policy(successful_func, policy=policy)

        assert result == "success"
        assert call_count == 1

    def test_function_succeeds_after_retries(self):
        """Test function that succeeds after some retries."""
        call_count = 0

        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Temporary failure")
            return "success"

        policy = RetryPolicy(
            max_attempts=5,
            initial_delay=0.01,  # Short delay for testing
        )
        result = retry_with_policy(flaky_func, policy=policy)

        assert result == "success"
        assert call_count == 3

    def test_function_fails_after_max_retries(self):
        """Test function that fails even after all retries."""
        call_count = 0

        def always_fails():
            nonlocal call_count
            call_count += 1
            raise ValueError("Always fails")

        policy = RetryPolicy(
            max_attempts=3,
            initial_delay=0.01,
        )

        with pytest.raises(ValueError, match="Always fails"):
            retry_with_policy(always_fails, policy=policy)

        assert call_count == 3

    def test_no_retry_policy(self):
        """Test with no retry policy (None)."""
        call_count = 0

        def func():
            nonlocal call_count
            call_count += 1
            return "result"

        result = retry_with_policy(func, policy=None)

        assert result == "result"
        assert call_count == 1

    def test_retry_with_args_and_kwargs(self):
        """Test retry with function arguments."""
        def add(a, b, multiplier=1):
            return (a + b) * multiplier

        policy = RetryPolicy(max_attempts=1)
        result = retry_with_policy(
            add,
            args=(5, 3),
            kwargs={'multiplier': 2},
            policy=policy,
        )

        assert result == 16

    def test_retry_callback(self):
        """Test on_retry callback."""
        retry_attempts = []

        def on_retry_callback(attempt, error):
            retry_attempts.append((attempt, str(error)))

        call_count = 0

        def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError(f"Attempt {call_count}")
            return "success"

        policy = RetryPolicy(
            max_attempts=5,
            initial_delay=0.01,
            on_retry=on_retry_callback,
        )

        result = retry_with_policy(flaky_func, policy=policy)

        assert result == "success"
        assert len(retry_attempts) == 2
        assert retry_attempts[0][0] == 1
        assert retry_attempts[1][0] == 2


def test_predefined_policies():
    """Test predefined retry policies."""
    # DEFAULT_RETRY_POLICY
    assert DEFAULT_RETRY_POLICY.max_attempts == 3
    assert DEFAULT_RETRY_POLICY.backoff == BackoffStrategy.EXPONENTIAL
