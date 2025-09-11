#!/usr/bin/env python3
"""
Retry Utilities for Data Components

This module provides simple retry logic for API calls and other operations.
"""

import logging
import os
import time
import functools
from typing import Callable, Any, Optional, Type, Tuple, Union

logger = logging.getLogger("tekmetric")


class RetryConfig:
    """Configuration for retry behavior.
    
    Simple configuration focusing on max_retries with fixed 1-second delays.
    """
    
    def __init__(self, max_retries: int = 5):
        self.max_retries = max_retries
    
    @classmethod
    def from_env(cls) -> 'RetryConfig':
        """Create RetryConfig from environment variables."""
        return cls(
            max_retries=int(os.environ.get('RETRY_COUNT', '5'))
        )




def retry_function(
    exceptions: Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception,
    config: Optional[RetryConfig] = None,
    on_retry: Optional[Callable[[Exception, int], None]] = None
):
    """
    Decorator for retrying functions with simple retry logic.
    
    Args:
        exceptions: Exception types to catch and retry on
        config: Retry configuration (uses environment variables if None)
        on_retry: Optional callback function called on each retry attempt
                 Signature: on_retry(exception, attempt_number)
    
    Example:
        @retry_function(APIError, RetryConfig(max_retries=3))
        def api_call():
            # API call that might fail
            pass
    """
    if config is None:
        config = RetryConfig.from_env()
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(config.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == config.max_retries:
                        logger.error(f"Function {func.__name__} failed after {config.max_retries} retries: {e}")
                        raise e
                    
                    # Simple fixed delay of 1 second between retries
                    delay = 1.0
                    logger.warning(f"Function {func.__name__} failed (attempt {attempt + 1}/{config.max_retries + 1}): {e}. Retrying in {delay}s...")
                    
                    if on_retry:
                        on_retry(e, attempt + 1)
                    
                    time.sleep(delay)
            
            # This should never be reached, but just in case
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator


def retry_api_call(max_retries: Optional[int] = None):
    """
    Specialized retry decorator for API calls with sensible defaults.
    
    Args:
        max_retries: Maximum number of retries (uses RETRY_COUNT env var if None)
    
    Example:
        @retry_api_call(max_retries=3)
        def fetch_data():
            # API call
            pass
    """
    if max_retries is None:
        max_retries = int(os.environ.get('RETRY_COUNT', '5'))
    
    config = RetryConfig(max_retries=max_retries)
    
    return retry_function(
        exceptions=(ConnectionError, TimeoutError, Exception),
        config=config,
        on_retry=lambda e, attempt: logger.info(f"API retry attempt {attempt}: {type(e).__name__}")
    )


def retry_file_operation(max_retries: int = 3):
    """
    Specialized retry decorator for file operations.
    
    Args:
        max_retries: Maximum number of retries
    
    Example:
        @retry_file_operation()
        def write_file():
            # File operation
            pass
    """
    config = RetryConfig(max_retries=max_retries)
    
    return retry_function(
        exceptions=(OSError, IOError, PermissionError),
        config=config,
        on_retry=lambda e, attempt: logger.warning(f"File operation retry attempt {attempt}: {e}")
    )
