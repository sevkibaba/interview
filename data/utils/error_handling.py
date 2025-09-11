#!/usr/bin/env python3
"""
Standardized Error Handling Utilities for Data Components

This module provides consistent error handling patterns across all data services.
"""

import logging
import functools
from typing import Any, Callable, Optional, Union
from enum import Enum

logger = logging.getLogger("tekmetric")


class ErrorSeverity(Enum):
    """Error severity levels for consistent handling."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class DataProcessingError(Exception):
    """Base exception for data processing errors."""
    def __init__(self, message: str, severity: ErrorSeverity = ErrorSeverity.MEDIUM, 
                 component: str = "unknown", original_error: Optional[Exception] = None):
        self.message = message
        self.severity = severity
        self.component = component
        self.original_error = original_error
        super().__init__(self.message)


class APIError(DataProcessingError):
    """Exception for API-related errors."""
    def __init__(self, message: str, status_code: Optional[int] = None, 
                 original_error: Optional[Exception] = None):
        super().__init__(message, ErrorSeverity.HIGH, "api", original_error)
        self.status_code = status_code


class DataValidationError(DataProcessingError):
    """Exception for data validation errors."""
    def __init__(self, message: str, field: Optional[str] = None, 
                 original_error: Optional[Exception] = None):
        super().__init__(message, ErrorSeverity.MEDIUM, "validation", original_error)
        self.field = field


class FileOperationError(DataProcessingError):
    """Exception for file operation errors."""
    def __init__(self, message: str, file_path: Optional[str] = None, 
                 original_error: Optional[Exception] = None):
        super().__init__(message, ErrorSeverity.MEDIUM, "file_ops", original_error)
        self.file_path = file_path


def handle_errors(component: str, severity: ErrorSeverity = ErrorSeverity.MEDIUM, 
                 reraise: bool = True, return_default: Any = None):
    """
    Decorator for consistent error handling across data components.
    
    Args:
        component: Name of the component for logging
        severity: Default severity level for unhandled errors
        reraise: Whether to reraise the exception after logging
        return_default: Default value to return if reraise is False
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except DataProcessingError as e:
                # Re-raise our custom exceptions as-is
                logger.error(f"[{component}] {e.message}", exc_info=True)
                if reraise:
                    raise
                return return_default
            except Exception as e:
                # Wrap unexpected exceptions
                error_msg = f"Unexpected error in {func.__name__}: {str(e)}"
                logger.error(f"[{component}] {error_msg}", exc_info=True)
                
                if reraise:
                    raise DataProcessingError(
                        error_msg, severity, component, e
                    )
                return return_default
        return wrapper
    return decorator


def safe_convert(value: Any, target_type: type, field_name: str = "unknown", 
                default: Any = None) -> Any:
    """
    Safely convert a value to target type with consistent error handling.
    
    Args:
        value: Value to convert
        target_type: Target type for conversion
        field_name: Name of the field for error logging
        default: Default value if conversion fails
        
    Returns:
        Converted value or default
    """
    if value is None:
        return default
        
    try:
        return target_type(value)
    except (ValueError, TypeError) as e:
        logger.warning(f"Failed to convert {field_name} to {target_type.__name__}: {e}")
        return default


def safe_float(value: Any, field_name: str = "unknown") -> Optional[float]:
    """Safely convert value to float."""
    return safe_convert(value, float, field_name, None)


def safe_int(value: Any, field_name: str = "unknown") -> Optional[int]:
    """Safely convert value to int."""
    return safe_convert(value, int, field_name, None)


def safe_bool(value: Any, field_name: str = "unknown") -> Optional[bool]:
    """Safely convert value to bool."""
    if value is None:
        return None
    return safe_convert(value, bool, field_name, None)


def log_and_continue(error: Exception, context: str = "", 
                    severity: ErrorSeverity = ErrorSeverity.LOW) -> None:
    """
    Log an error and continue execution (for non-critical errors).
    
    Args:
        error: Exception to log
        context: Additional context for logging
        severity: Error severity level
    """
    log_level = {
        ErrorSeverity.LOW: logger.debug,
        ErrorSeverity.MEDIUM: logger.warning,
        ErrorSeverity.HIGH: logger.error,
        ErrorSeverity.CRITICAL: logger.critical
    }
    
    message = f"Non-critical error in {context}: {str(error)}"
    log_level[severity](message)


def validate_required_fields(data: dict, required_fields: list, 
                           component: str = "validation") -> None:
    """
    Validate that required fields are present in data.
    
    Args:
        data: Dictionary to validate
        required_fields: List of required field names
        component: Component name for error context
        
    Raises:
        DataValidationError: If required fields are missing
    """
    missing_fields = [field for field in required_fields if field not in data or data[field] is None]
    
    if missing_fields:
        raise DataValidationError(
            f"Missing required fields: {missing_fields}",
            component=component
        )


def handle_api_response(response: dict, endpoint: str = "unknown") -> dict:
    """
    Handle API response with consistent error checking.
    
    Args:
        response: API response dictionary
        endpoint: API endpoint name for logging
        
    Returns:
        Response dictionary if valid
        
    Raises:
        APIError: If response contains error
    """
    if 'error' in response:
        error_msg = response['error'].get('message', 'Unknown API error')
        status_code = response['error'].get('status_code')
        raise APIError(f"API error from {endpoint}: {error_msg}", status_code)
    
    return response
