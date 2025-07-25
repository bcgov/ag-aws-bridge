# ==================================================
# FILE: python/lambda_structured_logger/__init__.py
# ==================================================

"""
Lambda Structured Logger Package
Provides consistent structured logging for AWS Lambda functions
"""

from .logger import LambdaStructuredLogger
from .enums import LogLevel, LogStatus

__version__ = "1.0.0"
__all__ = ["LambdaStructuredLogger", "LogLevel", "LogStatus"]