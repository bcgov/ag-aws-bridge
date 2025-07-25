# ==================================================
# FILE: python/lambda_structured_logger/enums.py
# ==================================================

from enum import Enum

class LogLevel(Enum):
    """Standard logging levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

class LogStatus(Enum):
    """Business logic status for operations"""
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    IN_PROGRESS = "IN_PROGRESS"
    RETRY = "RETRY"
    SKIPPED = "SKIPPED"