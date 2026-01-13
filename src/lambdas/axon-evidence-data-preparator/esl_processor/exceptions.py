class ESLProcessorException(Exception):
    """Base exception for ESL Processor"""
    pass


class CSVValidationException(ESLProcessorException):
    """Raised when CSV validation fails"""
    pass


class CSVReadException(ESLProcessorException):
    """Raised when CSV reading fails"""
    pass


class DataMappingException(ESLProcessorException):
    """Raised when data mapping fails"""
    pass


class CSVWriteException(ESLProcessorException):
    """Raised when CSV writing fails"""
    pass
