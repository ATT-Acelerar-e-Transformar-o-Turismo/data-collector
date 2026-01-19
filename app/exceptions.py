from fastapi import HTTPException, status


class DataCollectorException(Exception):
    """Base exception for data-collector service."""
    pass


class ValidationException(DataCollectorException):
    """Raised when message validation fails."""
    pass


class CacheException(DataCollectorException):
    """Raised when Redis cache operation fails."""
    pass


class MessageProcessingException(DataCollectorException):
    """Raised when message processing fails."""
    pass


class RabbitMQConnectionException(DataCollectorException):
    """Raised when RabbitMQ connection fails."""
    pass


def validation_error(detail: str) -> HTTPException:
    """Create HTTPException for validation error."""
    return HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Validation error: {detail}"
    )


def cache_error(detail: str) -> HTTPException:
    """Create HTTPException for cache error."""
    return HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Cache error: {detail}"
    )


def message_processing_error(detail: str) -> HTTPException:
    """Create HTTPException for message processing error."""
    return HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=f"Message processing error: {detail}"
    )
