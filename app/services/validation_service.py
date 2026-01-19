import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple

from schemas.wrapper_message import WrapperMessage, WrapperStatistics, XValueType, ValidationError
from dependencies.redis import redis_client


logger = logging.getLogger(__name__)

STATS_PREFIX = "wrapper_stats:"
COUNTER_PREFIX = "wrapper_count:"


class ValidationService:
    """Service for validating wrapper messages and tracking wrapper statistics."""

    def __init__(self) -> None:
        self._redis = redis_client
        self._stats_prefix = STATS_PREFIX
        self._counter_prefix = COUNTER_PREFIX
    
    def _detect_x_type(self, x_value: Any) -> XValueType:
        """Detect X value type: NUMBER, DATETIME, or STRING."""
        if isinstance(x_value, (int, float)):
            return XValueType.NUMBER
        elif isinstance(x_value, str):
            try:
                datetime.fromisoformat(x_value.replace('Z', '+00:00'))
                return XValueType.DATETIME
            except (ValueError, TypeError):
                return XValueType.STRING
        return XValueType.STRING

    async def get_wrapper_stats(self, wrapper_id: str) -> Optional[WrapperStatistics]:
        """Retrieve accumulated statistics for a specific wrapper."""
        stats_key = f"{self._stats_prefix}{wrapper_id}"
        data = await self._redis.hgetall(stats_key)
        if not data:
            return None

        return WrapperStatistics(
            wrapper_id=data["wrapper_id"],
            last_message_timestamp=datetime.fromisoformat(data["last_message_timestamp"]),
            total_messages=int(data["total_messages"]),
            x_value_type=XValueType(data["x_value_type"]),
            last_data_count=int(data["last_data_count"])
        )

    async def update_wrapper_stats(self, wrapper_id: str, message: WrapperMessage, x_type: XValueType) -> None:
        """Update wrapper statistics with atomic operations."""
        total_messages = await self._redis.incr(f"{self._counter_prefix}{wrapper_id}")

        stats_key = f"{self._stats_prefix}{wrapper_id}"
        await self._redis.hset(stats_key, mapping={
            "wrapper_id": wrapper_id,
            "last_message_timestamp": datetime.now(timezone.utc).isoformat(),
            "total_messages": str(total_messages),
            "x_value_type": x_type.value,
            "last_data_count": str(len(message.data))
        })
    
    async def validate_message(self, raw_data: dict) -> Tuple[Optional[WrapperMessage], Optional[ValidationError]]:
        """Validate wrapper message structure, types, and coherence with historical data."""
        wrapper_id = raw_data.get("wrapper_id", "unknown")
        
        try:
            if "wrapper_id" not in raw_data:
                error = ValidationError(
                    wrapper_id="unknown",
                    error_type="schema_error",
                    error_message="Missing wrapper_id field",
                    original_data=raw_data
                )
                return None, error

            if "data" not in raw_data:
                error = ValidationError(
                    wrapper_id=wrapper_id,
                    error_type="schema_error",
                    error_message="Missing data field",
                    original_data=raw_data
                )
                return None, error

            if not isinstance(raw_data["data"], list):
                error = ValidationError(
                    wrapper_id=wrapper_id,
                    error_type="schema_error",
                    error_message="Data field must be an array",
                    original_data=raw_data
                )
                return None, error

            if raw_data["data"]:
                first_point = raw_data["data"][0]
                if "x" not in first_point or "y" not in first_point:
                    error = ValidationError(
                        wrapper_id=wrapper_id,
                        error_type="schema_error",
                        error_message="Data points must have 'x' and 'y' fields",
                        original_data=raw_data
                    )
                    return None, error

                x_type = self._detect_x_type(first_point["x"])

                existing_stats = await self.get_wrapper_stats(wrapper_id)
                if existing_stats and existing_stats.x_value_type != x_type:
                    error = ValidationError(
                        wrapper_id=wrapper_id,
                        error_type="coherence_error",
                        error_message=f"X value type changed from {existing_stats.x_value_type} to {x_type}",
                        original_data=raw_data
                    )
                    return None, error

            message = WrapperMessage(**raw_data)

            if message.data:
                x_type = self._detect_x_type(message.data[0].x)
                await self.update_wrapper_stats(message.wrapper_id, message, x_type)

            return message, None

        except (ValueError, TypeError) as e:
            error = ValidationError(
                wrapper_id=wrapper_id,
                error_type="validation_error",
                error_message=str(e),
                original_data=raw_data
            )
            return None, error
        except KeyError as e:
            error = ValidationError(
                wrapper_id=wrapper_id,
                error_type="schema_error",
                error_message=f"Missing required field: {str(e)}",
                original_data=raw_data
            )
            logger.error(f"Schema validation error for wrapper_id={wrapper_id}: {e}")
            return None, error