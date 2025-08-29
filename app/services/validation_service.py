import json
from datetime import datetime
from typing import Dict, Any, Optional
from schemas.wrapper_message import WrapperMessage, WrapperStatistics, XValueType, ValidationError
from dependencies.redis import redis_client
import logging

logger = logging.getLogger(__name__)

class ValidationService:
    def __init__(self):
        self.redis = redis_client
        self.stats_prefix = "wrapper_stats:"
        self.counter_prefix = "wrapper_count:"
    
    def _detect_x_type(self, x_value: Any) -> XValueType:
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
        stats_key = f"{self.stats_prefix}{wrapper_id}"
        data = await self.redis.hgetall(stats_key)
        if not data:
            return None
        
        return WrapperStatistics(
            wrapper_id=data["wrapper_id"],
            last_message_timestamp=datetime.fromisoformat(data["last_message_timestamp"]),
            total_messages=int(data["total_messages"]),
            x_value_type=XValueType(data["x_value_type"]),
            last_data_count=int(data["last_data_count"])
        )
    
    async def update_wrapper_stats(self, wrapper_id: str, message: WrapperMessage, x_type: XValueType):
        # Atomic counter increment
        total_messages = await self.redis.incr(f"{self.counter_prefix}{wrapper_id}")
        
        # Update other stats using atomic hash operations
        stats_key = f"{self.stats_prefix}{wrapper_id}"
        await self.redis.hset(stats_key, mapping={
            "wrapper_id": wrapper_id,
            "last_message_timestamp": datetime.utcnow().isoformat(),
            "total_messages": str(total_messages),
            "x_value_type": x_type.value,
            "last_data_count": str(len(message.data))
        })
    
    async def validate_message(self, raw_data: dict) -> tuple[Optional[WrapperMessage], Optional[ValidationError]]:
        wrapper_id = raw_data.get("wrapper_id", "unknown")
        
        try:
            # Basic schema validation
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
            
            # Pre-validate data structure before pydantic validation
            if not isinstance(raw_data["data"], list):
                error = ValidationError(
                    wrapper_id=wrapper_id,
                    error_type="schema_error",
                    error_message="Data field must be an array",
                    original_data=raw_data
                )
                return None, error
            
            # Detect x value type from first data point before validation
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
                
                # Check coherence with previous messages
                existing_stats = await self.get_wrapper_stats(wrapper_id)
                if existing_stats and existing_stats.x_value_type != x_type:
                    error = ValidationError(
                        wrapper_id=wrapper_id,
                        error_type="coherence_error",
                        error_message=f"X value type changed from {existing_stats.x_value_type} to {x_type}",
                        original_data=raw_data
                    )
                    return None, error
            
            # Now validate with pydantic (includes duplicate detection)
            message = WrapperMessage(**raw_data)
            
            # Update stats atomically
            if message.data:
                x_type = self._detect_x_type(message.data[0].x)
                await self.update_wrapper_stats(message.wrapper_id, message, x_type)
            
            return message, None
            
        except Exception as e:
            error = ValidationError(
                wrapper_id=wrapper_id,
                error_type="validation_error",
                error_message=str(e),
                original_data=raw_data
            )
            return None, error