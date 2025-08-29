import json
from datetime import datetime, UTC
from schemas.wrapper_message import WrapperMessage
from typing import Optional, Dict, Any
from dependencies.redis import redis_client

class CacheService:
    def __init__(self):
        self.redis = redis_client
        self.last_message_key = "last_message"
        self.last_message_metadata_key = "last_message_metadata"
        self.wrapper_last_message_prefix = "wrapper_last_message:"

    async def store_message(self, message: WrapperMessage):
        # Store global last message
        await self.redis.set(
            self.last_message_key,
            message.model_dump_json()
        )

        # Store global metadata
        metadata = {
            "timestamp": datetime.now(UTC).isoformat(),
            "wrapper_id": message.wrapper_id,
            "data_points_count": len(message.data)
        }
        await self.redis.set(
            self.last_message_metadata_key,
            json.dumps(metadata)
        )
        
        # Store per-wrapper last message
        await self.redis.set(
            f"{self.wrapper_last_message_prefix}{message.wrapper_id}",
            message.model_dump_json()
        )

    async def get_last_message(self) -> Optional[WrapperMessage]:
        data = await self.redis.get(self.last_message_key)
        if not data:
            return None
        
        return WrapperMessage.model_validate_json(data)

    async def get_last_message_metadata(self) -> Optional[Dict[str, Any]]:
        data = await self.redis.get(self.last_message_metadata_key)
        if not data:
            return None
        
        return json.loads(data)
    
    async def get_wrapper_last_message(self, wrapper_id: str) -> Optional[WrapperMessage]:
        data = await self.redis.get(f"{self.wrapper_last_message_prefix}{wrapper_id}")
        if not data:
            return None
        
        return WrapperMessage.model_validate_json(data)

