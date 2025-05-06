import json
from datetime import datetime, UTC
from schemas.message import DataMessage
from typing import Optional, Dict, Any
from dependencies.redis import get_redis
from fastapi import Depends
from redis.asyncio import Redis

class CacheService:
    NOT_INITIALIZED_ERROR = "Cache service not initialized"

    def __init__(self, redis: Redis):
        self.redis = redis
        self.last_message_key = "last_message"
        self.last_message_metadata_key = "last_message_metadata"

    async def store_message(self, message: DataMessage):
        # Store the message
        await self.redis.set(
            self.last_message_key,
            message.model_dump_json()
        )

        # Store metadata
        metadata = {
            "timestamp": datetime.now(UTC).isoformat(),
            "wrapper_id": message.wrapper_id,
            "data_points_count": len(message.data_segment)
        }
        await self.redis.set(
            self.last_message_metadata_key,
            json.dumps(metadata)
        )

    async def get_last_message(self) -> Optional[DataMessage]:
        data = await self.redis.get(self.last_message_key)
        if not data:
            return None
        
        return DataMessage.model_validate_json(data)

    async def get_last_message_metadata(self) -> Optional[Dict[str, Any]]:
        data = await self.redis.get(self.last_message_metadata_key)
        if not data:
            return None
        
        return json.loads(data)

    @classmethod
    async def create(cls, redis: Redis = Depends(get_redis)):
        return cls(redis) 