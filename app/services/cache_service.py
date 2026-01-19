import json
from datetime import datetime, UTC
from typing import Optional, Dict, Any

from schemas.wrapper_message import WrapperMessage
from dependencies.redis import redis_client


LAST_MESSAGE_KEY = "last_message"
LAST_MESSAGE_METADATA_KEY = "last_message_metadata"
WRAPPER_LAST_MESSAGE_PREFIX = "wrapper_last_message:"


class CacheService:
    """Service for Redis-based caching of wrapper messages and metadata."""

    def __init__(self) -> None:
        self._redis = redis_client
        self._last_message_key = LAST_MESSAGE_KEY
        self._last_message_metadata_key = LAST_MESSAGE_METADATA_KEY
        self._wrapper_last_message_prefix = WRAPPER_LAST_MESSAGE_PREFIX

    async def store_message(self, message: WrapperMessage) -> None:
        """Store wrapper message in Redis with global and per-wrapper keys."""
        await self._redis.set(
            self._last_message_key,
            message.model_dump_json()
        )

        metadata = {
            "timestamp": datetime.now(UTC).isoformat(),
            "wrapper_id": message.wrapper_id,
            "data_points_count": len(message.data)
        }
        await self._redis.set(
            self._last_message_metadata_key,
            json.dumps(metadata)
        )

        await self._redis.set(
            f"{self._wrapper_last_message_prefix}{message.wrapper_id}",
            message.model_dump_json()
        )

    async def get_last_message(self) -> Optional[WrapperMessage]:
        """Retrieve the most recent message from any wrapper."""
        data = await self._redis.get(self._last_message_key)
        if not data:
            return None

        return WrapperMessage.model_validate_json(data)

    async def get_last_message_metadata(self) -> Optional[Dict[str, Any]]:
        """Retrieve metadata for the most recent message."""
        data = await self._redis.get(self._last_message_metadata_key)
        if not data:
            return None

        return json.loads(data)

    async def get_wrapper_last_message(self, wrapper_id: str) -> Optional[WrapperMessage]:
        """Retrieve the most recent message from a specific wrapper."""
        data = await self._redis.get(f"{self._wrapper_last_message_prefix}{wrapper_id}")
        if not data:
            return None

        return WrapperMessage.model_validate_json(data)

