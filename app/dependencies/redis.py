from redis.asyncio import Redis
from fastapi import Depends
from utils.config import Config
from typing import AsyncGenerator

async def get_redis() -> AsyncGenerator[Redis, None]:
    redis = Redis(
        host=Config.REDIS_HOST,
        port=Config.REDIS_PORT,
        decode_responses=True
    )
    try:
        yield redis
    finally:
        await redis.close() 