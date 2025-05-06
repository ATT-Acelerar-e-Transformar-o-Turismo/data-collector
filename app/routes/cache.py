from fastapi import APIRouter, Depends
from services.cache_service import CacheService
from typing import Dict, Any, Optional
from schemas.message import DataMessage

router = APIRouter(prefix="/cache", tags=["cache"])

@router.get("/last-message")
async def get_last_message(
    cache_service: CacheService = Depends(CacheService.create)
) -> Optional[DataMessage]:
    """
    Get the last message received by the data collector
    """
    return await cache_service.get_last_message()

@router.get("/last-message-metadata")
async def get_last_message_metadata(
    cache_service: CacheService = Depends(CacheService.create)
) -> Optional[Dict[str, Any]]:
    """
    Get metadata about the last message received by the data collector
    """
    return await cache_service.get_last_message_metadata() 