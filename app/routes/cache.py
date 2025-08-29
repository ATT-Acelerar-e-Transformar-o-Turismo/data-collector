from fastapi import APIRouter
from services.cache_service import CacheService
from typing import Dict, Any, Optional
from schemas.wrapper_message import WrapperMessage

router = APIRouter()
cache_service = CacheService()

@router.get("/last-message")
async def get_last_message() -> Optional[WrapperMessage]:
    """
    Get the last message received by the data collector
    """
    return await cache_service.get_last_message()

@router.get("/last-message-metadata")
async def get_last_message_metadata() -> Optional[Dict[str, Any]]:
    """
    Get metadata about the last message received by the data collector
    """
    return await cache_service.get_last_message_metadata() 
