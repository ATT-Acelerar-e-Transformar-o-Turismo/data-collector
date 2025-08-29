from fastapi import APIRouter, HTTPException
from services.validation_service import ValidationService
from services.cache_service import CacheService
from schemas.wrapper_message import WrapperStatistics, WrapperMessage
from typing import Optional

router = APIRouter()
validation_service = ValidationService()
cache_service = CacheService()

@router.get("/{wrapper_id}/statistics")
async def get_wrapper_statistics(wrapper_id: str) -> WrapperStatistics:
    """Get statistics for a specific wrapper"""
    stats = await validation_service.get_wrapper_stats(wrapper_id)
    if not stats:
        raise HTTPException(status_code=404, detail=f"No statistics found for wrapper {wrapper_id}")
    return stats

@router.get("/{wrapper_id}/last-message")
async def get_wrapper_last_message(wrapper_id: str) -> WrapperMessage:
    """Get the last message received from a specific wrapper"""
    message = await cache_service.get_wrapper_last_message(wrapper_id)
    if not message:
        raise HTTPException(status_code=404, detail=f"No messages found for wrapper {wrapper_id}")
    return message