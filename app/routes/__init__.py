from fastapi import APIRouter
from .health import router as health_router
from .cache import router as cache_router

router = APIRouter()
router.include_router(health_router, prefix="/health", tags=["Health"])
router.include_router(cache_router, prefix="/cache", tags=["Cache"])
