from fastapi import APIRouter
from .data import router as data_router
from .healthcheck import router as health_router
from .ai_chat import router as ai_chat_router

router = APIRouter()
router.include_router(data_router)
router.include_router(health_router)
router.include_router(ai_chat_router)
