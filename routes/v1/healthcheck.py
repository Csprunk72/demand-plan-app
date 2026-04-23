from fastapi import APIRouter
from fastapi.responses import JSONResponse
from datetime import datetime, timezone

router = APIRouter()


@router.get("/health")
async def healthcheck():
    return JSONResponse(content={"status": "ok", "ts": datetime.now(timezone.utc).isoformat()})
