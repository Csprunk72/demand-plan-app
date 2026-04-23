"""
GET  /api/v1/data    — returns the full demand plan dashboard JSON blob.
POST /api/v1/refresh — re-reads the latest data from DBFS.

Data is pre-computed by push_demand_plan.py and stored as a JSON blob on DBFS.
"""
from __future__ import annotations

import json
import logging
import os
import time
import threading
from typing import Optional

from fastapi import APIRouter
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

router = APIRouter()

DBFS_PATH = os.environ.get(
    "DASHBOARD_DBFS_PATH",
    "dbfs:/FileStore/ebp_dashboard/demand_plan_blob.json",
)

_cache_lock = threading.Lock()
_cached_data: Optional[dict] = None
_cached_at: float = 0
CACHE_TTL_SECONDS = 3600


def _get_cached() -> Optional[dict]:
    global _cached_data, _cached_at
    if _cached_data is not None and (time.time() - _cached_at) < CACHE_TTL_SECONDS:
        return _cached_data
    return None


def _set_cache(data: dict) -> None:
    global _cached_data, _cached_at
    with _cache_lock:
        _cached_data = data
        _cached_at = time.time()


def _clear_cache() -> None:
    global _cached_data, _cached_at
    with _cache_lock:
        _cached_data = None
        _cached_at = 0


def get_blob() -> dict:
    cached = _get_cached()
    if cached is not None:
        return cached
    data = _read_from_dbfs()
    _set_cache(data)
    return data


@router.get("/data")
async def get_dashboard_data():
    """Return the full demand plan data blob."""
    try:
        data = get_blob()
        return JSONResponse(content=data)
    except Exception as e:
        logger.exception("Failed to read demand plan data from DBFS")
        return JSONResponse(
            status_code=500,
            content={
                "error": str(e),
                "hint": "Run push_demand_plan.py locally to upload data, or check DBFS path.",
            },
        )


@router.post("/refresh")
async def refresh_dashboard_data():
    """Clear cache and re-read from DBFS."""
    _clear_cache()
    try:
        data = _read_from_dbfs()
        _set_cache(data)
        meta = data.get("m", {})
        return JSONResponse(content={
            "status": "ok",
            "ts": meta.get("ts", ""),
            "records": {
                "uif": len(data.get("u", [])),
                "op":  len(data.get("op", [])),
                "dp":  len(data.get("dp", [])),
                "sro": len(data.get("sro", [])),
                "fr":  len(data.get("fr", [])),
            },
        })
    except Exception as e:
        logger.exception("Refresh failed")
        return JSONResponse(
            status_code=500,
            content={
                "error": str(e),
                "hint": "Run push_demand_plan.py locally to upload fresh data.",
            },
        )


def _read_from_dbfs() -> dict:
    import base64
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    dbfs_path = DBFS_PATH
    if dbfs_path.startswith("dbfs:"):
        dbfs_path = dbfs_path[len("dbfs:"):]

    logger.info("Reading demand plan blob from DBFS: %s", dbfs_path)

    _CHUNK = 1_048_576
    chunks: list[bytes] = []
    offset = 0

    while True:
        resp = w.dbfs.read(dbfs_path, offset=offset, length=_CHUNK)
        if resp.data:
            chunks.append(base64.b64decode(resp.data))
        bytes_read = resp.bytes_read or 0
        if bytes_read < _CHUNK:
            break
        offset += bytes_read

    if not chunks:
        raise RuntimeError(
            f"DBFS file empty or missing: {dbfs_path}. "
            "Run push_demand_plan.py locally to upload data."
        )

    raw = b"".join(chunks).decode("utf-8")
    data = json.loads(raw)
    logger.info("Loaded blob: ts=%s, seasons=%s", data.get("m", {}).get("ts"), data.get("m", {}).get("seasons"))
    return data
