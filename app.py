"""
EBP Demand Plan Dashboard — Databricks App (FastAPI)

Serves the demand plan dashboard as static HTML and provides REST endpoints
that read pre-computed data from DBFS (uploaded by push_demand_plan.py).

Endpoints:
    GET  /              — serve index.html
    GET  /api/v1/data   — dashboard data blob (cached 1 hour, reads from DBFS)
    POST /api/v1/refresh — clear cache + re-read from DBFS
"""
import base64
import os
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles


def _app_dir() -> Path:
    """Databricks Apps may load the app without setting __file__ on the module — avoid bare __file__."""
    mod = sys.modules[__name__]
    mod_path = getattr(mod, "__file__", None)
    if mod_path:
        return Path(mod_path).resolve().parent
    return Path(os.environ.get("APP_ROOT", os.getcwd())).resolve()


# Inline so /favicon.ico always works even if static/ assets are missing on the server.
_FAVICON_SVG = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">'
    '<rect width="32" height="32" rx="6" fill="#0a0a0a"/>'
    '<path fill="#FA5400" d="M6 20c0-4 2.5-6 6-6h4c3.5 0 5-1.2 5-3.5S19.5 7 16 7h-2V5h2c4.5 0 7 2.2 7 5.5S20.5 16 16 16h-4c-1.5 0-2.5.8-2.5 2.5V22H6v-2z"/>'
    "</svg>"
)
# 1x1 transparent PNG — Safari/Chrome probe apple-touch paths; silences 404s without a real asset
_APPLE_TOUCH_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/wIAAgMBgWf8Z0QAAAAASUVORK5CYII="
)

app = FastAPI(
    title="EBP Demand Plan Dashboard",
    version="1.0.0",
)

_import_error = None
try:
    from routes import api_router
    app.include_router(api_router)
except Exception as e:
    _import_error = traceback.format_exc()

STATIC_DIR = _app_dir() / "static"

# Register page + favicon before /static mount so they are not shadowed.
@app.get("/", include_in_schema=False)
async def root():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    """Browsers request /favicon.ico by default; inline body avoids 404 on deploy."""
    return Response(
        content=_FAVICON_SVG.encode("utf-8"),
        media_type="image/svg+xml",
        headers={"Cache-Control": "public, max-age=604800"},
    )


@app.get("/apple-touch-icon.png", include_in_schema=False)
@app.get("/apple-touch-icon-precomposed.png", include_in_schema=False)
async def apple_touch_icon():
    """Some clients request these by convention; we do not ship a full icon asset."""
    return Response(
        content=_APPLE_TOUCH_PNG,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=604800"},
    )


@app.get("/robots.txt", include_in_schema=False)
async def robots():
    return Response(content=b"User-agent: *\nDisallow:\n", media_type="text/plain")


@app.get("/site.webmanifest", include_in_schema=False)
async def webmanifest():
    return JSONResponse(
        content={
            "name": "EBP Demand Plan Dashboard",
            "short_name": "Demand Plan",
            "display": "browser",
        }
    )


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/api/v1/startup-check", include_in_schema=False)
async def startup_check():
    """Always-available diagnostic endpoint — no external imports."""
    info = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "python_version": sys.version,
        "cwd": os.getcwd(),
        "app_dir_contents": sorted(os.listdir(_app_dir())),
        "routes_loaded": _import_error is None,
        "import_error": _import_error,
        "dbfs_path": os.environ.get("DASHBOARD_DBFS_PATH", "(not set)"),
    }

    for pkg in ["fastapi", "uvicorn", "pandas", "snowflake-connector-python", "databricks-sdk"]:
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pip", "show", pkg],
                capture_output=True, text=True, timeout=10,
            )
            info[f"pkg_{pkg}"] = "installed" if result.returncode == 0 else "NOT FOUND"
        except Exception as e:
            info[f"pkg_{pkg}"] = f"check failed: {e}"

    return JSONResponse(content=info)
