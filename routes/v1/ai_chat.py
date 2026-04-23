"""
POST /api/v1/ai-chat — proxy to Nike AIR Insights (or compatible HTTPS JSON API).

Configure via environment variables on the Databricks App (secrets recommended):

  AIR_INSIGHTS_API_URL   — Full HTTPS URL of the AIR chat/completions endpoint (required for live calls)
  AIR_INSIGHTS_API_TOKEN — Bearer token (optional if AIR uses other auth)
  AIR_INSIGHTS_TIMEOUT_S — Request timeout seconds (default 120)
  AIR_INSIGHTS_CONTEXT_PREFIX — Optional text prepended server-side to scope the assistant to this dashboard / datasets

Request body to upstream: {"prompt": "<user message>"} — override with AIR_INSIGHTS_JSON_KEY_MESSAGE
  if your API expects a different JSON key for the user text (e.g. "query").

Response: tries to extract text from JSON keys: reply, answer, message, text, data.reply, result.
"""
from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()


class ChatBody(BaseModel):
    message: str = ""


def _extract_reply(raw: object) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        return raw.strip() or None
    if not isinstance(raw, dict):
        return None
    for key in ("reply", "answer", "message", "text", "response", "output"):
        val = raw.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    data = raw.get("data")
    if isinstance(data, dict):
        for key in ("reply", "answer", "message", "text"):
            val = data.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    choices = raw.get("choices")
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            msg = first.get("message") or first.get("delta")
            if isinstance(msg, dict) and isinstance(msg.get("content"), str):
                return msg["content"].strip()
            if isinstance(first.get("text"), str):
                return first["text"].strip()
    return None


def _forward_to_air(message: str) -> tuple[str | None, str | None]:
    url = (os.environ.get("AIR_INSIGHTS_API_URL") or "").strip()
    token = (os.environ.get("AIR_INSIGHTS_API_TOKEN") or "").strip()
    timeout_s = float(os.environ.get("AIR_INSIGHTS_TIMEOUT_S") or "120")
    msg_key = (os.environ.get("AIR_INSIGHTS_JSON_KEY_MESSAGE") or "prompt").strip()

    if not url:
        return None, "AIR_INSIGHTS_API_URL is not set."

    payload_obj = {msg_key: message}
    data = json.dumps(payload_obj).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if token:
        scheme = (os.environ.get("AIR_INSIGHTS_AUTH_SCHEME") or "Bearer").strip()
        headers["Authorization"] = f"{scheme} {token}"

    req = urllib.request.Request(url, data=data, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            body = resp.read().decode(charset, errors="replace")
    except urllib.error.HTTPError as e:
        err_body = ""
        try:
            err_body = e.read().decode("utf-8", errors="replace")[:2000]
        except Exception:
            pass
        logger.warning("AIR HTTP %s: %s", e.code, err_body)
        return None, f"AIR returned HTTP {e.code}. Check URL and token. Body: {err_body[:500]}"
    except urllib.error.URLError as e:
        logger.exception("AIR connection failed")
        return None, f"Could not reach AIR endpoint: {e.reason!s}"

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return body.strip() if body.strip() else None, None

    text = _extract_reply(parsed)
    if text:
        return text, None
    return None, "AIR returned JSON but no recognized reply field. Raw keys: " + (
        ", ".join(sorted(parsed.keys())[:12]) if isinstance(parsed, dict) else "?"
    )


@router.post("/ai-chat")
async def ai_chat(body: ChatBody):
    msg = (body.message or "").strip()
    if not msg:
        return JSONResponse(status_code=400, content={"error": "message is empty", "reply": ""})

    ctx = (os.environ.get("AIR_INSIGHTS_CONTEXT_PREFIX") or "").strip()
    if ctx:
        msg = ctx.rstrip() + "\n\nUser question:\n" + msg

    reply, err = _forward_to_air(msg)
    if reply:
        return JSONResponse(content={"reply": reply, "configured": True})

    if err and "not set" in err.lower():
        return JSONResponse(
            content={
                "reply": (
                    "AIR Insights is not configured. In the Databricks App, set environment variables "
                    "AIR_INSIGHTS_API_URL (full HTTPS endpoint) and AIR_INSIGHTS_API_TOKEN if required. "
                    "Optional: AIR_INSIGHTS_JSON_KEY_MESSAGE if the API uses \"query\" instead of \"prompt\"."
                ),
                "configured": False,
                "hint": err,
            }
        )

    return JSONResponse(
        status_code=502,
        content={
            "reply": err or "Unknown error calling AIR.",
            "configured": bool((os.environ.get("AIR_INSIGHTS_API_URL") or "").strip()),
        },
    )
