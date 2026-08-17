import hmac
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from slowapi.errors import RateLimitExceeded

from app.account_pool import AccountPool
from app.api.attachment_guard import attachment_deployment_guard
from app.api.chat import router as chat_router
from app.api.chat_history import router as chat_history_router
from app.api.chat_history_resume import router as chat_history_resume_router
from app.api.chat_resume_thread_binding import apply_chat_resume_thread_bindings
from app.api.features import router as features_router
from app.api.hive_workforce import router as hive_workforce_router
from app.api.models import router as models_router
from app.api.responses import router as responses_router
from app.api.notion import router as notion_router
from app.api.usage import router as usage_router
from app.attachments.runtime_config import apply_attachment_runtime_config
from app.config import (
    ALLOWED_ORIGINS,
    API_KEY,
    get_governed_accounts,
    is_lite_mode,
    is_standard_mode,
)
from app.compression_observability import compression_telemetry_snapshot
from app.conversation import ConversationManager
from app.chat_history.contracts import runtime_history_contract
from app.chat_history.lossless_archive import history_schema_hash
from app.chat_history.store import get_chat_history_db_root
from app.core.errors import openai_error_payload
from app.core.internal_callers import is_repo_ai_internal_request
from app.limiter import limiter
from app.logger import logger, setup_uvicorn_logging
from app.notion_admission import get_notion_admission_controller
from app.notion_request_telemetry import UsageQuotaExceededError


apply_attachment_runtime_config()
apply_chat_resume_thread_bindings()


def _valid_bearer_token(auth_header: str, expected_key: str) -> bool:
    """Return whether an Authorization header contains the expected bearer token."""
    if not expected_key:
        return True
    scheme, separator, token = str(auth_header or "").partition(" ")
    if not separator or scheme.lower() != "bearer":
        return False
    token = token.strip()
    if not token or any(char.isspace() for char in token):
        return False
    return hmac.compare_digest(token, expected_key)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Re-patch uvicorn loggers now that uvicorn has fully initialised its handlers
    setup_uvicorn_logging()

    # text
    governed_accounts = get_governed_accounts()
    app.state.account_pool = AccountPool(governed_accounts)
    # Keep durable conversation storage available in every mode so chat-history
    # resume/fork can create real local conversations without forcing heavy mode.
    app.state.conversation_manager = ConversationManager()

    # text
    if is_lite_mode():
        logger.info(
            "Service starting up in LITE mode",
            extra={
                "request_info": {
                    "event": "startup",
                    "accounts": len(governed_accounts),
                    "mode": "lite",
                    "conversation_storage": True,
                }
            },
        )
    elif is_standard_mode():
        logger.info(
            "Service starting up in STANDARD mode",
            extra={
                "request_info": {
                    "event": "startup",
                    "accounts": len(governed_accounts),
                    "mode": "standard",
                    "conversation_storage": True,
                }
            },
        )
    else:
        logger.info(
            "Service starting up in HEAVY mode",
            extra={
                "request_info": {
                    "event": "startup",
                    "accounts": len(governed_accounts),
                    "mode": "heavy",
                    "conversation_storage": True,
                }
            },
        )

    app.state.start_time = time.time()
    yield
    # text
    logger.info("Service shutting down", extra={"request_info": {"event": "shutdown"}})


app = FastAPI(
    title="Notion Opus API",
    description="A FastAPI wrapper providing an OpenAI-compatible interface for Notion's Claude Opus backend.",
    version="1.0.1",
    lifespan=lifespan,
)

# text
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# text Limiter
app.state.limiter = limiter


# text 429 text
def custom_rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"error": "Too many requests, please try again later"},
    )


app.add_exception_handler(RateLimitExceeded, custom_rate_limit_exceeded_handler)


@app.exception_handler(UsageQuotaExceededError)
async def usage_quota_exceeded_handler(request: Request, exc: UsageQuotaExceededError):
    retry_after = max(1, int(exc.retry_after_seconds + 0.999))
    return JSONResponse(
        status_code=429,
        headers={"Retry-After": str(retry_after)},
        content={
            "error": {
                "message": "Operational usage quota exceeded",
                "type": "rate_limit_error",
                "code": "usage_quota_exceeded",
                "quota_id": exc.quota_id,
                "dimension": exc.dimension,
                "retry_after_seconds": retry_after,
            }
        },
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    if isinstance(exc.detail, dict):
        return JSONResponse(status_code=exc.status_code, content=exc.detail)
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.error(
        "Unhandled application exception",
        exc_info=True,
        extra={
            "request_info": {
                "event": "unhandled_exception",
                "method": request.method,
                "path": request.url.path,
            }
        },
    )
    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "message": "Internal server error",
                "type": "server_error",
            }
        },
    )


# Attachment deployment guard runs before body parsing in chat/response handlers.
app.middleware("http")(attachment_deployment_guard)


# text
@app.middleware("http")
async def log_requests_middleware(request: Request, call_next):
    start_time = time.time()

    # text
    skip_logging = request.url.path in ["/health", "/healthz", "/favicon.ico"]

    try:
        response = await call_next(request)
        status_code = response.status_code
    except Exception as e:
        status_code = 500
        logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
        raise
    finally:
        process_time = time.time() - start_time
        client_ip = request.client.host if request.client else "unknown"

        if not skip_logging:
            log_level = logger.error if status_code >= 400 else logger.info
            log_level(
                "Request processed",
                extra={
                    "request_info": {
                        "method": request.method,
                        "path": request.url.path,
                        "ip": client_ip,
                        "status_code": status_code,
                        "duration_ms": round(process_time * 1000, 2),
                    }
                },
            )

    return response


# text API Key text
@app.middleware("http")
async def api_key_auth(request: Request, call_next):
    # text API_KEYtext
    if API_KEY:
        # text OPTIONS text
        if request.url.path.startswith("/v1") and request.method != "OPTIONS":
            auth_header = request.headers.get("Authorization", "")
            if not is_repo_ai_internal_request(request) and not _valid_bearer_token(
                auth_header, API_KEY
            ):
                return JSONResponse(
                    status_code=401,
                    content=openai_error_payload(
                        message="Error: API KEY doesn't match.",
                        code="invalid_api_key",
                        status_code=401,
                    ),
                )
    return await call_next(request)


# text /v1
app.include_router(chat_router, prefix="/v1")
app.include_router(models_router, prefix="/v1")
app.include_router(chat_history_router, prefix="/v1")
app.include_router(chat_history_resume_router, prefix="/v1")
app.include_router(features_router, prefix="/v1")
app.include_router(hive_workforce_router, prefix="/v1")
app.include_router(responses_router, prefix="/v1")
app.include_router(notion_router, prefix="/v1")
app.include_router(usage_router, prefix="/v1")


# text
@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(content=b"", media_type="image/x-icon", status_code=204)


@app.get("/health", tags=["system"])
def health_check(request: Request):
    uptime = time.time() - request.app.state.start_time
    pool = request.app.state.account_pool
    status = pool.get_status_summary()
    history_contract = runtime_history_contract(
        conversation_store_path=request.app.state.conversation_manager.db_path,
        history_store_root=get_chat_history_db_root(),
        history_schema_hash=history_schema_hash(),
    )
    return {
        "status": "ok",
        "accounts": status["active"],
        "accounts_total": status["total"],
        "accounts_cooling": status["cooling"],
        "uptime": int(uptime),
        "account_selection": pool.get_selection_summary(),
        "governance": pool.get_governance_summary(),
        "notion_admission": get_notion_admission_controller().snapshot(),
        "history": history_contract,
        "conversation_compression": compression_telemetry_snapshot(),
    }


@app.get("/healthz", tags=["system"])
def healthz(request: Request):
    return health_check(request)


frontend_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "frontend"
)


def _frontend_js_response(filename: str):
    script_path = os.path.join(frontend_dir, "js", filename)
    if not os.path.exists(script_path):
        return Response(
            content=b"", media_type="application/javascript", status_code=404
        )
    with open(script_path, "rb") as f:
        return Response(
            content=f.read(),
            media_type="application/javascript",
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )


@app.get("/chat-history-import.js", include_in_schema=False)
def chat_history_import_js():
    return _frontend_js_response("chat-history-import.js")


@app.get("/chat-history-browser.js", include_in_schema=False)
def chat_history_browser_js():
    return _frontend_js_response("chat-history-browser.js")


@app.get("/chat-history-main.js", include_in_schema=False)
def chat_history_main_js():
    return _frontend_js_response("chat-history-main.js")


@app.get("/chat-history-resume.js", include_in_schema=False)
def chat_history_resume_js():
    return _frontend_js_response("chat-history-resume.js")


@app.get("/attachment-settings.js", include_in_schema=False)
def attachment_settings_js():
    return _frontend_js_response("attachment-settings.js")


@app.get("/", include_in_schema=False)
def frontend_index(request: Request):
    index_path = os.path.join(frontend_dir, "index.html")
    if not os.path.exists(index_path):
        return Response(content=b"", media_type="text/html", status_code=404)
    with open(index_path, "r", encoding="utf-8") as f:
        html = f.read()
    script_tags = [
        '<script src="/chat-history-import.js"></script>',
        '<script src="/chat-history-browser.js"></script>',
        '<script src="/chat-history-main.js"></script>',
        '<script src="/chat-history-resume.js"></script>',
        '<script src="/attachment-settings.js"></script>',
        '<script src="/js/operations-drawer.js"></script>',
        '<script src="/js/model-lab.js"></script>',
        '<script src="/js/notion-content-panel.js"></script>',
    ]
    missing_tags = [tag for tag in script_tags if tag not in html]
    if missing_tags:
        html = html.replace("</body>", "\n".join(missing_tags) + "\n</body>")

    # Auto-populate API key in WebUI if request is local (127.0.0.1 or ::1) and API_KEY is set
    client_ip = request.client.host if request.client else ""
    if API_KEY and client_ip in ("127.0.0.1", "::1"):
        injection = f"""
<script>
  (function() {{
    const serverKey = {repr(API_KEY)};
    const currentKey = localStorage.getItem('claude_api_key');
    if (currentKey !== serverKey) {{
      localStorage.setItem('claude_api_key', serverKey);
      if (window.NotionAI && window.NotionAI.Core && window.NotionAI.Core.State) {{
        window.NotionAI.Core.State._state.apiKey = serverKey;
      }}
    }}
  }})();
</script>
"""
        html = html.replace("</body>", injection + "\n</body>")

    return Response(content=html, media_type="text/html")


# text
if os.path.exists(frontend_dir):
    app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")
