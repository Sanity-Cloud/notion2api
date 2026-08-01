import json
import os

from app.workspace_routing import (
    default_workspace_definition,
    expand_accounts_for_workspaces,
)
from dotenv import load_dotenv

# Explicit process/profile settings take precedence over repository .env defaults.
load_dotenv(override=False)

REQUIRED_ACCOUNT_FIELDS = {"token_v2", "space_id", "user_id"}

DEFAULT_ALLOWED_ORIGINS = [
    "http://127.0.0.1:8000",
    "http://localhost:8000",
    "http://127.0.0.1:5173",
    "http://localhost:5173",
    "http://127.0.0.1:5174",
    "http://localhost:5174",
    "http://127.0.0.1:3000",
    "http://localhost:3000",
]


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_allowed_origins(
    value: str | None, *, allow_unsafe_wildcard: bool = False
) -> list[str]:
    """Parse and harden CORS origins.

    The service holds Notion session material locally, so wildcard CORS is not a
    safe default. A wildcard can still be explicitly enabled for isolated tests by
    setting ALLOW_UNSAFE_CORS=true.
    """
    raw = value if value is not None else ""
    origins = [origin.strip() for origin in raw.split(",") if origin.strip()]
    if not origins or "*" in origins:
        return ["*"] if allow_unsafe_wildcard else list(DEFAULT_ALLOWED_ORIGINS)
    return origins


def load_accounts():
    """
    text accounts.json text NOTION_ACCOUNTS text
    textaccounts.json > NOTION_ACCOUNTS text
    """
    # Prefer an explicitly mounted secret file in isolated/sandbox runtimes.
    default_accounts_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "accounts.json",
    )
    accounts_file = os.getenv("NOTION_ACCOUNTS_FILE") or default_accounts_file
    accounts_json = None

    if os.path.exists(accounts_file):
        with open(accounts_file, "r", encoding="utf-8") as f:
            accounts_json = f.read().strip()

    # text
    if not accounts_json:
        accounts_json = os.getenv("NOTION_ACCOUNTS")

    if not accounts_json:
        raise ValueError("text accounts.json text NOTION_ACCOUNTS text")

    try:
        accounts = json.loads(accounts_json)
        if not isinstance(accounts, list) or len(accounts) == 0:
            raise ValueError("text JSON text")
        for idx, account in enumerate(accounts):
            if not isinstance(account, dict):
                raise ValueError(f"text[{idx}] text")
            missing = sorted(
                field for field in REQUIRED_ACCOUNT_FIELDS if not account.get(field)
            )
            if missing:
                raise ValueError(f"text[{idx}] text: {', '.join(missing)}")
        return accounts
    except json.JSONDecodeError as e:
        raise ValueError(f"text: {e}")


# Parse secret-backed account configuration without creating provider clients.
# Runtime startup performs the fail-closed governance binding so isolated config
# loaders can validate mounted secrets without acquiring provider authority.
GOVERNANCE_CONTRACT = default_workspace_definition().contract
ACCOUNTS = load_accounts()


def get_governed_accounts() -> list[dict]:
    """Return accounts bound to the canonical governance/teamspace contract."""
    return expand_accounts_for_workspaces(ACCOUNTS)


def _secret_value(name: str, default: str = "") -> str:
    """Load a secret from NAME_FILE before falling back to NAME."""
    secret_file = os.getenv(f"{name}_FILE", "").strip()
    if secret_file:
        with open(secret_file, "r", encoding="utf-8") as handle:
            return handle.read().strip()
    return os.getenv(name, default)


# FastAPI text
API_KEY = _secret_value("API_KEY")
SILICONFLOW_API_KEY = os.getenv("SILICONFLOW_API_KEY", "")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
ALLOW_UNSAFE_CORS = _env_flag("ALLOW_UNSAFE_CORS", default=False)
ALLOWED_ORIGINS = parse_allowed_origins(
    os.getenv("ALLOWED_ORIGINS"), allow_unsafe_wildcard=ALLOW_UNSAFE_CORS
)

# External URL confirmations from Notion (connections.web.loadPage / urlSafety).
# Notion2API treats these as protocol continuation requirements, not local access
# gates. Authorization gating belongs outside this service.
# EXTERNAL_URL_APPROVAL_POLICY: allow_all (default) | manual
# AUTO_CONTINUE_EXTERNAL_URL_CONFIRMATIONS: automatically call allow-once continuation
EXTERNAL_URL_APPROVAL_POLICY = (
    (
        os.getenv("EXTERNAL_URL_APPROVAL_POLICY")
        or os.getenv("Notion__ExternalUrlApprovalPolicy")
        or "allow_all"
    )
    .strip()
    .lower()
)
AUTO_CONTINUE_EXTERNAL_URL_CONFIRMATIONS = _env_flag(
    "AUTO_CONTINUE_EXTERNAL_URL_CONFIRMATIONS",
    default=_env_flag("Notion__AutoContinueExternalUrlConfirmations", default=True),
)
EXTERNAL_URL_AUTO_APPROVE_MAX_RETRIES = max(
    0,
    int(os.getenv("EXTERNAL_URL_AUTO_APPROVE_MAX_RETRIES", "2") or "2"),
)

# APP_MODE: heavytextlite text standard
APP_MODE = os.getenv("APP_MODE", "heavy").lower().strip()


def is_lite_mode() -> bool:
    return APP_MODE == "lite"


def is_standard_mode() -> bool:
    """Standard text thinking text"""
    return APP_MODE == "standard"


def get_default_account():
    """Return the default account only after canonical governance binding."""
    return get_governed_accounts()[0]
