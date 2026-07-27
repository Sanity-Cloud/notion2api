import importlib
import json
from types import SimpleNamespace

from app.api import chat


class _Headers(dict):
    def get(self, key, default=None):
        return super().get(key.lower(), default)


def _request(**headers):
    return SimpleNamespace(headers=_Headers({k.lower(): v for k, v in headers.items()}))


def _body(metadata=None):
    return SimpleNamespace(metadata=metadata or {})


def test_sandbox_remote_guard_is_disabled_by_default(monkeypatch):
    monkeypatch.delenv("NOTION_SANDBOX_REQUIRE_EXPLICIT_REMOTE", raising=False)
    assert chat._sandbox_remote_request_authorized(_request(), _body()) is True


def test_sandbox_remote_guard_requires_explicit_opt_in(monkeypatch):
    monkeypatch.setenv("NOTION_SANDBOX_REQUIRE_EXPLICIT_REMOTE", "true")
    assert chat._sandbox_remote_request_authorized(_request(), _body()) is False
    assert (
        chat._sandbox_remote_request_authorized(
            _request(**{"X-Sandbox-Allow-Remote": "true"}), _body()
        )
        is True
    )
    assert (
        chat._sandbox_remote_request_authorized(
            _request(), _body({"sandbox_allow_remote": True})
        )
        is True
    )


def test_config_loads_accounts_from_explicit_secret_file(monkeypatch, tmp_path):
    accounts_file = tmp_path / "accounts.json"
    accounts_file.write_text(
        json.dumps(
            [
                {
                    "token_v2": "sandbox-token",
                    "space_id": "sandbox-space",
                    "user_id": "sandbox-user",
                }
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("NOTION_ACCOUNTS_FILE", str(accounts_file))

    import app.config as config

    reloaded = importlib.reload(config)
    assert reloaded.ACCOUNTS[0]["user_id"] == "sandbox-user"


def test_config_loads_api_key_from_secret_file(monkeypatch, tmp_path):
    key_file = tmp_path / "api-key.txt"
    key_file.write_text("sandbox-api-key", encoding="utf-8")
    monkeypatch.setenv("API_KEY_FILE", str(key_file))

    import app.config as config

    reloaded = importlib.reload(config)
    assert reloaded.API_KEY == "sandbox-api-key"
