"""Regression tests for Notion CDP target selection and cookie isolation."""

from __future__ import annotations

import login


def test_official_host_matching_rejects_lookalikes():
    assert login._looks_like_notion_url("https://app.notion.com/ai")
    assert login._looks_like_notion_url("https://www.notion.so/chat")
    assert login._looks_like_notion_url("https://team.notion.site/page")
    assert not login._looks_like_notion_url("https://notion.com.evil.example/")
    assert not login._looks_like_notion_url("https://evil.example/notion.so")
    assert not login._looks_like_notion_url("chrome://omnibox-popup/")


def test_debug_target_prefers_notion_page(monkeypatch):
    entries = [
        {"type": "browser_ui", "url": "chrome://omnibox-popup", "id": "ui"},
        {"type": "page", "url": "https://unrelated.example/", "id": "other"},
        {"type": "page", "url": "https://app.notion.com/ai", "id": "notion"},
    ]
    monkeypatch.setattr(login, "_get_json", lambda _url: entries)
    assert login._find_debug_target(9222)["id"] == "notion"
def test_debug_target_does_not_select_browser_ui(monkeypatch):
    entries = [{"type": "browser_ui", "url": "chrome://omnibox-popup"}]
    monkeypatch.setattr(login, "_get_json", lambda _url: entries)
    assert login._find_debug_target(9222) is None


def test_cookie_fallback_ignores_unrelated_domains(monkeypatch):
    class FakeCDP:
        def __init__(self):
            self.methods = []

        def call(self, method, params=None):
            self.methods.append(method)
            if method == "Runtime.evaluate":
                expression = (params or {}).get("expression", "")
                if expression == "location.href":
                    return {"result": {"value": "https://app.notion.com/ai"}}
                return {"result": {"value": ""}}
            if method == "Network.getCookies":
                return {"cookies": []}
            if method == "Network.getAllCookies":
                return {"cookies": [
                    {"domain": ".other.example", "name": "token_v2", "value": "WRONG"},
                    {"domain": ".notion.com", "name": "token_v2", "value": "TEST_TOKEN"},
                ]}
            return {}

        def close(self):
            self.methods.append("close")

    fake = FakeCDP()
    monkeypatch.setattr(login, "_find_debug_target", lambda _port: {
        "type": "page", "webSocketDebuggerUrl": "ws://127.0.0.1:9222/devtools/page/test"
    })
    monkeypatch.setattr(login, "_CDPClient", lambda _ws_url: fake)
    result = login._extract_cookies_from_cdp(9222)
    assert result["token_v2"] == "TEST_TOKEN"
    assert "Network.getAllCookies" in fake.methods
    assert fake.methods[-1] == "close"
