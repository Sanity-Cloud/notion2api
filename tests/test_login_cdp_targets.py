"""Regression tests for Notion CDP target selection and cookie isolation."""

from __future__ import annotations

import login
import pytest


def test_official_host_matching_rejects_lookalikes():
    assert login._looks_like_notion_url("https://app.notion.com/ai")
    assert login._looks_like_notion_url("https://www.notion.so/chat")
    assert not login._looks_like_notion_url("https://team.notion.site/page")
    assert not login._looks_like_notion_url("http://app.notion.com/ai")
    assert not login._looks_like_notion_url("https://evil.example@notion.com/ai")
    assert not login._looks_like_notion_url("https://www.notion.so:8443/ai")
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


def test_no_foreign_page_is_selected_or_navigated(monkeypatch):
    entries = [
        {"type": "page", "url": "https://other.example/", "id": "foreign"},
        {"type": "page", "url": "about:blank", "id": "blank"},
        {"type": "browser_ui", "url": "chrome://omnibox-popup", "id": "ui"},
    ]
    monkeypatch.setattr(login, "_get_json", lambda _url: entries)
    assert login._find_debug_target(9222) is None

    entries.append({
        "type": "page", "url": "https://accounts.google.com/signin", "id": "google"
    })
    assert login._find_debug_target(9222)["id"] == "google"


@pytest.mark.parametrize("host", [
    ".notion.com", ".notion.so", ".app.notion.com", "www.notion.so",
])
def test_cookie_domain_accepted_only_for_configured_https_origins(host):
    assert login._cookie_matches_configured_origin(
        host, [login.BASE_URL, login.notion_app_origin()]
    )


@pytest.mark.parametrize("host", [
    ".notion.site", ".other.example", ".notion.com.evil.example",
    "evil@notion.com", "..notion.com", ".notion.com.", "",
    "foo..notion.com", "notion.com:443", "notion%2ecom",
])
def test_cookie_domain_rejects_foreign_and_malformed_host(host):
    assert not login._cookie_matches_configured_origin(
        host, [login.BASE_URL, login.notion_app_origin()]
    )


class _FakeCDP:
    def __init__(self, href, *, cookies=None, fallback=None, location_error=False):
        self.href = href
        self.cookies = cookies if cookies is not None else []
        self.fallback = fallback if fallback is not None else []
        self.location_error = location_error
        self.methods = []
        self.closed = False

    def call(self, method, params=None):
        expression = (params or {}).get("expression", "")
        self.methods.append((method, expression))
        if method == "Runtime.evaluate":
            if expression != "location.href":
                raise AssertionError("Untrusted document.cookie was evaluated")
            if self.location_error:
                raise RuntimeError("navigation incomplete")
            return {"result": {"value": self.href}}
        if method == "Page.navigate":
            raise AssertionError("An existing tab was navigated")
        if method == "Network.getCookies":
            return {"cookies": self.cookies}
        if method == "Network.getAllCookies":
            return {"cookies": self.fallback}
        return {}

    def close(self):
        self.closed = True


def _test_cdp(monkeypatch, fake):
    monkeypatch.setattr(login, "_find_debug_target", lambda _port: {
        "type": "page",
        "webSocketDebuggerUrl": "ws://127.0.0.1:9222/devtools/page/test",
    })
    monkeypatch.setattr(login, "_CDPClient", lambda _url: fake)
    return fake


@pytest.mark.parametrize("untrusted_href", [
    "https://unrelated.example/", "https://accounts.google.com/signin",
    "about:blank", "http://app.notion.com/ai", "https://team.notion.site/page",
])
def test_untrusted_or_unfinished_navigation_yields_no_cookies(
    monkeypatch, untrusted_href
):
    fake = _test_cdp(monkeypatch, _FakeCDP(
        untrusted_href,
        fallback=[{
            "domain": ".notion.com", "name": "token_v2", "value": "DO_NOT_READ"
        }],
    ))
    assert login._extract_cookies_from_cdp(9222) == {}
    assert fake.closed
    assert all(
        method not in {"Page.navigate", "Network.getCookies", "Network.getAllCookies"}
        for method, _ in fake.methods
    )
    assert ("Runtime.evaluate", "document.cookie") not in fake.methods


def test_failed_origin_check_never_reads_cookies(monkeypatch):
    fake = _test_cdp(monkeypatch, _FakeCDP(
        "https://other.example", location_error=True
    ))
    with pytest.raises(RuntimeError, match="navigation incomplete"):
        login._extract_cookies_from_cdp(9222)
    assert fake.closed
    assert all(
        method not in {"Network.getCookies", "Network.getAllCookies", "Page.navigate"}
        for method, _ in fake.methods
    )


def test_mixed_notion_and_foreign_cookies_do_not_import_foreign_token(monkeypatch):
    fake = _test_cdp(monkeypatch, _FakeCDP(
        "https://app.notion.com/ai",
        fallback=[
            {"domain": ".other.example", "name": "token_v2", "value": "FOREIGN"},
            {"domain": "evil@notion.com", "name": "token_v2", "value": "FOREIGN"},
            {"domain": ".notion.site", "name": "token_v2", "value": "FOREIGN"},
            {"domain": ".notion.com", "name": "token_v2", "value": "NOTION_TEST"},
        ],
    ))
    result = login._extract_cookies_from_cdp(9222)
    assert result["token_v2"] == "NOTION_TEST"
    assert fake.closed
    assert ("Runtime.evaluate", "document.cookie") not in fake.methods


def test_conflicting_notion_token_domains_fail_closed(monkeypatch):
    fake = _test_cdp(monkeypatch, _FakeCDP(
        "https://www.notion.so/ai",
        cookies=[
            {"domain": ".notion.com", "name": "token_v2", "value": "FIRST"},
            {"domain": ".notion.so", "name": "token_v2", "value": "SECOND"},
        ],
    ))
    with pytest.raises(RuntimeError, match="Conflicting Notion identity cookies"):
        login._extract_cookies_from_cdp(9222)
    assert fake.closed


def test_cookie_fallback_skipped_when_network_has_valid_token(monkeypatch):
    fake = _test_cdp(monkeypatch, _FakeCDP(
        "https://www.notion.so/ai",
        cookies=[{
            "domain": ".notion.so", "name": "token_v2", "value": "VALID_TEST"
        }],
    ))
    assert login._extract_cookies_from_cdp(9222)["token_v2"] == "VALID_TEST"
    assert not any(method == "Network.getAllCookies" for method, _ in fake.methods)
    assert fake.closed


@pytest.mark.parametrize("host", ["com", ".com", "so", ".so"])
def test_cookie_domain_does_not_accept_public_suffix(host):
    assert not login._cookie_matches_configured_origin(
        host, [login.BASE_URL, login.notion_app_origin()]
    )


def test_only_minimal_identity_cookies_are_collected(monkeypatch):
    fake = _test_cdp(monkeypatch, _FakeCDP(
        "https://app.notion.com/ai",
        cookies=[
            {"domain": ".notion.com", "name": "token_v2", "value": "TEST_TOKEN"},
            {"domain": ".notion.com", "name": "notion_user_id", "value": "TEST_USER"},
            {"domain": ".notion.com", "name": "prefs", "value": "A"},
            {"domain": ".notion.so", "name": "prefs", "value": "B"},
            {"domain": ".notion.com", "name": "session_state", "value": "PRIVATE"},
        ],
    ))
    cookies = login._extract_cookies_from_cdp(9222)
    assert cookies == {"token_v2": "TEST_TOKEN", "notion_user_id": "TEST_USER"}
    assert fake.closed


def test_http_request_replays_only_token_cookie_not_auxiliary_state():
    session = login._session_for_token("TEST_TOKEN", {
        "token_v2": "TEST_TOKEN",
        "notion_user_id": "TEST_USER",
        "prefs": "DO_NOT_REPLAY",
        "session_state": "DO_NOT_REPLAY",
    })
    assert "Cookie" not in session.headers
    request = login.requests.Request("POST", login.BASE_URL + "/api/v3/getSpaces")
    prepared = session.prepare_request(request)
    assert prepared.headers.get("Cookie") == "token_v2=TEST_TOKEN"
    assert set(session.cookies.keys()) == {"token_v2"}


def test_google_oauth_target_rejects_userinfo_and_custom_port(monkeypatch):
    entries = [
        {"type": "page", "url": "https://evil@accounts.google.com/signin",
         "id": "userinfo"},
        {"type": "page", "url": "https://accounts.google.com:8443/signin",
         "id": "port"},
    ]
    monkeypatch.setattr(login, "_get_json", lambda _url: entries)
    assert login._find_debug_target(9222) is None
    entries.append({
        "type": "page", "url": "https://accounts.google.com/signin",
        "id": "valid",
    })
    assert login._find_debug_target(9222)["id"] == "valid"


def test_oauth_polling_retries_without_reusing_foreign_cookies(monkeypatch):
    observations = iter([{}, {}, {"token_v2": "TEST_NOTION_TOKEN"}])
    attempts = []
    monkeypatch.setattr(login, "_extract_cookies_from_cdp", lambda port: (
        attempts.append(port) or next(observations)
    ))
    monkeypatch.setattr(login.time, "sleep", lambda _delay: None)
    cookies = login._wait_for_browser_cookies(9222, timeout_seconds=5)
    assert cookies == {"token_v2": "TEST_NOTION_TOKEN"}
    assert attempts == [9222, 9222, 9222]
