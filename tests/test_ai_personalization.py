from __future__ import annotations

from types import SimpleNamespace

from app.notion_client import NotionOpusAPI


class _Response:
    status_code = 200
    text = "{}"


class _Scraper:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def post(self, url, *, headers, json, timeout):
        self.calls.append(
            {
                "url": url,
                "headers": headers,
                "json": json,
                "timeout": timeout,
            }
        )
        return _Response()


def _client() -> NotionOpusAPI:
    client = NotionOpusAPI(
        {
            "profile_name": "sanity-management-personaltouch",
            "workspace_key": "sanity-management",
            "space_id": "fe8b13aa-3ad2-811e-8292-0003b78a02f9",
            "user_id": "124d872b-594c-8171-aabc-00026ee9f5e5",
            "user_name": "user",
            "cookies": {},
        }
    )
    return client


def test_set_ai_personalization_uses_exact_space_view_and_prompt(monkeypatch) -> None:
    client = _client()
    scraper = _Scraper()
    client._scraper = scraper
    page_id = "eecf3821-3e23-4d4f-a4e4-b8454511cf68"
    space_view_id = "3abb13aa-3ad2-81da-94a8-0006cb60837a"
    record_map = {
        "space_view": {
            space_view_id: {
                "value": {
                    "space_id": client.space_id,
                    "user_id": client.user_id,
                    "settings": {},
                }
            }
        },
        "prompt": {},
    }
    monkeypatch.setattr(client, "check_page_access", lambda _page: {"accessible": True})
    monkeypatch.setattr(client, "_fetch_account_record_map", lambda: record_map)
    monkeypatch.setattr(
        client,
        "get_ai_personalization",
        lambda: {
            "ok": True,
            "profile_name": client.profile_name,
            "workspace_key": client.workspace_key,
            "space_id": client.space_id,
            "user_id": client.user_id,
            "space_view_id": space_view_id,
            "name": "AIgentBee Curator",
            "context_page_id": page_id,
            "customization_items": ["cat"],
            "has_already_seen_personalization_settings_modal": True,
        },
    )

    result = client.set_ai_personalization(
        name="AIgentBee Curator",
        context_page_id=page_id,
        customization_items=["cat"],
    )

    assert result["verified"] is True
    assert len(scraper.calls) == 1
    call = scraper.calls[0]
    assert call["url"].endswith("/saveTransactionsFanout")
    operations = call["json"]["transactions"][0]["operations"]
    assert operations[0]["pointer"] == {
        "table": "space_view",
        "id": space_view_id,
        "spaceId": client.space_id,
    }
    settings = operations[0]["args"]["agent_personalization_settings"]
    assert settings["name"] == "AIgentBee Curator"
    assert settings["context_page_id"] == page_id
    assert settings["customization_items"] == ["cat"]
    assert operations[1]["pointer"]["table"] == "prompt"
    assert operations[1]["command"] == "set"
    assert operations[1]["args"]["parent_id"] == page_id
    assert operations[1]["args"]["prompt_type"] == "instruction"


def test_resolve_space_view_prefers_exact_user_binding() -> None:
    client = _client()
    records = {
        "space_view": {
            "11111111-1111-1111-1111-111111111111": {
                "value": {"space_id": client.space_id, "settings": {}}
            },
            "22222222-2222-2222-2222-222222222222": {
                "value": {
                    "space_id": client.space_id,
                    "user_id": client.user_id,
                    "settings": {},
                }
            },
        }
    }

    record_id, _ = client._resolve_space_view_record(records)

    assert record_id == "22222222-2222-2222-2222-222222222222"

class _UniqueThenSuccessScraper:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def post(self, url, *, headers, json, timeout):
        self.calls.append({"url": url, "headers": headers, "json": json, "timeout": timeout})
        if len(self.calls) == 1:
            return SimpleNamespace(
                status_code=400,
                text='{"name":"PostgresUniqueViolation"}',
            )
        return SimpleNamespace(status_code=200, text="{}")


def test_set_ai_personalization_reuses_workspace_prompt_on_unique_conflict(
    monkeypatch,
) -> None:
    client = _client()
    scraper = _UniqueThenSuccessScraper()
    client._scraper = scraper
    page_id = "eecf3821-3e23-4d4f-a4e4-b8454511cf68"
    space_view_id = "3abb13aa-3ad2-81da-94a8-0006cb60837a"
    record_map = {
        "space_view": {
            space_view_id: {
                "value": {
                    "space_id": client.space_id,
                    "user_id": client.user_id,
                    "settings": {},
                }
            }
        },
        "prompt": {},
    }
    monkeypatch.setattr(client, "check_page_access", lambda _page: {"accessible": True})
    monkeypatch.setattr(client, "_fetch_account_record_map", lambda: record_map)
    monkeypatch.setattr(
        client,
        "get_ai_personalization",
        lambda: {
            "ok": True,
            "profile_name": client.profile_name,
            "workspace_key": client.workspace_key,
            "space_id": client.space_id,
            "user_id": client.user_id,
            "space_view_id": space_view_id,
            "name": "AIgentBee Curator",
            "context_page_id": page_id,
            "customization_items": ["flower"],
            "has_already_seen_personalization_settings_modal": True,
        },
    )

    result = client.set_ai_personalization(
        name="AIgentBee Curator",
        context_page_id=page_id,
        customization_items=["flower"],
    )

    assert result["verified"] is True
    assert result["reused_workspace_prompt"] is True
    assert result["prompt_id"] == ""
    assert len(scraper.calls) == 2
    first_operations = scraper.calls[0]["json"]["transactions"][0]["operations"]
    second_operations = scraper.calls[1]["json"]["transactions"][0]["operations"]
    assert len(first_operations) == 2
    assert len(second_operations) == 1
    assert second_operations[0]["pointer"]["table"] == "space_view"
