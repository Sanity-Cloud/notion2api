import unittest
from unittest.mock import MagicMock, patch

from app.conversation import ConversationManager
from app.notion_client import NotionOpusAPI, NotionUpstreamError
from app.schemas import ChatCompletionRequest
from app.thread_title import resolve_requested_thread_title


class ResolveRequestedThreadTitleTests(unittest.TestCase):
    def test_prefers_top_level_chat_title(self) -> None:
        title = resolve_requested_thread_title(
            chat_title="RepoAI AID - repo - task - abc123",
            title="ignored title",
            session_name="ignored session",
            metadata={
                "repo_ai_thread_title": "ignored metadata",
                "chat_title": "ignored metadata chat",
            },
        )
        self.assertEqual(title, "RepoAI AID - repo - task - abc123")

    def test_falls_back_to_metadata_repo_ai_thread_title(self) -> None:
        title = resolve_requested_thread_title(
            metadata={"repo_ai_thread_title": "RepoAI metadata title"},
        )
        self.assertEqual(title, "RepoAI metadata title")

    def test_strips_prompt_protocol_label(self) -> None:
        title = resolve_requested_thread_title(
            chat_title="CHAT THREAD TITLE: RepoAI - repo - Thread Titles - abc123",
        )
        self.assertEqual(title, "RepoAI - repo - Thread Titles - abc123")

    def test_schema_accepts_explicit_title_fields(self) -> None:
        req = ChatCompletionRequest(
            model="claude-sonnet4.6",
            messages=[{"role": "user", "content": "hello"}],
            chat_title="RepoAI explicit",
            title="RepoAI alias",
            session_name="RepoAI session",
            metadata={"repo_ai_thread_title": "RepoAI metadata"},
        )
        self.assertEqual(req.chat_title, "RepoAI explicit")
        self.assertEqual(req.title, "RepoAI alias")
        self.assertEqual(req.session_name, "RepoAI session")


class ConversationTitleTests(unittest.TestCase):
    def test_new_conversation_uses_requested_title(self) -> None:
        manager = ConversationManager()
        manager.db_path = self._temp_db_path()
        manager._init_db()
        conversation_id = manager.new_conversation(title="RepoAI AID - repo - task - abc123")
        self.assertEqual(
            manager.get_conversation_title(conversation_id),
            "RepoAI AID - repo - task - abc123",
        )

    def test_set_conversation_title_updates_existing_row(self) -> None:
        manager = ConversationManager()
        manager.db_path = self._temp_db_path()
        manager._init_db()
        conversation_id = manager.new_conversation()
        manager.set_conversation_title(conversation_id, "RepoAI renamed")
        self.assertEqual(manager.get_conversation_title(conversation_id), "RepoAI renamed")

    @staticmethod
    def _temp_db_path() -> str:
        import tempfile
        handle = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        handle.close()
        return handle.name


class StreamResponseThreadTitleTests(unittest.TestCase):
    def test_explicit_thread_title_disables_generate_title(self) -> None:
        client = NotionOpusAPI({"user_id": "u1", "space_id": "s1", "token_v2": "t1"})
        transcript = [{"type": "config", "value": {"type": "workflow"}}]

        with patch.object(client, "_to_notion_transcript", return_value=transcript), \
             patch.object(client, "_resolve_thread_type", return_value="workflow"), \
             patch.object(client, "_with_thread_type", side_effect=lambda t, tt: t), \
             patch.object(client, "_resolve_request_profile", return_value={"precreate_thread": False, "create_thread": True, "is_partial_transcript": False, "include_debug_overrides": False}), \
             patch.object(client, "_build_cookie_header", return_value=""), \
             patch.object(client, "set_thread_title", return_value=True) as mock_set_title, \
             patch.object(client, "_scraper", MagicMock()) as mock_scraper:
            mock_response = MagicMock()
            mock_response.status_code = 200
            with patch("app.notion_client.parse_stream", return_value=iter([
                {"type": "content", "text": "ok"},
                {"type": "stream_complete"},
            ])), patch(
                "app.notion_client._create_notion_http_session",
                return_value=mock_scraper,
            ):
                mock_scraper.post.return_value = mock_response
                list(client.stream_response(
                    transcript,
                    thread_id="thread-123",
                    persist_remote_chat=True,
                    thread_title="RepoAI AID - repo - task - abc123",
                ))

        payload = mock_scraper.post.call_args.kwargs["json"]
        self.assertFalse(payload["generateTitle"])
        self.assertEqual(mock_set_title.call_count, 3)
        mock_set_title.assert_called_with("thread-123", "RepoAI AID - repo - task - abc123")

    def test_unsaved_transactions_title_failure_does_not_abort_stream(self) -> None:
        client = NotionOpusAPI({"user_id": "u1", "space_id": "s1", "token_v2": "t1"})
        transcript = [{"type": "config", "value": {"type": "workflow"}}]

        def _failing_title(thread_id: str, title: str) -> bool:
            raise NotionUpstreamError(
                "Notion thread title update returned HTTP 400.",
                status_code=400,
                retriable=False,
                response_excerpt=(
                    '{"isNotionError":true,"name":"ValidationError",'
                    '"clientData":{"type":"unsaved_transactions"}}'
                ),
            )

        with patch.object(client, "_to_notion_transcript", return_value=transcript), \
             patch.object(client, "_resolve_thread_type", return_value="workflow"), \
             patch.object(client, "_with_thread_type", side_effect=lambda t, tt: t), \
             patch.object(client, "_resolve_request_profile", return_value={
                 "precreate_thread": False,
                 "create_thread": True,
                 "is_partial_transcript": False,
                 "include_debug_overrides": False,
             }), \
             patch.object(client, "_build_cookie_header", return_value=""), \
             patch.object(client, "set_thread_title", side_effect=_failing_title) as mock_set_title, \
             patch.object(client, "_scraper", MagicMock()) as mock_scraper:
            mock_response = MagicMock()
            mock_response.status_code = 200
            with patch("app.notion_client.parse_stream", return_value=iter([
                {"type": "content", "text": "grounded search result"},
                {"type": "stream_complete"},
            ])), patch(
                "app.notion_client._create_notion_http_session",
                return_value=mock_scraper,
            ):
                mock_scraper.post.return_value = mock_response
                events = list(client.stream_response(
                    transcript,
                    thread_id="thread-new",
                    persist_remote_chat=True,
                    thread_title="Search phone monitoring transcripts",
                ))

        self.assertGreaterEqual(mock_set_title.call_count, 1)
        self.assertTrue(any(
            isinstance(event, dict)
            and event.get("type") == "content"
            and "grounded search result" in str(event.get("text") or "")
            for event in events
        ))


class DefaultModelRequestTests(unittest.TestCase):
    def test_omitted_model_defaults_to_terra(self) -> None:
        req = ChatCompletionRequest(
            messages=[{"role": "user", "content": "hello"}],
        )
        self.assertEqual(req.model, "terra")


if __name__ == "__main__":
    unittest.main()
