from __future__ import annotations

import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient

from app.server import app
from app.attachments.normalizer import normalize_responses_input
from app.attachments.security import AttachmentPolicy, validate_content_type
from app.config import API_KEY


class ResponsesAttachmentTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        self.client.__enter__()
        self.auth_headers = {}
        if API_KEY:
            self.auth_headers = {"Authorization": f"Bearer {API_KEY}"}

    def tearDown(self):
        try:
            self.client.__exit__(None, None, None)
        except Exception:
            pass

    def test_no_attachments_preserves_behavior(self):
        payload = {"model": "claude-sonnet4.6", "input": "Hello"}

        with patch("app.notion_client.NotionOpusAPI.stream_response") as mock_stream:
            mock_stream.return_value = iter([{"type": "final_content", "text": "ok"}])
            resp = self.client.post("/v1/responses", json=payload, headers=self.auth_headers)

        self.assertEqual(resp.status_code, 200)
        mock_stream.assert_called()
        attachments = mock_stream.call_args.kwargs.get("attachments")
        self.assertIsNone(attachments)

    def test_input_file_normalized_and_passed(self):
        input_value = [{"type": "message", "role": "user", "content": [{"type": "text", "text": "Hi"}, {"type": "input_file", "filename": "a.csv", "file_data": "YQ"}]}]
        messages, attachments = normalize_responses_input(input_value)
        self.assertEqual(len(attachments), 1)
        self.assertEqual(messages[0]["content"], "Hi")

    def test_top_level_attachments_supported(self):
        input_value = "Hello"
        top = [{"type": "file", "file_data": "YQ", "filename": "a.csv"}]
        messages, attachments = normalize_responses_input(input_value, top)
        self.assertEqual(len(attachments), 1)

    def test_response_route_forwards_attachments_to_notion_stream(self):
        payload = {
            "model": "claude-sonnet4.6",
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "Summarize"},
                        {
                            "type": "input_file",
                            "filename": "a.csv",
                            "file_data": "YQ==",
                            "content_type": "text/csv",
                        },
                    ],
                }
            ],
        }
        with patch("app.api.attachment_guard.HOST", "127.0.0.1"):
            with patch.dict(
                "os.environ",
                {
                    "ENABLE_ATTACHMENTS": "true",
                    "ALLOW_REMOTE_ATTACHMENT_URLS": "false",
                },
            ):
                with patch("app.notion_client.NotionOpusAPI.stream_response") as mock_stream:
                    mock_stream.return_value = iter([{"type": "final_content", "text": "ok"}])
                    resp = self.client.post("/v1/responses", json=payload, headers=self.auth_headers)

        self.assertEqual(resp.status_code, 200)
        mock_stream.assert_called()
        attachments = mock_stream.call_args.kwargs.get("attachments")
        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].data, "YQ==")

    def test_attachments_disabled_returns_400(self):
        with patch.dict("os.environ", {"ENABLE_ATTACHMENTS": "false"}):
            policy = AttachmentPolicy.from_env()
            self.assertFalse(policy.enabled)
            payload = {"model": "claude-sonnet4.6", "input": [{"type": "message", "role": "user", "content": [{"type": "input_file", "filename": "a.csv", "file_data": "YQ"}]}]}
            resp = self.client.post("/v1/responses", json=payload, headers=self.auth_headers)
        self.assertEqual(resp.status_code, 400)
        self.assertIn("attachments_disabled", resp.text)

    def test_enabled_invalid_base64_returns_400(self):
        payload = {
            "model": "claude-sonnet4.6",
            "input": [
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "Summarize"},
                        {
                            "type": "input_file",
                            "filename": "bad.csv",
                            "file_data": "YQ",
                            "content_type": "text/csv",
                        },
                    ],
                }
            ],
        }
        with patch("app.api.attachment_guard.HOST", "127.0.0.1"):
            with patch.dict(
                "os.environ",
                {
                    "ENABLE_ATTACHMENTS": "true",
                    "ALLOW_REMOTE_ATTACHMENT_URLS": "false",
                },
            ):
                resp = self.client.post("/v1/responses", json=payload, headers=self.auth_headers)

        self.assertEqual(resp.status_code, 400)
        code = resp.json().get("error", {}).get("code")
        self.assertTrue(code == "invalid_request_error" or code.startswith("invalid_attachment"), code)
        self.assertNotIn("YQ", resp.text)

    def test_unsupported_mime_returns_400(self):
        input_value = [{"type": "message", "role": "user", "content": [{"type": "input_file", "filename": "a.csv", "file_data": "YQ", "content_type": "application/x-msdownload"}]}]
        messages, attachments = normalize_responses_input(input_value)
        self.assertEqual(len(attachments), 1)
        # validate using AttachmentPolicy
        policy = AttachmentPolicy.from_env()
        ok = True
        try:
            for att in attachments:
                validate_content_type(att.content_type, policy)
        except Exception:
            ok = False
        self.assertFalse(ok)

    def test_attachment_only_input_gets_fallback_prompt(self):
        input_value = [{"type": "file", "file_data": "YQ", "filename": "a.csv"}]
        messages, attachments = normalize_responses_input(input_value)
        self.assertTrue(any(m.get("content") for m in messages))

    def test_raw_base64_not_leaked(self):
        input_value = [{"type": "message", "role": "user", "content": [{"type": "text", "text": "Hi"}, {"type": "input_file", "filename": "a.csv", "file_data": "YQ=="}]}]
        messages, attachments = normalize_responses_input(input_value)
        self.assertEqual(len(attachments), 1)
        # simulate error stringification
        s = str(messages) + str(attachments)
        self.assertNotIn("YQ==", s)


if __name__ == "__main__":
    unittest.main()
