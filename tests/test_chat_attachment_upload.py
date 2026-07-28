import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient
from app.server import app
from app.config import API_KEY

class ChatAttachmentRouteTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        # Ensure FastAPI lifespan events run to initialize state (account_pool, etc.)
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
        payload = {
            "model": "claude-sonnet4.6",
            "messages": [{"role": "user", "content": "Hello"}],
            "stream": False,
        }
        resp = self.client.post("/v1/chat/completions", json=payload, headers=self.auth_headers)
        self.assertIn(resp.status_code, (200, 401, 503))
        if resp.status_code == 401:
            body = resp.json()
            self.assertEqual(body["error"]["code"], "NOTION_401")

    def test_attachments_disabled_returns_400(self):
        payload = {
            "model": "claude-sonnet4.6",
            "messages": [{"role": "user", "content": [{"type": "text", "text": "Hi"}, {"type": "input_file", "filename": "a.csv", "file_data": "YQ"}]}],
            "stream": False,
        }
        with patch.dict("os.environ", {"ENABLE_ATTACHMENTS": "false"}):
            resp = self.client.post("/v1/chat/completions", json=payload, headers=self.auth_headers)
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json().get("error", {}).get("code"), "attachments_disabled")

    def test_enabled_invalid_base64_returns_400(self):
        payload = {
            "model": "claude-sonnet4.6",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Summarize"},
                        {
                            "type": "input_file",
                            "filename": "bad.csv",
                            "file_data": "YQ",
                            "content_type": "text/csv",
                        },
                    ],
                }
            ],
            "stream": False,
        }
        with patch.dict("os.environ", {"ENABLE_ATTACHMENTS": "true"}):
            resp = self.client.post("/v1/chat/completions", json=payload, headers=self.auth_headers)

        self.assertEqual(resp.status_code, 400)
        code = resp.json().get("error", {}).get("code")
        self.assertTrue(code == "invalid_request_error" or code.startswith("invalid_attachment"), code)
        self.assertNotIn("YQ", resp.text)

    def test_content_array_file_normalized_and_stream_calls_uploader(self):
        payload = {
            "model": "claude-sonnet4.6",
            "messages": [{"role": "user", "content": [{"type": "text", "text": "Summarize"}, {"type": "input_file", "filename": "notes.csv", "file_data": "YSxi"}]}],
            "stream": False,
        }
        # Enable attachments for this test
        with patch.dict("os.environ", {"ENABLE_ATTACHMENTS": "true"}):
            with patch("app.notion_client.NotionOpusAPI.stream_response") as mock_stream:
                mock_stream.return_value = iter([{"type": "final_content", "text": "ok"}])
                resp = self.client.post("/v1/chat/completions", json=payload, headers=self.auth_headers)
                # downstream may return 200 or 503 depending on account pool; ensure call reached our patched method
                self.assertIn(resp.status_code, (200, 503))
                mock_stream.assert_called()

if __name__ == '__main__':
    unittest.main()
