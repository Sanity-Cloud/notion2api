import base64
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from app.attachments.errors import AttachmentError
from app.attachments.loader import decode_inline_data, infer_content_type, load_attachment_data
from app.attachments.models import InputAttachment
from app.api.chat import _attachments_enabled_for_request
from app.core.internal_callers import (
    REPO_AI_CALLER_HEADER,
    REPO_AI_CALLER_VALUE,
    _is_loopback_host,
    is_repo_ai_internal_request,
)
from app.attachments.security import (
    AttachmentPolicy,
    normalize_content_type,
    validate_content_type,
    validate_remote_url,
    validate_size,
)


def _mock_request(
    client_host: str = "127.0.0.1",
    url_hostname: str = "127.0.0.1",
    headers: dict | None = None,
) -> MagicMock:
    req = MagicMock()
    req.client = MagicMock()
    req.client.host = client_host
    req.url = MagicMock()
    req.url.hostname = url_hostname
    req.headers = headers or {}
    return req


class AttachmentSecurityTests(unittest.TestCase):
    # ── _is_loopback_host ──────────────────────────────────────────

    def test_loopback_host_detection(self) -> None:
        self.assertTrue(_is_loopback_host("127.0.0.1"))
        self.assertTrue(_is_loopback_host("127.0.0.2"))
        self.assertTrue(_is_loopback_host("::1"))
        self.assertTrue(_is_loopback_host("localhost"))
        self.assertTrue(_is_loopback_host("[::1]"))
        self.assertFalse(_is_loopback_host("192.0.2.10"))
        self.assertFalse(_is_loopback_host(""))
        self.assertFalse(_is_loopback_host("0.0.0.0"))
        self.assertFalse(_is_loopback_host("203.0.113.5"))

    # ── is_repo_ai_internal_request ─────────────────────────────────

    def test_internal_request_recognized(self) -> None:
        req = _mock_request(headers={REPO_AI_CALLER_HEADER: REPO_AI_CALLER_VALUE})
        self.assertTrue(is_repo_ai_internal_request(req))

    def test_internal_request_missing_header(self) -> None:
        req = _mock_request()
        self.assertFalse(is_repo_ai_internal_request(req))

    def test_internal_request_wrong_header_value(self) -> None:
        req = _mock_request(headers={REPO_AI_CALLER_HEADER: "0"})
        self.assertFalse(is_repo_ai_internal_request(req))

    def test_internal_request_non_loopback_client(self) -> None:
        req = _mock_request(
            client_host="192.0.2.10",
            headers={REPO_AI_CALLER_HEADER: REPO_AI_CALLER_VALUE},
        )
        self.assertFalse(is_repo_ai_internal_request(req))

    def test_internal_request_non_loopback_url(self) -> None:
        req = _mock_request(
            url_hostname="192.0.2.10",
            headers={REPO_AI_CALLER_HEADER: REPO_AI_CALLER_VALUE},
        )
        self.assertFalse(is_repo_ai_internal_request(req))

    def test_internal_request_both_non_loopback(self) -> None:
        req = _mock_request(
            client_host="192.0.2.10",
            url_hostname="203.0.113.5",
            headers={REPO_AI_CALLER_HEADER: REPO_AI_CALLER_VALUE},
        )
        self.assertFalse(is_repo_ai_internal_request(req))

    # ── _attachments_enabled_for_request ────────────────────────────

    def test_attachments_enabled_when_policy_enabled(self) -> None:
        req = _mock_request()
        policy = AttachmentPolicy(enabled=True)
        self.assertTrue(_attachments_enabled_for_request(req, policy))

    def test_attachments_disabled_when_policy_off_and_external(self) -> None:
        req = _mock_request(client_host="192.0.2.10")
        policy = AttachmentPolicy(enabled=False)
        self.assertFalse(_attachments_enabled_for_request(req, policy))

    def test_attachments_enabled_for_internal_caller_despite_policy(self) -> None:
        req = _mock_request(headers={REPO_AI_CALLER_HEADER: REPO_AI_CALLER_VALUE})
        policy = AttachmentPolicy(enabled=False)
        self.assertTrue(_attachments_enabled_for_request(req, policy))


    def test_normalizes_common_content_type_aliases(self) -> None:
        self.assertEqual(normalize_content_type("text/x-markdown; charset=utf-8"), "text/markdown")
        self.assertEqual(normalize_content_type("application/x-yaml"), "application/yaml")
        self.assertEqual(normalize_content_type("application/x-zip-compressed"), "application/zip")

    def test_infers_deterministic_development_and_document_types(self) -> None:
        expected = {
            "README.md": "text/markdown",
            "review.patch": "text/x-diff",
            "notes.txt": "text/plain",
            "config.yaml": "application/yaml",
            "module.py": "text/x-python",
            "report.docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        }
        for name, content_type in expected.items():
            with self.subTest(name=name):
                self.assertEqual(infer_content_type(name), content_type)
                self.assertEqual(validate_content_type(content_type, AttachmentPolicy(enabled=True)), content_type)

    def test_extension_mapping_overrides_generic_os_guess(self) -> None:
        self.assertEqual(infer_content_type("review.patch", "text/plain"), "text/x-diff")
        self.assertEqual(infer_content_type("config.yaml", "application/octet-stream"), "application/yaml")

    def test_rejects_unsupported_content_type(self) -> None:
        with self.assertRaises(AttachmentError) as ctx:
            validate_content_type("application/x-msdownload", AttachmentPolicy(enabled=True))
        self.assertEqual(ctx.exception.code, "unsupported_attachment_type")

    def test_rejects_oversized_payload(self) -> None:
        with self.assertRaises(AttachmentError) as ctx:
            validate_size(11, AttachmentPolicy(enabled=True, max_attachment_bytes=10))
        self.assertEqual(ctx.exception.status_code, 413)

    def test_blocks_localhost_remote_url(self) -> None:
        with self.assertRaises(AttachmentError) as ctx:
            validate_remote_url("http://localhost/file.pdf", AttachmentPolicy(enabled=True))
        self.assertEqual(ctx.exception.code, "attachment_url_blocked")

    @patch("app.attachments.security.socket.getaddrinfo")
    def test_blocks_private_resolved_address(self, getaddrinfo) -> None:
        getaddrinfo.return_value = [(None, None, None, None, ("192.168.1.50", 0))]
        with self.assertRaises(AttachmentError) as ctx:
            validate_remote_url("https://example.test/file.pdf", AttachmentPolicy(enabled=True))
        self.assertEqual(ctx.exception.code, "attachment_url_blocked")

    @patch("app.attachments.security.socket.getaddrinfo")
    def test_allows_public_resolved_address(self, getaddrinfo) -> None:
        getaddrinfo.return_value = [(None, None, None, None, ("93.184.216.34", 0))]
        self.assertEqual(
            validate_remote_url("https://example.com/file.pdf", AttachmentPolicy(enabled=True)),
            "https://example.com/file.pdf",
        )

    def test_decode_data_url(self) -> None:
        payload = base64.b64encode(b"hello").decode("ascii")
        data, content_type = decode_inline_data(f"data:text/csv;base64,{payload}")
        self.assertEqual(data, b"hello")
        self.assertEqual(content_type, "text/csv")

    def test_load_inline_attachment(self) -> None:
        payload = base64.b64encode(b"a,b\n1,2\n").decode("ascii")
        loaded = load_attachment_data(
            InputAttachment(
                name="records.csv",
                content_type="text/csv",
                source="inline_data",
                data=payload,
            ),
            AttachmentPolicy(enabled=True),
        )

        self.assertEqual(loaded.name, "records.csv")
        self.assertEqual(loaded.content_type, "text/csv")
        self.assertEqual(loaded.data, b"a,b\n1,2\n")

    def test_local_path_disabled_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "file.pdf"
            path.write_bytes(b"%PDF")
            with self.assertRaises(AttachmentError) as ctx:
                load_attachment_data(
                    InputAttachment(
                        name="file.pdf",
                        content_type="application/pdf",
                        source="local_path",
                        path=str(path),
                    ),
                    AttachmentPolicy(enabled=True, allow_local_paths=False),
                )
            self.assertEqual(ctx.exception.code, "local_attachment_paths_disabled")


if __name__ == "__main__":
    unittest.main()
