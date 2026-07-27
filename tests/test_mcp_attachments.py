import asyncio
import base64
import os
import tempfile
from pathlib import Path
import pytest
from unittest.mock import patch

from app.attachments.errors import AttachmentError
from app.attachments.security import AttachmentPolicy
from app.mcp_server import create_server, prepare_mcp_file_attachments

def test_prepare_mcp_file_attachments_empty():
    # Empty list/None returns empty list even if disabled
    with patch.object(AttachmentPolicy, 'from_env') as mock_policy:
        mock_policy.return_value = AttachmentPolicy(enabled=False)
        assert prepare_mcp_file_attachments(None) == []
        assert prepare_mcp_file_attachments([]) == []

def test_prepare_mcp_file_attachments_disabled():
    with patch.object(AttachmentPolicy, 'from_env') as mock_policy:
        mock_policy.return_value = AttachmentPolicy(enabled=False)
        with pytest.raises(AttachmentError) as exc_info:
            prepare_mcp_file_attachments(["some_file.pdf"])
        assert exc_info.value.code == "attachments_disabled"

def test_prepare_mcp_file_attachments_limit_exceeded():
    with patch.object(AttachmentPolicy, 'from_env') as mock_policy:
        mock_policy.return_value = AttachmentPolicy(enabled=True, max_attachments_per_request=1)
        with pytest.raises(AttachmentError) as exc_info:
            prepare_mcp_file_attachments(["file1.pdf", "file2.pdf"])
        assert exc_info.value.code == "too_many_attachments"

def test_prepare_mcp_file_attachments_not_exists():
    with patch.object(AttachmentPolicy, 'from_env') as mock_policy:
        mock_policy.return_value = AttachmentPolicy(enabled=True)
        with pytest.raises(AttachmentError) as exc_info:
            prepare_mcp_file_attachments(["nonexistent_file_xyz.pdf"])
        assert exc_info.value.code == "attachment_not_found"

def test_prepare_mcp_file_attachments_directory():
    with patch.object(AttachmentPolicy, 'from_env') as mock_policy:
        mock_policy.return_value = AttachmentPolicy(enabled=True)
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(AttachmentError) as exc_info:
                prepare_mcp_file_attachments([tmpdir])
            assert exc_info.value.code == "invalid_attachment_type"

def test_prepare_mcp_file_attachments_unsupported_mime():
    with patch.object(AttachmentPolicy, 'from_env') as mock_policy:
        mock_policy.return_value = AttachmentPolicy(enabled=True, allowed_mime_types={"application/pdf"})
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
            tmp.write(b"hello")
            tmp_name = tmp.name
        try:
            with pytest.raises(AttachmentError) as exc_info:
                prepare_mcp_file_attachments([tmp_name])
            assert exc_info.value.code == "unsupported_attachment_type"
        finally:
            os.unlink(tmp_name)

def test_prepare_mcp_file_attachments_oversized():
    with patch.object(AttachmentPolicy, 'from_env') as mock_policy:
        mock_policy.return_value = AttachmentPolicy(enabled=True, max_attachment_bytes=5)
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(b"123456") # 6 bytes
            tmp_name = tmp.name
        try:
            with pytest.raises(AttachmentError) as exc_info:
                prepare_mcp_file_attachments([tmp_name])
            assert exc_info.value.code == "attachment_too_large"
        finally:
            os.unlink(tmp_name)

def test_prepare_mcp_file_attachments_success():
    with patch.object(AttachmentPolicy, 'from_env') as mock_policy:
        mock_policy.return_value = AttachmentPolicy(enabled=True, allowed_mime_types={"application/pdf"})
        pdf_content = b"%PDF-1.4\nhello\n%EOF"
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_content)
            tmp_name = tmp.name
        try:
            res = prepare_mcp_file_attachments([tmp_name])
            assert len(res) == 1
            assert res[0]["name"] == Path(tmp_name).name
            assert res[0]["content_type"] == "application/pdf"
            prefix = "data:application/pdf;base64,"
            assert res[0]["data"].startswith(prefix)
            encoded_part = res[0]["data"].split(",", 1)[1]
            assert base64.b64decode(encoded_part) == pdf_content
        finally:
            os.unlink(tmp_name)

def test_generated_mcp_schema_separates_local_paths_from_transferred_files():
    server = create_server(
        base_url="http://127.0.0.1:8120",
        api_key="test-key",
        timeout=30,
        host="127.0.0.1",
        port=8130,
        mcp_path="/mcp",
    )

    tools = asyncio.run(server.list_tools())
    by_name = {tool.name: tool for tool in tools}
    chat_properties = by_name["chat"].inputSchema["properties"]
    attachment_schema = chat_properties["attachments"]

    assert "Service-host local file paths" in attachment_schema["description"]
    assert "/mnt/data" in attachment_schema["description"]
    assert "format" not in attachment_schema
    assert "format" not in str(attachment_schema.get("anyOf", []))
    assert "staged_file_ids" in chat_properties
    assert "require_attachments" in chat_properties

    for tool_name in (
        "stage_file",
        "chat_with_file",
        "upload_file_to_page",
    ):
        file_schema = by_name[tool_name].inputSchema["properties"]["file"]
        assert file_schema["type"] == "string"
        assert file_schema["format"] == "file"


@pytest.mark.parametrize(
    ("suffix", "expected_type"),
    [
        (".md", "text/markdown"),
        (".txt", "text/plain"),
        (".patch", "text/x-diff"),
        (".yaml", "application/yaml"),
        (".py", "text/x-python"),
        (".docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
    ],
)
def test_prepare_mcp_file_attachments_supports_common_formats(suffix, expected_type):
    with patch.object(AttachmentPolicy, "from_env") as mock_policy:
        mock_policy.return_value = AttachmentPolicy(enabled=True)
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(b"sample")
            tmp_name = tmp.name
        try:
            result = prepare_mcp_file_attachments([tmp_name])
            assert result[0]["content_type"] == expected_type
        finally:
            os.unlink(tmp_name)


def test_prepare_mcp_file_attachments_rejects_executable_by_default():
    with patch.object(AttachmentPolicy, "from_env") as mock_policy:
        mock_policy.return_value = AttachmentPolicy(enabled=True)
        with tempfile.NamedTemporaryFile(suffix=".exe", delete=False) as tmp:
            tmp.write(b"MZ")
            tmp_name = tmp.name
        try:
            with pytest.raises(AttachmentError) as exc_info:
                prepare_mcp_file_attachments([tmp_name])
            assert exc_info.value.code == "unsupported_attachment_type"
        finally:
            os.unlink(tmp_name)
