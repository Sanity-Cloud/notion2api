import unittest

import pytest

from app.attachments.models import DEFAULT_ATTACHMENT_PROMPT
from app.attachments.normalizer import (
    PromptValidationError,
    normalize_chat_messages,
    normalize_responses_input,
)


class AttachmentNormalizerTests(unittest.TestCase):
    def test_top_level_attachment_adds_fallback_prompt(self) -> None:
        messages, attachments = normalize_chat_messages(
            [{"role": "user", "content": ""}],
            [{"name": "order.pdf", "content_type": "application/pdf", "data": "data:application/pdf;base64,Zm9v"}],
        )

        self.assertEqual(messages[-1]["content"], DEFAULT_ATTACHMENT_PROMPT)
        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].name, "order.pdf")
        self.assertEqual(attachments[0].content_type, "application/pdf")
        self.assertEqual(attachments[0].source, "inline_data")

    def test_content_array_extracts_text_and_file_data(self) -> None:
        messages, attachments = normalize_chat_messages(
            [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Summarize this."},
                        {
                            "type": "input_file",
                            "filename": "notes.csv",
                            "mime_type": "text/csv",
                            "file_data": "YSxiCjEsMgo=",
                        },
                    ],
                }
            ]
        )

        self.assertEqual(messages[0]["content"], "Summarize this.")
        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].name, "notes.csv")
        self.assertEqual(attachments[0].content_type, "text/csv")
        self.assertEqual(attachments[0].source, "inline_data")

    def test_image_url_extracts_remote_url(self) -> None:
        messages, attachments = normalize_chat_messages(
            [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "What is in the image?"},
                        {
                            "type": "image_url",
                            "image_url": {"url": "https://example.com/image.png"},
                        },
                    ],
                }
            ]
        )

        self.assertEqual(messages[0]["content"], "What is in the image?")
        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].source, "remote_url")
        self.assertEqual(attachments[0].url, "https://example.com/image.png")
        self.assertEqual(attachments[0].content_type, "image/png")

    def test_responses_input_preserves_attachment_parts(self) -> None:
        messages, attachments = normalize_responses_input(
            [
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "Read this."},
                        {
                            "type": "input_file",
                            "filename": "brief.pdf",
                            "mime_type": "application/pdf",
                            "file_data": "data:application/pdf;base64,Zm9v",
                        },
                    ],
                }
            ]
        )

        self.assertEqual(messages, [{"role": "user", "content": "Read this."}])
        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].name, "brief.pdf")
        self.assertEqual(attachments[0].source, "inline_data")

    def test_local_windows_path_is_detected(self) -> None:
        _, attachments = normalize_chat_messages(
            [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "file",
                            "filename": "local.pdf",
                            "mime_type": "application/pdf",
                            "path": r"C:\\Temp\\local.pdf",
                        }
                    ],
                }
            ]
        )

        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].source, "local_path")
        self.assertEqual(attachments[0].path, r"C:\\Temp\\local.pdf")


if __name__ == "__main__":
    unittest.main()


def test_prompt_validation_rejects_non_string_without_echoing_value():
    secret = {"private": "do-not-echo"}

    with pytest.raises(PromptValidationError) as caught:
        normalize_chat_messages([{"role": "system", "content": secret}])

    assert caught.value.code == "invalid_prompt_type"
    assert "do-not-echo" not in str(caught.value)


def test_prompt_validation_rejects_control_character_without_echoing_prompt():
    prompt = "private-prefix\x00private-suffix"

    with pytest.raises(PromptValidationError) as caught:
        normalize_chat_messages([{"role": "user", "content": prompt}])

    assert caught.value.code == "invalid_prompt_control_character"
    assert "private-prefix" not in str(caught.value)


def test_prompt_validation_rejects_oversized_field(monkeypatch):
    monkeypatch.setenv("MAX_PROMPT_FIELD_CHARS", "8")

    with pytest.raises(PromptValidationError) as caught:
        normalize_chat_messages([{"role": "user", "content": "123456789"}])

    assert caught.value.code == "prompt_too_large"
    assert caught.value.param == "messages[0].content"


def test_structured_attachment_data_is_not_concatenated_into_prompt_text():
    marker = "Considering file options /home/oai/skills/pdfs/SKILL.md ---FILES---"
    messages, attachments = normalize_chat_messages(
        [{"role": "user", "content": "Review the attachment."}],
        [
            {
                "type": "file",
                "name": "record.txt",
                "content_type": "text/plain",
                "data": marker,
            }
        ],
    )

    assert messages[0]["content"] == "Review the attachment."
    assert marker not in messages[0]["content"]
    assert attachments[0].data == marker
