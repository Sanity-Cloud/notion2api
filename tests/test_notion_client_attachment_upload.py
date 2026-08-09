import unittest
from unittest.mock import Mock, patch

from app.api.notion import CreatePageResponse
from app.notion_client import NotionOpusAPI, NotionUpstreamError
from app.attachments.models import InputAttachment, UploadedAttachment
from app.attachments.notion_upload import NotionAttachmentUploadError


class NotionClientAttachmentTests(unittest.TestCase):
    def setUp(self):
        self.client = NotionOpusAPI({"token_v2": "tok", "space_id": "space", "user_id": "user"})
        # replace real scraper with a mock
        self.client._scraper = Mock()

        # mock requests.Session.post to avoid real network calls from fresh sessions
        self.session_post_patcher = patch("requests.Session.post")
        self.mock_session_post = self.session_post_patcher.start()

        # mock response for requests.Session.post
        mock_resp = Mock(status_code=200)
        mock_resp.close = Mock()
        self.mock_session_post.return_value = mock_resp

    def tearDown(self):
        self.session_post_patcher.stop()

    def test_page_urls_include_web_and_desktop_variants(self):
        page_id = "b3824128-1465-4419-ad59-e63617c1e383"

        self.assertEqual(
            self.client._page_url(page_id),
            "https://www.notion.so/b382412814654419ad59e63617c1e383",
        )
        self.assertEqual(
            self.client._page_app_url(page_id),
            "notion://www.notion.so/b382412814654419ad59e63617c1e383",
        )

    def test_create_page_response_exposes_desktop_link(self):
        response = CreatePageResponse(
            ok=True,
            page_id="page-id",
            page_url="https://www.notion.so/page-id",
            page_app_url="notion://www.notion.so/page-id",
            parent_page_id="parent-id",
            title="Example",
        )

        self.assertTrue(response.page_app_url.startswith("notion://"))

    def test_create_thread_persists_markdown_chat_type(self):
        response = Mock(status_code=200)
        with patch("app.notion_client.requests.post", return_value=response) as post:
            self.assertTrue(self.client._create_thread("thread-1", "markdown-chat"))

        payload = post.call_args.kwargs["json"]
        operation = payload["transactions"][0]["operations"][0]
        self.assertEqual(operation["args"]["type"], "markdownChat")

    def test_request_upload_descriptor_payload_and_response(self):
        # mock response
        resp = Mock()
        resp.status_code = 200
        resp.json.return_value = {
            "signedUploadPostUrl": "https://upload.test/u",
            "fields": {"k": "v"},
            "url": "attachment:file-1:block-1",
            "signedGetUrl": "https://signed.test/a",
            "chatId": "chat-1",
        }
        self.client._scraper.post.return_value = resp

        desc = self.client.request_upload_descriptor(name="a.txt", content_type="text/csv", size=10, thread_id="t", create_thread=False)
        self.assertEqual(desc["upload_url"], "https://upload.test/u")
        self.assertEqual(desc["file_id"], "file-1")
        self.assertEqual(desc["attachment_url"], "attachment:file-1:block-1")
        self.assertEqual(desc["signed_get_url"], "https://signed.test/a")
        self.assertEqual(desc["chat_id"], "chat-1")
        self.assertEqual(desc["fields"], {"k": "v"})
        # verify payload sent
        args, kwargs = self.client._scraper.post.call_args
        self.assertIn("getUploadFileUrlForAssistantChatTranscriptUpload", args[0])
        body = kwargs.get("json")
        self.assertEqual(body["name"], "a.txt")
        self.assertEqual(body["contentType"], "text/csv")
        self.assertEqual(body["contentLength"], 10)
        self.assertEqual(body["assistantChatTranscriptSessionPointer"], {"spaceId": "space", "table": "thread", "id": "t"})
        self.assertNotIn("fileName", body)
        self.assertNotIn("size", body)
        self.assertNotIn("threadId", body)

    def test_zip_upload_descriptor_payload(self):
        resp = Mock(status_code=200)
        resp.json.return_value = {"signedUploadPostUrl": "https://upload.test/zip", "url": "attachment:file-zip:block-zip"}
        self.client._scraper.post.return_value = resp
        self.client.request_upload_descriptor(name="source.zip", content_type="application/zip", size=123, thread_id="thread-zip", create_thread=False)
        body = self.client._scraper.post.call_args.kwargs["json"]
        self.assertEqual(body["contentType"], "application/x-zip-compressed")
        self.assertTrue(body["allowUnsupportedTypes"])

    def test_request_upload_descriptor_nested_file_id_and_aliases(self):
        resp = Mock()
        resp.status_code = 200
        resp.json.return_value = {
            "signedUploadPostUrl": "https://upload.test/u2",
            "postFields": {"p": "q"},
            "file": {"id": "file-2"},
            "url": "attachment:file-2:block-2",
        }
        self.client._scraper.post.return_value = resp

        desc = self.client.request_upload_descriptor(name="b.pdf", content_type="application/pdf", size=22, thread_id="t", create_thread=True)
        self.assertEqual(desc["upload_url"], "https://upload.test/u2")
        self.assertEqual(desc["file_id"], "file-2")
        self.assertEqual(desc["attachment_url"], "attachment:file-2:block-2")
        self.assertEqual(desc["fields"], {"p": "q"})
        body = self.client._scraper.post.call_args.kwargs["json"]
        self.assertEqual(body["name"], "b.pdf")
        self.assertEqual(body["contentType"], "application/pdf")
        self.assertEqual(body["contentLength"], 22)
        self.assertNotIn("allowUnsupportedTypes", body)

    def test_request_upload_descriptor_zip_uses_zip_specific_payload(self):
        resp = Mock()
        resp.status_code = 200
        resp.json.return_value = {
            "signedUploadPostUrl": "https://upload.test/zip",
            "fields": {},
            "url": "attachment:file-zip:block-zip",
        }
        self.client._scraper.post.return_value = resp

        self.client.request_upload_descriptor(
            name="source.zip",
            content_type="application/zip",
            size=44,
            thread_id="t",
            create_thread=False,
        )

        body = self.client._scraper.post.call_args.kwargs["json"]
        self.assertNotEqual(body["name"], "source.zip")
        self.assertTrue(body["name"].endswith("zip"))
        self.assertEqual(body["contentType"], "application/x-zip-compressed")
        self.assertEqual(body["contentLength"], 44)
        self.assertTrue(body["allowUnsupportedTypes"])

    def test_request_upload_descriptor_missing_required_fields_fails(self):
        resp = Mock()
        resp.status_code = 200
        resp.json.return_value = {"metadata": {"a": 1}}
        self.client._scraper.post.return_value = resp
        with self.assertRaises(NotionUpstreamError):
            self.client.request_upload_descriptor(name="a.txt", content_type="text/csv", size=10, thread_id="t", create_thread=False)

    def test_request_upload_descriptor_http_error(self):
        resp = Mock()
        resp.status_code = 500
        resp.text = "server"
        self.client._scraper.post.return_value = resp
        with self.assertRaises(NotionUpstreamError):
            self.client.request_upload_descriptor(name="a.txt", content_type="text/csv", size=10, thread_id=None, create_thread=False)

    def test_perform_multipart_upload_uses_requests(self):
        descriptor = {"upload_url": "https://upload.test/u", "fields": {"k": "v"}}
        # patch requests.post
        import requests
        real_post = requests.post
        try:
            requests.post = Mock()
            mock_resp = Mock()
            mock_resp.status_code = 204
            requests.post.return_value = mock_resp
            self.client.perform_multipart_upload(descriptor=descriptor, name="a.txt", data=b"x", content_type="text/csv")
            requests.post.assert_called()
        finally:
            requests.post = real_post

    def test_enqueue_task_and_get_status(self):
        # enqueue returns task id
        enq = Mock()
        enq.status_code = 200
        enq.json.return_value = {"taskId": "task-1"}
        self.client._scraper.post.return_value = enq
        tid = self.client.enqueue_attachment_processing(attachment_url="attachment:file-1:block", thread_id="t")
        self.assertEqual(tid, "task-1")
        payload = self.client._scraper.post.call_args.kwargs["json"]
        self.assertEqual(payload["task"]["eventName"], "processAgentAttachment")
        self.assertEqual(payload["task"]["request"]["url"], "attachment:file-1:block")
        self.assertEqual(payload["task"]["request"]["aiSessionPointer"], {"spaceId": "space", "table": "thread", "id": "t"})
        self.assertEqual(payload["task"]["request"]["source"], "user_upload")
        self.assertEqual(payload["task"]["cellRouting"]["spaceIds"], ["space"])

        # get task status
        status_resp = Mock()
        status_resp.status_code = 200
        status_resp.json.return_value = {"results": [{"state": "success", "status": {"result": {"type": "success", "data": {"pages": 1}}}}]}
        self.client._scraper.post.return_value = status_resp
        st = self.client.get_task_status("task-1")
        self.assertEqual(st.get("status"), "completed")
        self.assertTrue(st.get("success"))
        self.assertEqual(st.get("data"), {"pages": 1})

    def test_get_signed_read_url(self):
        r = Mock()
        r.status_code = 200
        r.json.return_value = {"signedUrls": ["https://signed/test"]}
        self.client._scraper.post.return_value = r
        url = self.client.get_signed_read_url("attachment:file-1:block", thread_id="t", download_name="a.csv")
        self.assertEqual(url, "https://signed/test")
        payload = self.client._scraper.post.call_args.kwargs["json"]
        self.assertEqual(
            payload["urls"][0],
            {
                "url": "attachment:file-1:block",
                "download": False,
                "downloadName": "a.csv",
                "permissionRecord": {"table": "thread", "id": "t", "spaceId": "space"},
            },
        )

    def test_missing_signed_url_raises(self):
        r = Mock()
        r.status_code = 200
        r.json.return_value = {}
        self.client._scraper.post.return_value = r
        with self.assertRaises(NotionUpstreamError):
            self.client.get_signed_read_url("attachment:missing:block", thread_id="t", download_name="missing.csv")

    def test_exception_text_does_not_include_sensitive_values(self):
        resp = Mock()
        resp.status_code = 200
        resp.json.return_value = {"metadata": {"token_v2": "secret", "raw": "bytes"}}
        self.client._scraper.post.return_value = resp
        with self.assertRaises(NotionUpstreamError) as ctx:
            self.client.request_upload_descriptor(name="a.txt", content_type="text/csv", size=10, thread_id="t", create_thread=False)
        text = str(ctx.exception)
        self.assertNotIn("secret", text)
        self.assertNotIn("bytes", text)

    def test_stream_response_without_attachments_preserves_payload_shape(self):
        resp = Mock()
        resp.status_code = 200
        resp.text = ""
        resp.close = Mock()
        scraper = Mock()
        scraper.cookies.clear = Mock()
        scraper.post.return_value = resp

        transcript = [{"type": "config", "value": {"type": "workflow", "model": "gpt-4"}}, {"type": "user", "value": "hi"}]

        stream_chunks = [{"type": "chunk", "value": "ok"}, {"type": "stream_complete"}]
        with patch("app.notion_client._create_notion_http_session", return_value=scraper), patch("app.notion_client.parse_stream", return_value=stream_chunks), patch("app.notion_client._resolve_thread_persistence", return_value={"persist": True, "generate_title": False, "save_all_thread_operations": False, "set_unread_state": False, "delete_after_stream": False}):
            chunks = list(self.client.stream_response(transcript, thread_id="thread-1"))

        self.assertEqual(chunks, [{"type": "chunk", "value": "ok"}])
        payload = scraper.post.call_args.kwargs["json"]
        self.assertNotIn("attachments", payload)
        self.assertEqual(payload["threadId"], "thread-1")

    def test_stream_response_with_attachments_calls_uploader_and_builds_attachment_steps(self):
        resp = Mock()
        resp.status_code = 200
        resp.text = ""
        resp.close = Mock()
        scraper = Mock()
        scraper.cookies.clear = Mock()
        scraper.post.return_value = resp

        transcript = [{"type": "config", "value": {"type": "workflow", "model": "gpt-4"}}, {"type": "user", "value": "hi"}]
        attachments = [InputAttachment(name="a.csv", content_type="text/csv", source="inline_data", data="YQpi")]
        uploaded = [
            UploadedAttachment(
                name="a.csv",
                content_type="text/csv",
                size_bytes=3,
                source="inline_data",
                file_id="file-1",
                thread_mounted=True,
                attachment_url="https://files.test/a.csv",
                signed_get_url="https://signed.test/a.csv",
                task_id="task-1",
                metadata={"fileSizeBytes": 3, "contentType": "text/csv", "source": "inline_data", "taskId": "task-1", "fileId": "file-1"},
            )
        ]

        uploader_instance = Mock()
        uploader_instance.upload_attachments.return_value = (uploaded, "thread-actual")
        stream_chunks = [{"type": "chunk", "value": "ok"}, {"type": "stream_complete"}]
        with patch("app.notion_client.NotionAttachmentUploader", return_value=uploader_instance), patch("app.notion_client._create_notion_http_session", return_value=scraper), patch("app.notion_client.parse_stream", return_value=stream_chunks), patch("app.notion_client._resolve_thread_persistence", return_value={"persist": True, "generate_title": False, "save_all_thread_operations": False, "set_unread_state": False, "delete_after_stream": False}):
            chunks = list(self.client.stream_response(transcript, thread_id="thread-1", attachments=attachments))

        self.assertEqual(chunks, [{"type": "chunk", "value": "ok"}])
        uploader_instance.upload_attachments.assert_called_once()
        payload = scraper.post.call_args.kwargs["json"]
        self.assertEqual(payload["threadId"], "thread-actual")
        self.assertFalse(payload["createThread"])
        self.assertEqual(payload["threadType"], "markdown-chat")
        self.assertEqual(payload["createdSource"], "ai_module")
        config = next(item for item in payload["transcript"] if item.get("type") == "config")
        self.assertEqual(config["value"]["type"], "markdown-chat")
        uploader_instance.upload_attachments.assert_called_once_with(
            thread_id="thread-1",
            attachments=attachments,
            create_thread=False,
        )
        self.assertNotIn("threadParentPointer", payload)
        self.assertEqual(
            payload["attachments"],
            [
                {
                    "type": "attachment",
                    "fileName": "a.csv",
                    "contentType": "text/csv",
                    "fileUrl": "https://files.test/a.csv",
                }
            ],
        )
        transcript_steps = [item for item in payload["transcript"] if item.get("type") == "attachment"]
        self.assertTrue(transcript_steps)
        self.assertEqual(transcript_steps[0]["fileName"], "a.csv")
        self.assertEqual(transcript_steps[0]["contentType"], "text/csv")
        self.assertEqual(transcript_steps[0]["fileUrl"], "https://files.test/a.csv")
        self.assertIn("id", transcript_steps[0])
        self.assertNotIn("value", transcript_steps[0])
        self.assertNotIn("attachmentUrl", transcript_steps[0])
        self.assertNotIn("https://signed.test/a.csv", str(payload))
        self.assertNotIn("token_v2", str(payload))
        self.assertNotIn("bytes", str(payload))

    def test_new_attachment_chat_precreates_markdown_chat_before_upload(self):
        response = Mock(status_code=200, text="")
        response.close = Mock()
        scraper = Mock()
        scraper.cookies.clear = Mock()
        scraper.post.return_value = response

        transcript = [{"type": "config", "value": {"type": "workflow", "model": "gpt-4"}}]
        attachments = [InputAttachment(name="a.csv", content_type="text/csv", source="inline_data", data="YQpi")]
        uploaded = [UploadedAttachment(name="a.csv", content_type="text/csv", size_bytes=3, source="inline_data", file_id="file-1", thread_mounted=True, attachment_url="https://files.test/a.csv")]

        uploader_instance = Mock()
        uploader_instance.upload_attachments.side_effect = lambda **kwargs: (uploaded, kwargs["thread_id"])
        with patch.object(self.client, "_create_thread", return_value=True) as create_thread, patch(
            "app.notion_client.NotionAttachmentUploader", return_value=uploader_instance
        ), patch("app.notion_client.requests.Session", return_value=scraper), patch(
            "app.notion_client._create_notion_http_session", return_value=scraper
        ), patch(
            "app.notion_client.parse_stream",
            return_value=[{"type": "chunk", "value": "ok"}, {"type": "stream_complete"}],
        ), patch(
            "app.notion_client._resolve_thread_persistence",
            return_value={"persist": True, "generate_title": True, "save_all_thread_operations": True, "set_unread_state": True, "delete_after_stream": False},
        ):
            list(self.client.stream_response(transcript, attachments=attachments))

        create_thread.assert_called_once()
        created_thread_id, created_thread_type = create_thread.call_args.args
        self.assertEqual(created_thread_type, "markdown-chat")
        uploader_instance.upload_attachments.assert_called_once_with(
            thread_id=created_thread_id,
            attachments=attachments,
            create_thread=False,
        )
        payload = scraper.post.call_args.kwargs["json"]
        self.assertEqual(payload["threadId"], created_thread_id)
        self.assertFalse(payload["createThread"])
        self.assertTrue(payload["isPartialTranscript"])
        self.assertEqual(payload["threadType"], "markdown-chat")
        self.assertEqual(payload["createdSource"], "ai_module")
        self.assertNotIn("threadParentPointer", payload)

    def test_new_ephemeral_attachment_chat_precreates_markdown_chat(self):
        response = Mock(status_code=200, text="")
        response.close = Mock()
        scraper = Mock()
        scraper.cookies.clear = Mock()
        scraper.post.return_value = response

        transcript = [{"type": "config", "value": {"type": "workflow", "model": "gpt-4"}}]
        attachments = [InputAttachment(name="a.csv", content_type="text/csv", source="inline_data", data="YQpi")]
        uploaded = [UploadedAttachment(name="a.csv", content_type="text/csv", size_bytes=3, source="inline_data", file_id="file-1", thread_mounted=True, attachment_url="https://files.test/a.csv")]

        uploader_instance = Mock()
        uploader_instance.upload_attachments.side_effect = lambda **kwargs: (uploaded, kwargs["thread_id"])
        with patch.object(self.client, "_create_thread", return_value=True) as create_thread, patch(
            "app.notion_client.NotionAttachmentUploader", return_value=uploader_instance
        ), patch("app.notion_client.requests.Session", return_value=scraper), patch(
            "app.notion_client._create_notion_http_session", return_value=scraper
        ), patch(
            "app.notion_client.parse_stream",
            return_value=[{"type": "chunk", "value": "ok"}, {"type": "stream_complete"}],
        ), patch(
            "app.notion_client._resolve_thread_persistence",
            return_value={"persist": False, "generate_title": False, "save_all_thread_operations": False, "set_unread_state": False, "delete_after_stream": True},
        ):
            list(self.client.stream_response(transcript, attachments=attachments))

        create_thread.assert_called_once()
        created_thread_id, created_thread_type = create_thread.call_args.args
        self.assertEqual(created_thread_type, "markdown-chat")
        uploader_instance.upload_attachments.assert_called_once_with(
            thread_id=created_thread_id,
            attachments=attachments,
            create_thread=False,
        )
        payload = scraper.post.call_args.kwargs["json"]
        self.assertEqual(payload["threadId"], created_thread_id)
        self.assertFalse(payload["createThread"])
        self.assertTrue(payload["isPartialTranscript"])
        self.assertEqual(payload["threadType"], "markdown-chat")
        self.assertNotIn("threadParentPointer", payload)

    def test_stream_response_attachment_failure_wraps_upstream_error(self):
        self.client._scraper.cookies = Mock()
        self.client._scraper.cookies.clear = Mock()
        attachments = [InputAttachment(name="a.csv", content_type="text/csv", source="inline_data", data="YQpi")]
        uploader_instance = Mock()
        uploader_instance.upload_attachments.side_effect = NotionAttachmentUploadError("upload failed", reason="upload_failed")
        with patch("app.notion_client.NotionAttachmentUploader", return_value=uploader_instance), patch("app.notion_client._resolve_thread_persistence", return_value={"persist": True, "generate_title": False, "save_all_thread_operations": False, "set_unread_state": False, "delete_after_stream": False}):
            with self.assertRaises(NotionUpstreamError) as ctx:
                list(self.client.stream_response([{"type": "config", "value": {"model": "gpt-4"}}], thread_id="thread-1", attachments=attachments))

        self.assertIn("Attachment upload staging failed", str(ctx.exception))
        self.assertNotIn("upload failed", str(ctx.exception).lower())


if __name__ == '__main__':
    unittest.main()



def test_current_zip_workflow_contract():
    from app.attachments.models import UploadedAttachment
    from app.notion_client import NOTION_CLIENT_VERSION, NotionOpusAPI

    client = NotionOpusAPI(
        {
            "token_v2": "token",
            "space_id": "space-1",
            "user_id": "user-1",
            "workspace_name": "Sanity Management",
            "timezone": "America/Chicago",
        }
    )
    response = Mock(status_code=200, text="")
    client._scraper = Mock()
    client._scraper.post.return_value = response
    uploader = Mock()
    uploader.upload_attachments.return_value = (
        [
            UploadedAttachment(
                name="source.zip",
                content_type="application/x-zip-compressed",
                size_bytes=4096,
                source="local_path",
                file_id="file-1",
                thread_mounted=True,
                attachment_url="attachment:file-1:source.zip",
            )
        ],
        "resolved-thread",
    )
    transcript = [
        {"id": "config", "type": "config", "value": {"type": "workflow", "model": "gpt-4"}},
        {"id": "context", "type": "context", "value": {"surface": "workflows"}},
        {"id": "user", "type": "user", "value": [["Review the repository."]]},
    ]
    active_context = {
        "id": "active-context",
        "type": "context",
        "value": {
            "surface": "full_page_chat",
            "spaceViewId": "space-view-1",
            "agentName": "SanityBee Worker",
        },
    }
    persistence = {
        "persist": True,
        "generate_title": False,
        "save_all_thread_operations": True,
        "set_unread_state": True,
        "delete_after_stream": False,
    }

    with patch("app.notion_client.NotionAttachmentUploader", return_value=uploader), patch.object(
        client, "_build_active_workspace_context_step", return_value=active_context
    ), patch.object(
        client,
        "_wait_for_thread_file_mount",
        return_value={"file_ids": ["file-1"]},
    ) as wait_for_mount, patch.object(client, "warm_script_agent_cache"), patch(
        "app.notion_client._resolve_thread_persistence", return_value=persistence
    ), patch(
        "app.notion_client.parse_stream",
        return_value=iter([
            {"type": "content", "text": "ok"},
            {"type": "stream_complete", "finished_at": 1},
        ]),
    ), patch(
        "app.notion_client._create_notion_http_session", return_value=client._scraper
    ):
        events = list(
            client.stream_response(
                transcript,
                attachments=[object()],
                persist_remote_chat=True,
                computer_use_review=True,
            )
        )

    assert events == [{"type": "content", "text": "ok"}]
    uploader.upload_attachments.assert_called_once()
    assert uploader.upload_attachments.call_args.kwargs["create_thread"] is True
    persistence_calls = [
        call
        for call in client._scraper.post.call_args_list
        if call.args and call.args[0].endswith("/saveTransactionsFanout")
    ]
    assert len(persistence_calls) == 1
    persistence_payload = persistence_calls[0].kwargs["json"]
    add_steps = persistence_payload["transactions"][0]
    assert add_steps["debug"]["userAction"] == (
        "WorkflowActions.addStepsToExistingThreadAndRun"
    )
    persisted_set_operations = [
        operation
        for operation in add_steps["operations"]
        if operation["command"] == "set"
    ]
    assert [operation["args"]["step"]["type"] for operation in persisted_set_operations] == [
        "context",
        "updated-config",
        "context",
        "computer-file",
        "updated-config",
        "user",
    ]
    assert add_steps["operations"][-1]["command"] == "listAfterMulti"

    inference_call = client._scraper.post.call_args_list[-1]
    assert inference_call.args[0] == "https://www.notion.so/api/v3/runInferenceTranscript"
    headers = inference_call.kwargs["headers"]
    assert headers["notion-client-version"] == NOTION_CLIENT_VERSION == "23.13.20260805.0803"
    assert headers["notion-audit-log-platform"] == "web"
    assert headers["origin"] == "https://www.notion.so"
    payload = inference_call.kwargs["json"]
    assert payload["threadId"] == "resolved-thread"
    assert payload["threadType"] == "workflow"
    assert payload["createThread"] is False
    assert payload["isPartialTranscript"] is True
    assert payload["createdSource"] == "full_page_chat"
    assert "supportsCustomAgentNudgeTranscriptStep" not in payload
    assert "threadParentPointer" not in payload
    assert "attachments" not in payload
    assert [step["type"] for step in payload["transcript"]] == [
        "context",
        "updated-config",
        "context",
        "updated-config",
        "config",
        "user",
    ]
    config = payload["transcript"][4]["value"]
    assert config["enableComputer"] is True
    assert config["enableScriptAgent"] is True
    assert config["modelFromUser"] is False
    assert "model" not in config
    assert "enableRunAgentTool" not in config
    assert config["enableAgentThreadTools"] is False
    assert not any(
        step.get("type") == "computer-file" for step in payload["transcript"]
    )
    persisted_file = next(
        operation["args"]["step"]
        for operation in persisted_set_operations
        if operation["args"]["step"]["type"] == "computer-file"
    )
    assert persisted_file["contentType"] == "application/x-zip-compressed"
    assert persisted_file["metadata"] == {
        "fileSize": 4096,
        "attachmentSource": "user_upload",
    }
    assert sum(step["type"] == "user" for step in payload["transcript"]) == 1


def test_zip_descriptor_uses_current_web_identity():
    import re
    from app.notion_client import NotionOpusAPI

    client = NotionOpusAPI({"token_v2": "token", "space_id": "space", "user_id": "user"})
    response = Mock(status_code=200, text="")
    response.json.return_value = {
        "fileId": "file-1",
        "url": "https://upload.invalid",
        "signedGetUrl": "https://download.invalid",
        "chatId": "thread-2",
        "fields": {"key": "value"},
    }
    client._scraper = Mock()
    client._scraper.post.return_value = response

    descriptor = client.request_upload_descriptor(
        name="source.zip",
        content_type="application/zip",
        size=123,
        thread_id="thread-1",
        create_thread=True,
    )

    call = client._scraper.post.call_args
    assert call.args[0] == "https://www.notion.so/api/v3/getUploadFileUrlForAssistantChatTranscriptUpload"
    request = call.kwargs["json"]
    assert re.fullmatch(r"[0-9a-f-]{36}\.zip", request["name"])
    assert request["contentType"] == "application/x-zip-compressed"
    assert request["allowUnsupportedTypes"] is True
    assert request["createThread"] is True
    headers = call.kwargs["headers"]
    assert headers["x-notion-active-user-header"] == "user"
    assert headers["x-notion-space-id"] == "space"
    assert "notion-audit-log-platform" not in headers
    assert descriptor["chat_id"] == "thread-2"


def test_attachment_task_semantic_error_is_not_success():
    from app.notion_client import NotionOpusAPI

    client = NotionOpusAPI({"token_v2": "token", "space_id": "space", "user_id": "user"})
    response = Mock(status_code=200, text="")
    response.json.return_value = {
        "results": [
            {
                "state": "success",
                "status": {
                    "result": {
                        "type": "success",
                        "data": {
                            "code": "UNSUPPORTED_CONTENT_TYPE",
                            "message": "Unsupported content type",
                        },
                    }
                },
            }
        ]
    }
    client._scraper = Mock()
    client._scraper.post.return_value = response

    result = client.get_task_status("task-1")

    assert result["status"] == "failed"
    assert result["success"] is False
    assert result["error_code"] == "UNSUPPORTED_CONTENT_TYPE"


def test_wait_for_thread_file_mount_retries_until_visible(monkeypatch):
    from app.notion_client import NotionOpusAPI

    client = NotionOpusAPI(
        {"token_v2": "token", "space_id": "space-1", "user_id": "user-1"}
    )
    first = Mock(status_code=200)
    first.json.return_value = {
        "recordMap": {"thread": {"thread-1": {"value": {"value": {"file_ids": []}}}}}
    }
    second = Mock(status_code=200)
    second.json.return_value = {
        "recordMap": {
            "thread": {
                "thread-1": {
                    "value": {"value": {"file_ids": ["file-1"], "messages": []}}
                }
            }
        }
    }
    client._scraper = Mock()
    client._scraper.post.side_effect = [first, second]
    monkeypatch.setenv("NOTION_ATTACHMENT_THREAD_READY_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("NOTION_ATTACHMENT_THREAD_READY_POLL_SECONDS", "0.05")

    record = client._wait_for_thread_file_mount("thread-1", ["file-1"])

    assert record["file_ids"] == ["file-1"]
    assert client._scraper.post.call_count == 2


def test_wait_for_thread_file_mount_fails_closed(monkeypatch):
    import pytest
    from app.notion_client import NotionOpusAPI, NotionUpstreamError

    client = NotionOpusAPI(
        {"token_v2": "token", "space_id": "space-1", "user_id": "user-1"}
    )
    response = Mock(status_code=200)
    response.json.return_value = {
        "recordMap": {"thread": {"thread-1": {"value": {"value": {"file_ids": []}}}}}
    }
    client._scraper = Mock()
    client._scraper.post.return_value = response
    monkeypatch.setenv("NOTION_ATTACHMENT_THREAD_READY_TIMEOUT_SECONDS", "0")

    with pytest.raises(NotionUpstreamError) as exc_info:
        client._wait_for_thread_file_mount("thread-1", ["file-1"])

    assert "attachment_thread_not_ready" in exc_info.value.response_excerpt


def test_image_generation_reference_attachment_preserves_workflow_and_prompt_order():
    client = NotionOpusAPI({"token_v2": "tok", "space_id": "space", "user_id": "user"})
    response = Mock(status_code=200, text="")
    response.close = Mock()
    scraper = Mock()
    scraper.cookies.clear = Mock()
    scraper.post.return_value = response
    transcript = [
        {"id": "cfg", "type": "config", "value": {"type": "workflow", "model": "orchid-muffin", "enableAgentGenerateImage": True}},
        {"id": "ctx", "type": "context", "value": {"surface": "ai_module"}},
        {"id": "prompt", "type": "agent-prebuilt-prompt", "args": {"type": "image_generation_mode"}, "promptType": "image_generation_mode", "value": [["Use this reference image."]]},
    ]
    attachments = [InputAttachment(name="lock.png", content_type="image/png", source="inline_data", data="aW1hZ2U=")]
    uploaded = [UploadedAttachment(name="lock.png", content_type="image/png", size_bytes=5, source="inline_data", file_id="file-1", thread_mounted=True, attachment_url="attachment:file-1:lock.png")]
    uploader_instance = Mock()
    uploader_instance.upload_attachments.side_effect = lambda **kwargs: (uploaded, kwargs["thread_id"])
    with patch.object(client, "_create_thread", return_value=True) as create_thread, patch("app.notion_client.NotionAttachmentUploader", return_value=uploader_instance), patch("app.notion_client._create_notion_http_session", return_value=scraper), patch("app.notion_client.parse_stream", return_value=[{"type": "chunk", "value": "ok"}, {"type": "stream_complete"}]), patch("app.notion_client._resolve_thread_persistence", return_value={"persist": False, "generate_title": False, "save_all_thread_operations": False, "set_unread_state": False, "delete_after_stream": True}):
        list(client.stream_response(transcript, attachments=attachments, persist_remote_chat=False))
    created_thread_id, created_thread_type = create_thread.call_args.args
    assert created_thread_type == "workflow"
    payload = scraper.post.call_args.kwargs["json"]
    assert payload["threadType"] == "workflow"
    step_types = [step.get("type") for step in payload["transcript"]]
    assert step_types[-2:] == ["attachment", "agent-prebuilt-prompt"]
    assert payload["transcript"][-1]["promptType"] == "image_generation_mode"


def test_attachment_task_metadata_matches_native_image_reference_shape():
    from app.attachments.notion_upload import NotionAttachmentUploader

    notion = Mock()
    notion.get_task_status.return_value = {
        "status": "completed",
        "success": True,
        "data": {
            "fileSizeBytes": 247825,
            "contentType": "image/png",
            "width": 640,
            "height": 360,
            "stepMetadata": {
                "width": 640,
                "height": 360,
                "moderation": {"status": "passed"},
                "guardrail": {"attachmentRisk": "skipped", "inferenceId": "inf-1"},
                "fileSizeBytes": 247825,
                "aiTraceId": "trace-1",
                "estimatedTokens": {"openai": 1105, "anthropic": 307.2},
            },
        },
    }
    uploader = NotionAttachmentUploader(notion, poll_interval=0, poll_timeout=1)
    result = uploader.wait_attachment_task("task-1")
    metadata = uploader.build_attachment_step_metadata(
        {"fileSizeBytes": 247825, "contentType": "image/png"},
        task_data=result["data"],
    )

    assert result["data"]["stepMetadata"]["width"] == 640
    assert metadata == {
        "width": 640,
        "height": 360,
        "moderation": {"status": "passed"},
        "guardrail": {"attachmentRisk": "skipped", "inferenceId": "inf-1"},
        "fileSizeBytes": 247825,
        "aiTraceId": "trace-1",
        "estimatedTokens": {"openai": 1105, "anthropic": 307.2},
        "attachmentSource": "user_upload",
    }
