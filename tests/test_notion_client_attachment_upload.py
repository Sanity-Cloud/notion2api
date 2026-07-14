import unittest
from unittest.mock import Mock, patch

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
        with patch("app.notion_client.requests.Session", return_value=scraper), patch("app.notion_client.cloudscraper", Mock(create_scraper=Mock(return_value=scraper))), patch("app.notion_client.parse_stream", return_value=stream_chunks), patch("app.notion_client._resolve_thread_persistence", return_value={"persist": True, "generate_title": False, "save_all_thread_operations": False, "set_unread_state": False, "delete_after_stream": False}):
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
        with patch("app.notion_client.NotionAttachmentUploader", return_value=uploader_instance), patch("app.notion_client.requests.Session", return_value=scraper), patch("app.notion_client.cloudscraper", Mock(create_scraper=Mock(return_value=scraper))), patch("app.notion_client.parse_stream", return_value=stream_chunks), patch("app.notion_client._resolve_thread_persistence", return_value={"persist": True, "generate_title": False, "save_all_thread_operations": False, "set_unread_state": False, "delete_after_stream": False}):
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
        self.assertIn("attachments", payload)
        self.assertEqual(
            payload["attachments"][0],
            {
                "type": "attachment",
                "fileName": "a.csv",
                "contentType": "text/csv",
                "fileUrl": "https://files.test/a.csv",
            },
        )
        self.assertNotIn("attachmentUrl", payload["attachments"][0])
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

    def test_attachment_workflow_uses_ai_module_source_without_precreate(self):
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
            "app.notion_client.cloudscraper", Mock(create_scraper=Mock(return_value=scraper))
        ), patch(
            "app.notion_client.parse_stream",
            return_value=[{"type": "chunk", "value": "ok"}, {"type": "stream_complete"}],
        ), patch(
            "app.notion_client._resolve_thread_persistence",
            return_value={"persist": True, "generate_title": True, "save_all_thread_operations": True, "set_unread_state": True, "delete_after_stream": False},
        ):
            list(self.client.stream_response(transcript, attachments=attachments))

        create_thread.assert_not_called()
        created_thread_id = uploader_instance.upload_attachments.call_args.kwargs["thread_id"]
        uploader_instance.upload_attachments.assert_called_once_with(
            thread_id=created_thread_id,
            attachments=attachments,
            create_thread=True,
        )
        payload = scraper.post.call_args.kwargs["json"]
        self.assertEqual(payload["threadId"], created_thread_id)
        self.assertEqual(payload["threadType"], "markdown-chat")
        self.assertEqual(payload["createdSource"], "ai_module")

    def test_new_ephemeral_attachment_chat_creates_descriptor_thread(self):
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
            "app.notion_client.cloudscraper", Mock(create_scraper=Mock(return_value=scraper))
        ), patch(
            "app.notion_client.parse_stream",
            return_value=[{"type": "chunk", "value": "ok"}, {"type": "stream_complete"}],
        ), patch(
            "app.notion_client._resolve_thread_persistence",
            return_value={"persist": False, "generate_title": False, "save_all_thread_operations": False, "set_unread_state": False, "delete_after_stream": True},
        ):
            list(self.client.stream_response(transcript, attachments=attachments))

        create_thread.assert_not_called()
        created_thread_id = uploader_instance.upload_attachments.call_args.kwargs["thread_id"]
        uploader_instance.upload_attachments.assert_called_once_with(
            thread_id=created_thread_id,
            attachments=attachments,
            create_thread=True,
        )
        payload = scraper.post.call_args.kwargs["json"]
        self.assertEqual(payload["threadId"], created_thread_id)
        self.assertFalse(payload["createThread"])
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
