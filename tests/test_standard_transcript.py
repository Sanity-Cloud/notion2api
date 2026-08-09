from app.conversation import apply_notion_ai_options, build_standard_transcript


def test_standard_transcript_answers_latest_user_turn():
    transcript = build_standard_transcript(
        [
            {"role": "user", "content": "first question"},
            {"role": "assistant", "content": "first answer"},
            {"role": "user", "content": "second question"},
        ],
        "gemini-2.5flash",
        {"user_id": "user-1", "space_id": "space-1"},
    )

    message_blocks = [block for block in transcript if block["type"] != "config" and block["type"] != "context"]

    assert [block["type"] for block in message_blocks] == ["user"]
    prompt = message_blocks[0]["value"][0][0]
    assert "[Previous conversation context]" in prompt
    assert "user: first question" in prompt
    assert "assistant: first answer" in prompt
    assert "[Current user request]" not in prompt
    assert prompt.endswith("second question")



def test_standard_transcript_routes_system_instructions_to_config():
    transcript = build_standard_transcript(
        [
            {"role": "system", "content": "governance instruction"},
            {"role": "user", "content": "review source.zip"},
        ],
        "orchid-muffin",
        {"user_id": "user-1", "space_id": "space-1"},
    )

    config = next(block for block in transcript if block["type"] == "config")
    user = next(block for block in transcript if block["type"] == "user")
    assert config["value"]["ephemeralInstructions"] == "governance instruction"
    assert user["value"][0][0] == "review source.zip"
    assert "System Instructions" not in user["value"][0][0]


def test_visualize_does_not_enable_image_generation():
    transcript = [{"type": "config", "value": {"enableAgentGenerateImage": False}}]

    apply_notion_ai_options(transcript, task="visualize")

    config = transcript[0]["value"]
    assert config["enableScriptAgent"] is True
    assert config["enableComputer"] is True
    assert config["enableAgentGenerateImage"] is False
    assert "data visualization" in config["ephemeralInstructions"]


def test_generate_image_enables_notion_image_generation():
    transcript = [{"type": "config", "value": {"enableAgentGenerateImage": False}}]

    apply_notion_ai_options(transcript, task="generate_image")

    config = transcript[0]["value"]
    assert config["enableScriptAgent"] is True
    assert config["enableComputer"] is True
    assert config["enableAgentGenerateImage"] is True
    assert "image-generation" in config["ephemeralInstructions"]


def test_generate_image_uses_native_prebuilt_prompt_shape():
    transcript = build_standard_transcript(
        [{"role": "user", "content": "create a graphic"}],
        "orchid-muffin",
        {"user_id": "user-1", "space_id": "space-1"},
    )

    apply_notion_ai_options(transcript, task="generate_image")

    prompt = transcript[-1]
    assert prompt["type"] == "agent-prebuilt-prompt"
    assert prompt["args"] == {"type": "image_generation_mode"}
    assert prompt["promptType"] == "image_generation_mode"
    assert prompt["locale"] == "en-US"
    assert prompt["isEdited"] is False
    assert prompt["value"] == [["create a graphic"]]
