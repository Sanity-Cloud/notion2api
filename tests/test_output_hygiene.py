from app.output_hygiene import (
    build_hygiene_metadata,
    clean_visible_output,
    detect_visible_output_contamination,
    finalize_visible_output,
    is_hidden_content_type,
    prepare_visible_stream_chunk,
    repair_missing_inter_word_spaces,
    strip_model_name_splices,
    strip_thinking_blocks,
    strip_thinking_blocks_from_chunk,
)


def test_strip_thinking_blocks_removes_complete_and_unclosed_markup():
    assert (
        strip_thinking_blocks("<think>hidden</think>\n\nVisible answer")
        == "Visible answer"
    )
    assert strip_thinking_blocks("<think>hidden only") == ""


def test_strip_thinking_blocks_from_chunk_preserves_whitespace_only_segments():
    assert strip_thinking_blocks_from_chunk(" ") == " "
    assert strip_thinking_blocks_from_chunk("\n") == "\n"
    assert (
        strip_thinking_blocks_from_chunk("Hello")
        + strip_thinking_blocks_from_chunk(" ")
        + strip_thinking_blocks_from_chunk("world")
        == "Hello world"
    )


def test_prepare_visible_stream_chunk_does_not_split_intra_word_chunks():
    assert prepare_visible_stream_chunk("str", "ategic") == "ategic"
    assert prepare_visible_stream_chunk("pa", "nel") == "nel"
    assert prepare_visible_stream_chunk("Hello", "world") == "world"
    assert prepare_visible_stream_chunk("Assessment of the", "proposed") == " proposed"


def test_repair_missing_inter_word_spaces_fixes_glued_common_words():
    assert repair_missing_inter_word_spaces("Assessment of theproposed edits") == (
        "Assessment of the proposed edits"
    )
    assert repair_missing_inter_word_spaces("yourletterand envelope") == (
        "your letter and envelope"
    )


def test_strip_model_name_splices_removes_known_display_name_fragments():
    assert strip_model_name_splices("Sonnet 5owever the issue remains.") == (
        "owever the issue remains."
    )
    assert strip_model_name_splices("## ****Opus 4.7LM 5.2hairman's Synthesis") == (
        "##  Synthesis"
    )


def test_clean_visible_output_is_idempotent():
    dirty = "<l### Overall assessment\n\nBody"
    once = clean_visible_output(dirty)
    twice = clean_visible_output(once)
    assert once == twice
    assert once.startswith("### Overall assessment")


def test_legitimate_openers_are_not_flagged_or_mutated():
    samples = [
        "User wants a refund policy summary. Here it is: full refund within 30 days.",
        "Let me walk you through the steps to complete the form.",
        "I need to reset my password, can you help?",
    ]
    for raw in samples:
        assert detect_visible_output_contamination(raw) is False
        assert clean_visible_output(raw) == raw


def test_true_reasoning_leak_still_trims_to_answer_boundary():
    raw = (
        "user is trying to apply a template and I need to verify statutes."
        "### Threshold framing\n\nThis is the answer."
    )
    assert detect_visible_output_contamination(raw) is True
    assert clean_visible_output(raw) == "### Threshold framing\n\nThis is the answer."


def test_finalize_visible_output_returns_metadata():
    raw = "<think>hidden</think>\n\nVisible answer."
    cleaned, hygiene = finalize_visible_output(raw)
    assert cleaned == "Visible answer."
    assert hygiene["hidden_thinking_removed"] is True
    assert hygiene["visible_contamination_detected"] is False
    assert hygiene["retry_recommended"] is False


def test_build_hygiene_metadata_marks_contamination_for_retry():
    raw = "Sonnet 5owever the issue remains."
    cleaned = clean_visible_output(raw)
    hygiene = build_hygiene_metadata(raw, cleaned)
    assert hygiene["visible_contamination_detected"] is True
    assert hygiene["retry_recommended"] is True
    assert "owever" in cleaned


def test_is_hidden_content_type_matches_transport_reasoning_types():
    assert is_hidden_content_type("thinking") is True
    assert is_hidden_content_type("redacted-thinking") is True
    assert is_hidden_content_type("text") is False


def test_clean_visible_output_strips_trailing_orphan_markdown():
    assert clean_visible_output("Useful answer about Mission World.##") == (
        "Useful answer about Mission World."
    )
    assert detect_visible_output_contamination(
        "Sanity Cloud AI Portal existing product architecture and governance concepts "
        "including departments, Oz roles, authority A0-A4, QuickBind, workflow school "
        "levels, and autonomy maturitySanity Cloud AI Portal governance QuickBind "
        "authority A0 A4 Oz Hollywood White House Government Militaryall_time##"
    ) is True
