from app.output_integrity import assess_output_integrity
from app.schemas import ChatCompletionResponse, ChatMessage, ChatMessageResponseChoice


def test_clean_output_is_validated():
    receipt = assess_output_integrity(
        "A concise, distinct response with no repeated material."
    )

    assert receipt["status"] == "validated"
    assert receipt["contaminated"] is False
    assert receipt["quarantine_required"] is False


def test_response_over_size_limit_is_quarantined():
    receipt = assess_output_integrity("x" * 100_001)

    assert receipt["status"] == "indeterminate_output"
    assert "response_size_limit_exceeded" in receipt["reasons"]
    assert receipt["response_chars"] == 100_001


def test_four_identical_substantive_paragraphs_are_quarantined():
    paragraph = (
        "This substantive paragraph repeats the same unsupported completion claim."
    )
    receipt = assess_output_integrity("\n\n".join([paragraph] * 4))

    assert "identical_paragraph_repetition" in receipt["reasons"]
    assert receipt["max_identical_paragraph_occurrences"] == 4


def test_duplicate_paragraph_ratio_is_quarantined_below_repeat_limit():
    repeated = "This paragraph is intentionally duplicated for ratio testing only."
    text = "\n\n".join(
        [
            repeated,
            repeated,
            "This second substantive paragraph is unique and independently meaningful.",
            "This third substantive paragraph is also unique and independently meaningful.",
            "This fourth substantive paragraph remains unique and independently meaningful.",
        ]
    )
    receipt = assess_output_integrity(text)

    assert "duplicate_paragraph_ratio_exceeded" in receipt["reasons"]
    assert receipt["max_identical_paragraph_occurrences"] == 2


def test_repeated_markdown_heading_is_quarantined():
    text = "\n\n".join(
        ["## Fabricated completion\nUnique body " + str(i) for i in range(4)]
    )
    receipt = assess_output_integrity(text)

    assert "repeated_markdown_heading" in receipt["reasons"]
    assert receipt["max_repeated_heading_occurrences"] == 4


def test_malformed_notion_citation_is_quarantined():
    receipt = assess_output_integrity(
        "Evidence follows {{notion-page-id without a closing marker"
    )

    assert "malformed_notion_citation" in receipt["reasons"]


def test_geometric_event_growth_is_quarantined():
    receipt = assess_output_integrity("final", event_lengths=[10, 16, 25, 38])

    assert "geometric_event_growth" in receipt["reasons"]
    assert receipt["geometric_event_growth_detected"] is True


def test_observed_recursive_incident_pattern_is_quarantined():
    report = (
        "The operation completed and created all records, but this paragraph is "
        "recursively repeated without independent evidence or distinct content."
    )
    receipt = assess_output_integrity("\n\n".join([report] * 5))

    assert receipt["quarantine_required"] is True
    assert "identical_paragraph_repetition" in receipt["reasons"]


def test_chat_response_schema_preserves_nested_integrity_receipt():
    response = ChatCompletionResponse(
        id="integrity-response",
        model="terra",
        choices=[
            ChatMessageResponseChoice(
                message=ChatMessage(role="assistant", content="clean")
            )
        ],
        hygiene={
            "hidden_thinking_removed": False,
            "output_integrity": {
                "status": "validated",
                "quarantine_required": False,
            },
        },
    )

    dumped = response.model_dump()
    assert dumped["hygiene"]["output_integrity"]["status"] == "validated"


def test_nonsentence_keyword_dump_is_quarantined():
    text = (
        "Sanity Cloud AI Portal existing product architecture and governance concepts "
        "including departments, Oz roles, authority A0-A4, QuickBind, workflow school "
        "levels, and autonomy maturitySanity Cloud AI Portal governance QuickBind "
        "authority A0 A4 Oz Hollywood White House Government Militaryall_time##"
    )
    receipt = assess_output_integrity(text)

    assert receipt["quarantine_required"] is True
    assert "nonsentence_keyword_dump" in receipt["reasons"]
    assert receipt["nonsentence_keyword_dump_detected"] is True


def test_normal_prose_with_product_names_is_not_keyword_dump():
    text = (
        "Use QuickBind to compose a Mission World packet. Keep authority ceilings "
        "independent from themed labels, and require a human gate for mission-critical work."
    )
    receipt = assess_output_integrity(text)

    assert receipt["quarantine_required"] is False
    assert "nonsentence_keyword_dump" not in receipt["reasons"]
