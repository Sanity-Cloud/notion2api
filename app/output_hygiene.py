"""Transport-neutral visible-output hygiene helpers.

Some upstream transports can leak private reasoning markup or token-corruption
artifacts into assistant-visible text. These helpers clean and diagnose that
output without applying council/audit retry policy.
"""
from __future__ import annotations

import re
from collections import Counter
from typing import Any, Dict

from .output_integrity import assess_output_integrity

THINK_BLOCK_RE = re.compile(r"<think\b[^>]*>[\s\S]*?</think>", re.IGNORECASE)
UNCLOSED_THINK_RE = re.compile(r"<think\b[^>]*>[\s\S]*$", re.IGNORECASE)
LEADING_PARTIAL_TAG_RE = re.compile(r"^\s*<[A-Za-z]{1,24}(?=#{1,6}\s)")
VISIBLE_REASONING_PREAMBLE_RE = re.compile(
    r"(?is)^\s*(?:"
    r"user\s+(?:is\s+trying|is\s+asking|message)\b|"
    r"the\s+user-message\s+is\b|"
    r"i(?:'m| am)\s+(?:looking|re-evaluating|trying|going|checking|reading)\b|"
    r"i\s+need\s+to\s+(?:acknowledge|verify|check|analyze|evaluate|assess|determine|parse|re-?evaluate|look|inspect|review)\b|"
    r"i(?:'ll| will)\s+(?:verify|check|search|look|inspect|review)\b|"
    r"let\s+me\s+(?:think|check|re-?evaluate|verify|analyze|see|read|parse|assess|inspect|review|search)\b|"
    r"generating\s+evaluation\b"
    r")"
)
VISIBLE_REASONING_TRIM_TARGET_RE = re.compile(
    r"(?is)(```(?:json)?\s*\n|#{1,6}\s+(?:review|overall|threshold|assessment|analysis|strengths|evaluation|final|summary)\b|\{\s*\"responses\")"
)
CORRUPT_CITATION_OR_HEADING_RE = re.compile(
    r"(?is)(\[\^\{\{[^\]\n]*(?:notion-#{1,6}|#{1,6}\s)|notion-#{1,6}|\[\^\{\{notion-)"
)
MODEL_NAME_SPLICE_RE = re.compile(
    r"(?i)(?:\*{2,})?(?:"
    r"grok(?:\s+build\s+0\.1|\s+4\.3)?|"
    r"(?:glm|lm)\s+5\.2|"
    r"gpt-?5\.5|"
    r"sonnet\s+5|"
    r"opus\s+4\.[78]|"
    r"deepseek\s+v4\s+pro|"
    r"gemini\s+3\.1\s+pro|"
    r"strawberry\s+whoopiepie|"
    r"fable\s*5|"
    r"angel[\s-]?cake(?:[\s-]?high)?|"
    r"xinomavro[\s-]?cake|"
    r"opal[\s-]?quince"
    r")(?=[A-Za-z0-9])"
)

MODEL_NAME_TOKEN_RE = re.compile(
    r"(?i)(?:\*{2,})?(?:"
    r"grok(?:\s+build\s+0\.1|\s+4\.3)?|"
    r"(?:glm|lm)\s+5\.2|"
    r"gpt-?5\.5|"
    r"sonnet\s+5|"
    r"opus\s+4\.[78]|"
    r"deepseek\s+v4\s+pro|"
    r"gemini\s+3\.1\s+pro|"
    r"strawberry\s+whoopiepie|"
    r"fable\s*5|"
    r"angel[\s-]?cake(?:[\s-]?high)?|"
    r"xinomavro[\s-]?cake|"
    r"opal[\s-]?quince"
    r")"
)
_COMMON_GLUE_WORDS = (
    "attorney",
    "chairman",
    "clarification",
    "county",
    "december",
    "letter",
    "proposed",
    "recording",
    "response",
    "statutory",
    "synthesis",
    "that",
    "the",
    "this",
    "transactional",
    "whether",
    "your",
)

_COMMON_PREFIX_WORDS = (
    "a",
    "an",
    "and",
    "for",
    "in",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
    "your",
)

_GLUED_PREFIX_PAIR_RE = re.compile(
    r"(?i)\b("
    + "|".join(re.escape(word) for word in sorted(_COMMON_PREFIX_WORDS, key=len, reverse=True))
    + r")("
    + "|".join(re.escape(word) for word in sorted(_COMMON_GLUE_WORDS, key=len, reverse=True))
    + r")\b"
)
_COMMON_GLUE_WORD_RE = re.compile(
    r"(?<![A-Za-z])(?:"
    + "|".join(re.escape(word) for word in sorted(_COMMON_GLUE_WORDS, key=len, reverse=True))
    + r")(?=[a-z])",
    re.IGNORECASE,
)
TEXT_CORRUPTION_ARTIFACT_RE = re.compile(
    r"(?i)(^\s*<[A-Za-z]{1,24}(?=#{1,6}\s)|"
    r"\bMempt\s+facts\b|\brelateected\b|\brespon\.\d|\btope\s+[?-]|"
    r"\bqueming\b|\bsated\s+basis\b|\bex\s+available\s+sources\b|"
    r"recordingearns\b|AxonmLet\b)"
)
TRAILING_ORPHAN_MARKDOWN_RE = re.compile(r"(?s)[\s#]*#+\s*$")
HIDDEN_CONTENT_TYPES = frozenset({
    "thinking",
    "reasoning",
    "reasoning_content",
    "redacted_thinking",
    "chain_of_thought",
})


def is_hidden_content_type(value: Any) -> bool:
    """Return True when a structured content item should stay hidden."""

    normalized = str(value or "").strip().lower().replace("-", "_")
    if not normalized:
        return False
    if normalized in HIDDEN_CONTENT_TYPES:
        return True
    return normalized.endswith("_thinking")


def strip_thinking_blocks(text: Any) -> str:
    """Remove hidden-reasoning markup from model-visible text."""

    cleaned = str(text or "").strip()
    cleaned = THINK_BLOCK_RE.sub("", cleaned)
    cleaned = UNCLOSED_THINK_RE.sub("", cleaned)
    return cleaned.strip()


def strip_thinking_blocks_from_chunk(text: Any) -> str:
    """Remove hidden-reasoning markup from one streamed chunk.

    Unlike ``strip_thinking_blocks``, this preserves whitespace-only chunks.
    Notion often streams inter-word spaces as their own chunks; trimming them
    would concatenate words in the assembled assistant reply.
    """

    cleaned = str(text or "")
    if not cleaned:
        return ""
    cleaned = THINK_BLOCK_RE.sub("", cleaned)
    cleaned = UNCLOSED_THINK_RE.sub("", cleaned)
    return cleaned


def needs_visible_stream_boundary_space(previous: str, chunk: str) -> bool:
    """Return True when two streamed chunks need an inferred word boundary."""

    if not previous or not chunk:
        return False
    if chunk[0].isspace() or previous[-1].isspace():
        return False
    previous_tail = previous.rstrip()
    if not previous_tail:
        return False
    if not chunk or not chunk[0].isalpha():
        return False
    # Notion sometimes streams the space between a stopword and the next token as
    # its own chunk, but occasionally omits it. Only infer a boundary after
    # common stopwords to avoid splitting real intra-word chunks like "str"+"ategic".
    return bool(re.search(r"(?i)\b(?:a|an|and|as|at|but|by|for|from|in|of|on|or|the|to|with)$", previous_tail))


def prepare_visible_stream_chunk(previous: str, raw_chunk: Any) -> str:
    """Normalize one streamed chunk and infer narrow missing word boundaries."""

    raw = str(raw_chunk or "")
    if not raw:
        return ""
    lowered = raw.lower()
    if "redacted_thinking" in lowered or "<think" in lowered:
        chunk = strip_thinking_blocks(raw)
    else:
        chunk = strip_thinking_blocks_from_chunk(raw)
    if not chunk:
        return ""
    if needs_visible_stream_boundary_space(previous, chunk):
        return " " + chunk
    return chunk


def strip_model_name_splices(text: Any) -> str:
    """Remove known model-display-name fragments spliced into visible prose."""

    cleaned = str(text or "")
    if not cleaned:
        return ""
    cleaned = re.sub(r"(?i)(?:chairman'?s|hairman'?s)", "", cleaned)
    previous = None
    while cleaned != previous:
        previous = cleaned
        cleaned = MODEL_NAME_SPLICE_RE.sub("", cleaned)
        cleaned = MODEL_NAME_TOKEN_RE.sub("", cleaned)
    return cleaned


def repair_missing_inter_word_spaces(text: Any) -> str:
    """Repair a narrow class of glued English words left by dropped space chunks."""

    cleaned = str(text or "")
    if not cleaned:
        return ""
    if detect_visible_output_contamination(cleaned):
        return cleaned
    previous = None
    while cleaned != previous:
        previous = cleaned
        cleaned = _GLUED_PREFIX_PAIR_RE.sub(lambda match: f"{match.group(1)} {match.group(2)}", cleaned)
        cleaned = _COMMON_GLUE_WORD_RE.sub(lambda match: f"{match.group(0)} ", cleaned)
    return cleaned


def _has_repeated_markdown_heading(text: str) -> bool:
    headings = [
        re.sub(r"\s+", " ", match.group(1)).strip().casefold()
        for match in re.finditer(r"(?m)^\s*#{1,6}\s+([^\n]{8,160})\s*$", text)
    ]
    if not headings:
        return False
    return any(count >= 3 for count in Counter(headings).values())


def detect_visible_output_contamination(text: Any) -> bool:
    """Detect visible reasoning leaks, token corruption, or integrity amplification."""

    cleaned = strip_thinking_blocks(text)
    if not cleaned:
        return False
    legacy_contamination = bool(
        VISIBLE_REASONING_PREAMBLE_RE.search(cleaned)
        or TEXT_CORRUPTION_ARTIFACT_RE.search(cleaned)
        or CORRUPT_CITATION_OR_HEADING_RE.search(cleaned)
        or MODEL_NAME_SPLICE_RE.search(cleaned)
        or _has_repeated_markdown_heading(cleaned)
    )
    return legacy_contamination or bool(assess_output_integrity(cleaned)["contaminated"])


def strip_trailing_orphan_markdown(text: Any) -> str:
    """Remove dangling trailing heading markers left by truncated streams."""

    cleaned = str(text or "")
    if not cleaned:
        return ""
    return TRAILING_ORPHAN_MARKDOWN_RE.sub("", cleaned).rstrip()


def clean_visible_output(text: Any) -> str:
    """Clean visible output without changing substantive answer content."""

    cleaned = strip_thinking_blocks(text)
    cleaned = strip_model_name_splices(cleaned)
    cleaned = repair_missing_inter_word_spaces(cleaned)
    cleaned = LEADING_PARTIAL_TAG_RE.sub("", cleaned).strip()
    if VISIBLE_REASONING_PREAMBLE_RE.search(cleaned):
        match = VISIBLE_REASONING_TRIM_TARGET_RE.search(cleaned)
        if match and match.start() > 0:
            cleaned = cleaned[match.start():].strip()
            cleaned = LEADING_PARTIAL_TAG_RE.sub("", cleaned).strip()
    cleaned = strip_model_name_splices(cleaned)
    cleaned = repair_missing_inter_word_spaces(cleaned)
    cleaned = strip_trailing_orphan_markdown(cleaned)
    return cleaned.strip()


def build_hygiene_metadata(raw_text: Any, cleaned_text: Any) -> Dict[str, bool]:
    """Build transport metadata describing hygiene actions and follow-up hints."""

    raw = str(raw_text or "")
    cleaned = str(cleaned_text or "")
    stripped = strip_thinking_blocks(raw)
    hidden_thinking_removed = bool(raw.strip()) and (
        stripped != raw.strip() or cleaned != raw.strip()
    )
    contamination = (
        detect_visible_output_contamination(raw)
        or detect_visible_output_contamination(cleaned)
    )
    return {
        "hidden_thinking_removed": hidden_thinking_removed,
        "visible_contamination_detected": contamination,
        "retry_recommended": contamination,
    }


def finalize_visible_output(text: Any) -> tuple[str, Dict[str, bool]]:
    """Return cleaned visible text plus hygiene metadata for transport layers."""

    raw = str(text or "")
    cleaned = clean_visible_output(raw)
    return cleaned, build_hygiene_metadata(raw, cleaned)
