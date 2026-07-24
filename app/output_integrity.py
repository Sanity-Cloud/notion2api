"""Fail-closed integrity assessment for assistant-visible output.

This module classifies output before it is promoted into normal conversation
state. It does not delete or rewrite raw evidence.
"""

from __future__ import annotations

import hashlib
import re
from collections import Counter
from typing import Any, Iterable

MAX_VISIBLE_RESPONSE_CHARS = 100_000
MAX_IDENTICAL_PARAGRAPH_OCCURRENCES = 3
MAX_REPEATED_HEADING_OCCURRENCES = 3
MAX_DUPLICATE_PARAGRAPH_RATIO = 0.15
MIN_SUBSTANTIVE_PARAGRAPH_CHARS = 40
GEOMETRIC_GROWTH_FACTOR = 1.5
GEOMETRIC_GROWTH_EXPANSIONS = 3

_MALFORMED_NOTION_CITATION_RE = re.compile(
    r"(?is)(?:\[\^\{\{|\{\{notion-|notion-#{1,6}|\[\^[^\]\n]{0,200}$)"
)
_HEADING_RE = re.compile(r"(?m)^\s*#{1,6}\s+([^\n]{1,240})\s*$")


def _normalize_text(value: Any) -> str:
    return str(value or "").replace("\r\n", "\n").replace("\r", "\n")


def _normalized_headings(text: str) -> list[str]:
    return [
        re.sub(r"\s+", " ", match.group(1)).strip().casefold()
        for match in _HEADING_RE.finditer(text)
        if match.group(1).strip()
    ]


def _substantive_paragraphs(text: str) -> list[str]:
    paragraphs: list[str] = []
    for raw in re.split(r"\n\s*\n+", text):
        normalized = re.sub(r"\s+", " ", raw).strip()
        if len(normalized) < MIN_SUBSTANTIVE_PARAGRAPH_CHARS:
            continue
        if normalized.startswith("#"):
            continue
        paragraphs.append(normalized.casefold())
    return paragraphs


def _geometric_growth_detected(event_lengths: Iterable[int] | None) -> bool:
    if event_lengths is None:
        return False
    lengths = [max(0, int(value)) for value in event_lengths]
    if len(lengths) < GEOMETRIC_GROWTH_EXPANSIONS + 1:
        return False
    consecutive = 0
    for previous, current in zip(lengths, lengths[1:]):
        if previous > 0 and current >= previous * GEOMETRIC_GROWTH_FACTOR:
            consecutive += 1
            if consecutive >= GEOMETRIC_GROWTH_EXPANSIONS:
                return True
        else:
            consecutive = 0
    return False


def assess_output_integrity(
    text: Any,
    *,
    event_lengths: Iterable[int] | None = None,
    additional_reasons: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Return a deterministic quarantine decision and bounded evidence receipt."""

    normalized = _normalize_text(text)
    response_chars = len(normalized)
    reasons = list(
        dict.fromkeys(str(reason) for reason in (additional_reasons or ()) if reason)
    )

    headings = _normalized_headings(normalized)
    heading_counts = Counter(headings)
    max_heading_occurrences = max(heading_counts.values(), default=0)

    paragraphs = _substantive_paragraphs(normalized)
    paragraph_counts = Counter(paragraphs)
    max_paragraph_occurrences = max(paragraph_counts.values(), default=0)
    duplicate_instances = sum(max(0, count - 1) for count in paragraph_counts.values())
    duplicate_ratio = duplicate_instances / len(paragraphs) if paragraphs else 0.0

    malformed_citations = bool(_MALFORMED_NOTION_CITATION_RE.search(normalized))
    geometric_growth = _geometric_growth_detected(event_lengths)

    if response_chars > MAX_VISIBLE_RESPONSE_CHARS:
        reasons.append("response_size_limit_exceeded")
    if max_paragraph_occurrences > MAX_IDENTICAL_PARAGRAPH_OCCURRENCES:
        reasons.append("identical_paragraph_repetition")
    if len(paragraphs) >= 4 and duplicate_ratio > MAX_DUPLICATE_PARAGRAPH_RATIO:
        reasons.append("duplicate_paragraph_ratio_exceeded")
    if max_heading_occurrences > MAX_REPEATED_HEADING_OCCURRENCES:
        reasons.append("repeated_markdown_heading")
    if malformed_citations:
        reasons.append("malformed_notion_citation")
    if geometric_growth:
        reasons.append("geometric_event_growth")

    reasons = list(dict.fromkeys(reasons))
    contaminated = bool(reasons)
    return {
        "schema_version": 1,
        "status": "indeterminate_output" if contaminated else "validated",
        "contaminated": contaminated,
        "quarantine_required": contaminated,
        "response_chars": response_chars,
        "response_sha256": hashlib.sha256(normalized.encode("utf-8")).hexdigest(),
        "reasons": reasons,
        "substantive_paragraph_count": len(paragraphs),
        "duplicate_paragraph_ratio": round(duplicate_ratio, 4),
        "max_identical_paragraph_occurrences": max_paragraph_occurrences,
        "max_repeated_heading_occurrences": max_heading_occurrences,
        "malformed_notion_citation_detected": malformed_citations,
        "geometric_event_growth_detected": geometric_growth,
    }


def is_output_contaminated(
    text: Any, *, event_lengths: Iterable[int] | None = None
) -> bool:
    return bool(
        assess_output_integrity(text, event_lengths=event_lengths)["contaminated"]
    )
