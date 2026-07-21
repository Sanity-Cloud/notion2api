"""Safe stream parser wrapper.

This module keeps Notion's parser output compatible with the local UI while
preventing full assistant answers from being displayed in the Thinking panel.
"""

from __future__ import annotations

from typing import Any, Generator

import requests

from app.stream_parser import parse_stream as _parse_stream


def _is_citation_only(text: str) -> bool:
    cleaned = text.strip()
    if not cleaned:
        return False
    import re
    # Remove citation markers like [1], [2], [1,2], [abc]
    no_citations = re.sub(r'\[\s*\d+(?:\s*,\s*\d+)*\s*\]', '', cleaned).strip()
    no_citations = re.sub(r'\[\s*[a-zA-Z0-9_-]+\s*\]', '', no_citations).strip()
    if not no_citations:
        return True
    
    # Check if remaining text is just urls, sources, references, webpage labels
    norm = no_citations.lower()
    norm = re.sub(r'https?://\S+', '', norm).strip()
    norm = re.sub(r'www\.\S+', '', norm).strip()
    norm = re.sub(r'\b(sources?|references?|links?|citations?|url|urls|webpage|webpages)\b', '', norm).strip()
    norm = re.sub(r'[^\w\s]', '', norm).strip()
    return len(norm) == 0


def parse_stream(response: requests.Response) -> Generator[dict[str, Any], None, None]:
    """Yield content/search/final events while suppressing streamed thinking.

    Some Notion responses put the answer body in an ``agent-inference`` segment.
    The lower-level parser historically maps that segment to ``thinking`` so the
    frontend renders a complete answer under a visible Thinking card. This wrapper
    buffers thinking chunks and only falls them back to normal content if no real
    content/final event arrives.
    """
    buffered_thinking: list[str] = []
    yielded_content_parts: list[str] = []

    for item in _parse_stream(response):
        if not isinstance(item, dict):
            yield item
            continue

        item_type = str(item.get("type", "") or "").lower()
        if item_type == "thinking":
            text = str(item.get("text", "") or "")
            if text:
                buffered_thinking.append(text)
            continue

        if item_type in {"content", "final_content"}:
            text = str(item.get("text", "") or "")
            if text.strip():
                yielded_content_parts.append(text)

        yield item

    if buffered_thinking:
        thinking_text = "".join(buffered_thinking).strip()
        if thinking_text:
            yielded_text = "".join(yielded_content_parts).strip()
            # Promote if no visible content was yielded, OR if the yielded content is citation-only
            if not yielded_text or _is_citation_only(yielded_text):
                # Normalize whitespace and lowercase to compare content
                norm_thinking = " ".join(thinking_text.split()).lower()
                norm_yielded = " ".join(yielded_text.split()).lower()
                
                is_duplicate = False
                if norm_thinking in norm_yielded:
                    is_duplicate = True
                else:
                    from difflib import SequenceMatcher
                    ratio = SequenceMatcher(None, norm_thinking, norm_yielded).ratio()
                    if ratio > 0.75:
                        is_duplicate = True
                
                if not is_duplicate:
                    if not yielded_text:
                        yield {"type": "content", "text": thinking_text}
                    else:
                        yield {"type": "content", "text": "\n\n" + thinking_text}
