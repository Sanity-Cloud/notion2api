from __future__ import annotations

import re


def normalize_model_lookup_key(model_name: str | None) -> str:
    """One normalization rule for names, aliases, and collision checks."""
    normalized = normalize_model_id(model_name)
    candidate = str(normalized or "").strip().lower()
    candidate = re.sub(r"[\s_]+", "-", candidate)
    return re.sub(r"-+", "-", candidate).strip("-")


def normalize_model_id(model: str | None) -> str | None:
    if not model:
        return model

    if model.startswith("custom:"):
        return model.removeprefix("custom:")

    return model
