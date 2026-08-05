from app.core.models import normalize_model_id
import os
import re
import threading
import time
import uuid

from app.logger import logger
from app.model_restriction_cache import ModelRestrictionCache



MODEL_MAP: dict[str, str] = {
    # Anthropic
    "claude-sonnet4.6": "almond-croissant-low",
    "claude-sonnet-4.6": "almond-croissant-low",
    "sonnet-4.6": "almond-croissant-low",
    "sonnet4.6": "almond-croissant-low",
    "claude-sonnet5": "angel-cake-high",
    "claude-sonnet-5": "angel-cake-high",
    "sonnet-5": "angel-cake-high",
    "sonnet5": "angel-cake-high",
    "claude-opus4.6": "avocado-froyo-medium",
    "claude-opus-4.6": "avocado-froyo-medium",
    "opus-4.6": "avocado-froyo-medium",
    "opus4.6": "avocado-froyo-medium",
    "claude-opus4.7": "apricot-sorbet-high",
    "claude-opus-4.7": "apricot-sorbet-high",
    "opus-4.7": "apricot-sorbet-high",
    "opus4.7": "apricot-sorbet-high",
    "claude-opus4.8": "ambrosia-tart-high",
    "claude-opus-4.8": "ambrosia-tart-high",
    "opus-4.8": "ambrosia-tart-high",
    "opus4.8": "ambrosia-tart-high",
    "claude-haiku4.5": "anthropic-haiku-4.5",
    "haiku-4.5": "anthropic-haiku-4.5",
    "haiku4.5": "anthropic-haiku-4.5",
    "claude-fable5": "acai-budino",
    "claude-fable-5": "acai-budino",
    "fable-5": "acai-budino",
    "fable5": "acai-budino",
    "claude-fable5-high": "acai-budino-high",
    "claude-fable-5-high": "acai-budino-high",
    "fable5-high": "acai-budino-high",

    # OpenAI
    "gpt-5.6-sol": "orange-mousse",
    "gpt-5.6sol": "orange-mousse",
    "sol": "orange-mousse",
    "gpt-5.6-terra": "orchid-muffin",
    "gpt-5.6terra": "orchid-muffin",
    "terra": "orchid-muffin",
    "gpt-5.6-luna": "olive-jellyroll",
    "gpt-5.6luna": "olive-jellyroll",
    "luna": "olive-jellyroll",
    "gpt-5.2": "oatmeal-cookie",
    "gpt-5.4": "oval-kumquat-medium",
    "gpt-5.5": "opal-quince-medium",
    "gpt-5.4mini": "oregon-grape-medium",
    "gpt-5.4nano": "otaheite-apple-medium",

    # Google
    "gemini-3-flash": "gingerbread",
    "gemini-3flash": "gingerbread",
    "gemini-3.1-pro": "galette-medium-thinking",
    "gemini-3.1pro": "galette-medium-thinking",
    "gemini-3.5flash": "vertex-gemini-3.5-flash",
    "gemini-3.5-flash": "vertex-gemini-3.5-flash",
    "gemini-2.5flash": "vertex-gemini-2.5-flash",
    "gemini-2.5-flash": "vertex-gemini-2.5-flash",

    # xAI
    "grok-4.3": "xigua-mochi-medium",
    "grok-4.5": "strawberry-whoopiepie",
    "spacexai-4.5": "strawberry-whoopiepie",
    "spacex-ai-4.5": "strawberry-whoopiepie",
    "grok-build0.1": "xinomavro-cake",

    # Other
    "minimax-m2.5": "fireworks-minimax-m2.5",
    "kimi-2.6": "fireworks-kimi-k2.6",
    "kimi-k2.6": "fireworks-kimi-k2.6",
    "kimi-2.7": "fireworks-kimi-k2.7",
    "kimi-2.7-code": "fireworks-kimi-k2.7",
    "kimi-k2.7": "fireworks-kimi-k2.7",
    "kimi-k2.7-code": "fireworks-kimi-k2.7",
    "deepseek-v4pro": "baseten-deepseek-v4-pro",
    "glm-5.2": "baseten-glm-5.2",

    # Additional compatibility aliases requested
    "claude-haiku-4.5": "anthropic-haiku-4.5",
    "gpt-5.4-mini": "oregon-grape-medium",
    "gpt-5.4-nano": "otaheite-apple-medium",
    "deepseek-v4-pro": "baseten-deepseek-v4-pro",
    "grok-build-0.1": "xinomavro-cake",

    # Backend Model IDs mapping to themselves
    "orange-mousse": "orange-mousse",
    "orchid-muffin": "orchid-muffin",
    "olive-jellyroll": "olive-jellyroll",
    "oatmeal-cookie": "oatmeal-cookie",
    "oval-kumquat-medium": "oval-kumquat-medium",
    "opal-quince-medium": "opal-quince-medium",
    "vertex-gemini-2.5-flash": "vertex-gemini-2.5-flash",
    "vertex-gemini-3.5-flash": "vertex-gemini-3.5-flash",
    "almond-croissant-low": "almond-croissant-low",
    "angel-cake-high": "angel-cake-high",
    "avocado-froyo-medium": "avocado-froyo-medium",
    "apricot-sorbet-high": "apricot-sorbet-high",
    "ambrosia-tart-high": "ambrosia-tart-high",
    "oregon-grape-medium": "oregon-grape-medium",
    "otaheite-apple-medium": "otaheite-apple-medium",
    "fireworks-minimax-m2.5": "fireworks-minimax-m2.5",
    "fireworks-kimi-k2.6": "fireworks-kimi-k2.6",
    "fireworks-kimi-k2.7": "fireworks-kimi-k2.7",
    "baseten-deepseek-v4-pro": "baseten-deepseek-v4-pro",
    "baseten-glm-5.2": "baseten-glm-5.2",
    "xigua-mochi-medium": "xigua-mochi-medium",
    "strawberry-whoopiepie": "strawberry-whoopiepie",
    "xinomavro-cake": "xinomavro-cake",
    "galette-medium-thinking": "galette-medium-thinking",
    "anthropic-haiku-4.5": "anthropic-haiku-4.5",
    "gingerbread": "gingerbread",
    "acai-budino": "acai-budino",
    "acai-budino-high": "acai-budino-high",
}

NOTION_MODEL_REVERSE_MAP: dict[str, str] = {
    # Anthropic
    "almond-croissant-low": "claude-sonnet4.6",
    "angel-cake-high": "claude-sonnet5",
    "avocado-froyo-medium": "claude-opus4.6",
    "apricot-sorbet-high": "claude-opus4.7",
    "ambrosia-tart-high": "claude-opus4.8",
    "anthropic-haiku-4.5": "claude-haiku4.5",
    "acai-budino": "claude-fable5",
    "acai-budino-high": "claude-fable5-high",

    # OpenAI
    "orange-mousse": "gpt-5.6-sol",
    "orchid-muffin": "gpt-5.6-terra",
    "olive-jellyroll": "gpt-5.6-luna",
    "oatmeal-cookie": "gpt-5.2",
    "oval-kumquat-medium": "gpt-5.4",
    "opal-quince-medium": "gpt-5.5",
    "oregon-grape-medium": "gpt-5.4mini",
    "otaheite-apple-medium": "gpt-5.4nano",

    # Google
    "gingerbread": "gemini-3flash",
    "galette-medium-thinking": "gemini-3.1pro",
    "vertex-gemini-3.5-flash": "gemini-3.5flash",
    "vertex-gemini-2.5-flash": "gemini-2.5flash",

    # xAI
    "xigua-mochi-medium": "grok-4.3",
    "strawberry-whoopiepie": "spacexai-4.5",
    "xinomavro-cake": "grok-build0.1",

    # Other
    "fireworks-minimax-m2.5": "minimax-m2.5",
    "fireworks-kimi-k2.6": "kimi-2.6",
    "fireworks-kimi-k2.7": "kimi-2.7-code",
    "baseten-deepseek-v4-pro": "deepseek-v4pro",
    "baseten-glm-5.2": "glm-5.2",
}

DISPLAY_NAMES: dict[str, str] = {
    # Aliases
    "claude-sonnet4.6": "Claude Sonnet 4.6",
    "claude-sonnet5": "Claude Sonnet 5",
    "claude-opus4.6": "Claude Opus 4.6",
    "claude-opus4.7": "Claude Opus 4.7",
    "claude-opus4.8": "Claude Opus 4.8",
    "claude-haiku4.5": "Claude Haiku 4.5",
    "claude-fable5": "Fable 5",
    "claude-fable5-high": "Fable 5",
    "claude-fable-5-high": "Fable 5",
    "fable5-high": "Fable 5",
    "gpt-5.6-sol": "GPT-5.6 Sol",
    "gpt-5.6-terra": "GPT-5.6 Terra",
    "gpt-5.6-luna": "GPT-5.6 Luna",
    "gpt-5.2": "GPT-5.2",
    "gpt-5.4": "GPT-5.4",
    "gpt-5.4mini": "GPT-5.4 Mini",
    "gpt-5.4nano": "GPT-5.4 Nano",
    "gpt-5.5": "GPT-5.5",
    "gemini-3flash": "Gemini 3 Flash",
    "gemini-3-flash": "Gemini 3 Flash",
    "gemini-3.1pro": "Gemini 3.1 Pro",
    "gemini-3.1-pro": "Gemini 3.1 Pro",
    "gemini-3.5flash": "Gemini 3.5 Flash",
    "gemini-2.5flash": "Gemini 2.5 Flash",
    "grok-4.3": "Grok 4.3",
    "grok-4.5": "SpaceXAI 4.5",
    "spacexai-4.5": "SpaceXAI 4.5",
    "spacex-ai-4.5": "SpaceXAI 4.5",
    "grok-build0.1": "Grok Build 0.1",
    "minimax-m2.5": "MiniMax M2.5",
    "kimi-2.6": "Kimi 2.6",
    "kimi-2.7": "Kimi K2.7 Code",
    "kimi-2.7-code": "Kimi K2.7 Code",
    "kimi-k2.7": "Kimi K2.7 Code",
    "kimi-k2.7-code": "Kimi K2.7 Code",
    "deepseek-v4pro": "DeepSeek V4 Pro",
    "glm-5.2": "GLM 5.2",

    # Backend Model IDs
    "orange-mousse": "GPT-5.6 Sol",
    "orchid-muffin": "GPT-5.6 Terra",
    "olive-jellyroll": "GPT-5.6 Luna",
    "oatmeal-cookie": "GPT-5.2",
    "oval-kumquat-medium": "GPT-5.4",
    "opal-quince-medium": "GPT-5.5",
    "vertex-gemini-2.5-flash": "Gemini 2.5 Flash",
    "vertex-gemini-3.5-flash": "Gemini 3.5 Flash",
    "almond-croissant-low": "Sonnet 4.6",
    "angel-cake-high": "Sonnet 5",
    "avocado-froyo-medium": "Opus 4.6",
    "apricot-sorbet-high": "Opus 4.7",
    "ambrosia-tart-high": "Opus 4.8",
    "oregon-grape-medium": "GPT-5.4 Mini",
    "otaheite-apple-medium": "GPT-5.4 Nano",
    "fireworks-minimax-m2.5": "MiniMax M2.5",
    "fireworks-kimi-k2.6": "Kimi K2.6",
    "fireworks-kimi-k2.7": "Kimi K2.7 Code",
    "baseten-deepseek-v4-pro": "DeepSeek V4 Pro",
    "baseten-glm-5.2": "GLM 5.2",
    "xigua-mochi-medium": "Grok 4.3",
    "strawberry-whoopiepie": "SpaceXAI 4.5",
    "xinomavro-cake": "Grok Build 0.1",
    "galette-medium-thinking": "Gemini 3.1 Pro",
    "anthropic-haiku-4.5": "Haiku 4.5",
    "gingerbread": "Gemini 3 Flash",
    "acai-budino": "Fable 5",
    "acai-budino-high": "Fable 5",
}


# Only canonical Notion model codenames are advertised by /v1/models.
# Friendly names and compatibility aliases remain accepted for requests through MODEL_MAP.
STATIC_DISABLED_MODEL_IDS: set[str] = {"acai-budino-high"}
EXPOSED_MODEL_IDS: tuple[str, ...] = tuple(
    model_id
    for model_id in NOTION_MODEL_REVERSE_MAP
    if model_id not in STATIC_DISABLED_MODEL_IDS
)

MODEL_DISPLAY_GROUPS: dict[str, str] = {
    "orange-mousse": "intelligent",
    "orchid-muffin": "intelligent",
    "olive-jellyroll": "fast",
    "oatmeal-cookie": "fast",
    "oval-kumquat-medium": "fast",
    "opal-quince-medium": "intelligent",
    "vertex-gemini-3.5-flash": "fast",
    "almond-croissant-low": "fast",
    "angel-cake-high": "intelligent",
    "avocado-froyo-medium": "intelligent",
    "apricot-sorbet-high": "intelligent",
    "ambrosia-tart-high": "intelligent",
    "oregon-grape-medium": "fast",
    "otaheite-apple-medium": "fast",
    "fireworks-kimi-k2.6": "intelligent",
    "fireworks-kimi-k2.7": "intelligent",
    "baseten-deepseek-v4-pro": "intelligent",
    "baseten-glm-5.2": "intelligent",
    "xigua-mochi-medium": "intelligent",
    "strawberry-whoopiepie": "intelligent",
    "xinomavro-cake": "intelligent",
    "galette-medium-thinking": "intelligent",
    "anthropic-haiku-4.5": "fast",
    "gingerbread": "fast",
    "acai-budino": "intelligent",
    "acai-budino-high": "intelligent",
}

MODEL_CARD_ATTRIBUTES: dict[str, dict[str, int]] = {
    "orange-mousse": {"speed": 3, "intelligence": 5, "cost": 5},
    "orchid-muffin": {"speed": 4, "intelligence": 4, "cost": 4},
    "olive-jellyroll": {"speed": 5, "intelligence": 3, "cost": 2},
    "oatmeal-cookie": {"speed": 4, "intelligence": 4, "cost": 3},
    "oval-kumquat-medium": {"speed": 4, "intelligence": 5, "cost": 4},
    "opal-quince-medium": {"speed": 4, "intelligence": 5, "cost": 5},
    "vertex-gemini-3.5-flash": {"speed": 5, "intelligence": 3, "cost": 3},
    "almond-croissant-low": {"speed": 3, "intelligence": 5, "cost": 4},
    "angel-cake-high": {"speed": 3, "intelligence": 5, "cost": 3},
    "avocado-froyo-medium": {"speed": 2, "intelligence": 5, "cost": 5},
    "apricot-sorbet-high": {"speed": 2, "intelligence": 5, "cost": 5},
    "ambrosia-tart-high": {"speed": 2, "intelligence": 5, "cost": 5},
    "oregon-grape-medium": {"speed": 5, "intelligence": 2, "cost": 2},
    "otaheite-apple-medium": {"speed": 5, "intelligence": 1, "cost": 1},
    "fireworks-kimi-k2.6": {"speed": 5, "intelligence": 4, "cost": 2},
    "fireworks-kimi-k2.7": {"speed": 5, "intelligence": 4, "cost": 2},
    "baseten-deepseek-v4-pro": {"speed": 3, "intelligence": 5, "cost": 4},
    "baseten-glm-5.2": {"speed": 3, "intelligence": 5, "cost": 3},
    "xigua-mochi-medium": {"speed": 3, "intelligence": 5, "cost": 4},
    "strawberry-whoopiepie": {"speed": 3, "intelligence": 5, "cost": 4},
    "xinomavro-cake": {"speed": 3, "intelligence": 5, "cost": 4},
    "anthropic-haiku-4.5": {"speed": 5, "intelligence": 2, "cost": 2},
    "gingerbread": {"speed": 5, "intelligence": 2, "cost": 2},
    "acai-budino-high": {"speed": 2, "intelligence": 5, "cost": 5},
}

MODEL_FAMILIES: dict[str, str] = {
    "almond-croissant-low": "anthropic",
    "angel-cake-high": "anthropic",
    "avocado-froyo-medium": "anthropic",
    "apricot-sorbet-high": "anthropic",
    "ambrosia-tart-high": "anthropic",
    "anthropic-haiku-4.5": "anthropic",
    "acai-budino": "anthropic",
    "acai-budino-high": "anthropic",
    "orange-mousse": "openai",
    "orchid-muffin": "openai",
    "olive-jellyroll": "openai",
    "oatmeal-cookie": "openai",
    "oval-kumquat-medium": "openai",
    "opal-quince-medium": "openai",
    "oregon-grape-medium": "openai",
    "otaheite-apple-medium": "openai",
    "gingerbread": "gemini",
    "galette-medium-thinking": "gemini",
    "vertex-gemini-3.5-flash": "gemini",
    "vertex-gemini-2.5-flash": "gemini",
    "xigua-mochi-medium": "xai",
    "strawberry-whoopiepie": "xai",
    "xinomavro-cake": "xai",
    "fireworks-minimax-m2.5": "minimax",
    "fireworks-kimi-k2.6": "kimi",
    "fireworks-kimi-k2.7": "kimi",
    "baseten-deepseek-v4-pro": "deepseek",
    "baseten-glm-5.2": "glm",
}

MODEL_ICONS: dict[str, str] = {
    # Anthropic
    "claude-sonnet4.6": "✳️",
    "claude-sonnet5": "✳️",
    "claude-opus4.6": "✳️",
    "claude-opus4.7": "✳️",
    "claude-opus4.8": "✳️",
    "claude-haiku4.5": "✳️",
    "claude-fable5": "✳️",
    # OpenAI
    "gpt-5.6-sol": "⚙",
    "gpt-5.6-terra": "⚙",
    "gpt-5.6-luna": "⚙",
    "gpt-5.2": "⚙",
    "gpt-5.4": "⚙",
    "gpt-5.4mini": "⚙",
    "gpt-5.4nano": "⚙",
    "gpt-5.5": "⚙",
    # Google
    "gemini-3flash": "✦",
    "gemini-3-flash": "✦",
    "gemini-3.1pro": "✦",
    "gemini-3.1-pro": "✦",
    "gemini-3.5flash": "✦",
    "gemini-2.5flash": "✦",
    # xAI
    "grok-4.3": "◐",
    "grok-4.5": "◐",
    "grok-build0.1": "◐",
    # Other
    "minimax-m2.5": "◈",
    "kimi-2.6": "🌙",
    "kimi-2.7": "🌙",
    "kimi-2.7-code": "🌙",
    "kimi-k2.7": "🌙",
    "kimi-k2.7-code": "🌙",
    "deepseek-v4pro": "🔷",
    "glm-5.2": "◆",

    # Backend Model IDs
    "almond-croissant-low": "✳️",
    "angel-cake-high": "✳️",
    "avocado-froyo-medium": "✳️",
    "apricot-sorbet-high": "✳️",
    "ambrosia-tart-high": "✳️",
    "anthropic-haiku-4.5": "✳️",
    "acai-budino": "✳️",
    "acai-budino-high": "✳️",
    "orange-mousse": "⚙",
    "orchid-muffin": "⚙",
    "olive-jellyroll": "⚙",
    "oatmeal-cookie": "⚙",
    "oval-kumquat-medium": "⚙",
    "oregon-grape-medium": "⚙",
    "otaheite-apple-medium": "⚙",
    "opal-quince-medium": "⚙",
    "gingerbread": "✦",
    "galette-medium-thinking": "✦",
    "vertex-gemini-3.5-flash": "✦",
    "vertex-gemini-2.5-flash": "✦",
    "xigua-mochi-medium": "◐",
    "strawberry-whoopiepie": "◐",
    "xinomavro-cake": "◐",
    "fireworks-minimax-m2.5": "◈",
    "fireworks-kimi-k2.6": "🌙",
    "fireworks-kimi-k2.7": "🌙",
    "baseten-deepseek-v4-pro": "🔷",
    "baseten-glm-5.2": "◆",
}

# Consumer-facing default. "terra" resolves to Notion's orchid-muffin route.
DEFAULT_MODEL = "terra"


def _normalize_registry_model_name(model_name: str) -> str:
    normalized = normalize_model_id(model_name)
    candidate = str(normalized or "").strip().lower()
    candidate = re.sub(r"[\s_]+", "-", candidate)
    return re.sub(r"-+", "-", candidate).strip("-")


def get_notion_model(model_name: str) -> str:
    normalized_name = get_standard_model(model_name)
    return MODEL_MAP.get(normalized_name, MODEL_MAP[DEFAULT_MODEL])


# Notion's public model metadata currently advertises the newer Gemini models
# through workflow/custom-agent capable routes. Keeping them on markdown-chat
# causes Notion upstream 502 responses.
MARKDOWN_CHAT_MODELS: set[str] = {
}


def is_gemini_model(model_name: str) -> bool:
    """Return whether the model belongs to the Gemini family."""
    standard_name = get_standard_model(model_name)
    if standard_name.startswith("gemini-"):
        return True
    notion_model = get_notion_model(standard_name)
    return notion_model.startswith("vertex-") or notion_model.startswith("galette-")


def get_thread_type(model_name: str) -> str:
    """
    Resolve the Notion thread type for a model.
    Only vertex-prefixed models use markdown-chat; all other models use workflow.
    """
    standard_name = get_standard_model(model_name)
    notion_model = get_notion_model(standard_name)
    if notion_model in MARKDOWN_CHAT_MODELS:
        return "markdown-chat"
    return "workflow"


def get_standard_model(model_name: str) -> str:
    model_name = _normalize_registry_model_name(model_name)
    if not model_name:
        return DEFAULT_MODEL
    if model_name in NOTION_MODEL_REVERSE_MAP:
        return NOTION_MODEL_REVERSE_MAP[model_name]
    if model_name in MODEL_MAP:
        return model_name
    return DEFAULT_MODEL


def list_available_models() -> list[str]:
    """Return one canonical selectable ID per underlying Notion model."""
    return list(EXPOSED_MODEL_IDS)


def get_model_metadata(model_name: str) -> dict[str, object]:
    """Return canonical model metadata without exposing compatibility aliases as models."""
    notion_model = get_notion_model(model_name)
    public_name = NOTION_MODEL_REVERSE_MAP.get(notion_model, DEFAULT_MODEL)

    if notion_model.startswith("fireworks-"):
        upstream_host = "fireworks"
    elif notion_model.startswith("baseten-"):
        upstream_host = "baseten"
    elif notion_model.startswith("vertex-"):
        upstream_host = "vertex"
    else:
        upstream_host = "notion"

    aliases = [
        alias
        for alias, target in MODEL_MAP.items()
        if target == notion_model and alias != notion_model
    ]

    is_disabled = notion_model in STATIC_DISABLED_MODEL_IDS
    return {
        "canonical_id": notion_model,
        "public_name": public_name,
        "display_name": DISPLAY_NAMES.get(notion_model, DISPLAY_NAMES.get(public_name, public_name)),
        "model_family": MODEL_FAMILIES.get(notion_model, "unknown"),
        "display_group": MODEL_DISPLAY_GROUPS.get(notion_model, ""),
        "model_card_attributes": MODEL_CARD_ATTRIBUTES.get(notion_model),
        "is_disabled": is_disabled,
        "disabled_reason": "trial_not_allowed" if is_disabled else "",
        "transport": "notion2api",
        "upstream_host": upstream_host,
        "aliases": aliases,
    }


def is_static_disabled_model(model_name: str) -> bool:
    normalized_name = _normalize_registry_model_name(model_name)
    if not normalized_name:
        return False
    if normalized_name in NOTION_MODEL_REVERSE_MAP:
        notion_model = normalized_name
    elif normalized_name in MODEL_MAP:
        notion_model = MODEL_MAP[normalized_name]
    else:
        return False
    return notion_model in STATIC_DISABLED_MODEL_IDS


def is_supported_model(model_name: str) -> bool:
    normalized_name = _normalize_registry_model_name(model_name)
    if not normalized_name or normalized_name not in MODEL_MAP:
        return False
    return not is_static_disabled_model(normalized_name)


def get_display_name(model_name: str) -> str:
    standard_name = get_standard_model(model_name)
    notion_model = get_notion_model(standard_name)
    public_name = NOTION_MODEL_REVERSE_MAP.get(notion_model, standard_name)
    return DISPLAY_NAMES.get(
        standard_name,
        DISPLAY_NAMES.get(notion_model, DISPLAY_NAMES.get(public_name, public_name)),
    )


def get_model_icon(model_name: str) -> str:
    standard_name = get_standard_model(model_name)
    notion_model = get_notion_model(standard_name)
    public_name = NOTION_MODEL_REVERSE_MAP.get(notion_model, standard_name)
    return MODEL_ICONS.get(
        standard_name,
        MODEL_ICONS.get(notion_model, MODEL_ICONS.get(public_name, "")),
    )


_RESTRICTED_MODELS_CACHE: dict[str, tuple[float, set[str]]] = {}
_RESTRICTED_MODELS_LOCKS: dict[str, threading.Lock] = {}
_RESTRICTED_MODELS_LOCK_GUARD = threading.Lock()
_SHARED_RESTRICTION_CACHE = ModelRestrictionCache()
_RESTRICTION_CACHE_OWNER = f"{os.getpid()}:{uuid.uuid4().hex}"


def _restriction_cache_ttl_seconds() -> float:
    try:
        return max(1.0, float(os.getenv("NOTION_MODEL_RESTRICTION_CACHE_TTL_SECONDS", "300")))
    except (TypeError, ValueError):
        return 300.0


def _restriction_refresh_wait_seconds() -> float:
    try:
        return max(0.1, float(os.getenv("NOTION_MODEL_RESTRICTION_REFRESH_WAIT_SECONDS", "10")))
    except (TypeError, ValueError):
        return 10.0


def _restriction_lock(space_id: str) -> threading.Lock:
    with _RESTRICTED_MODELS_LOCK_GUARD:
        return _RESTRICTED_MODELS_LOCKS.setdefault(space_id, threading.Lock())


def _restricted_models_from_picker_config(config: dict) -> set[str]:
    restricted: set[str] = set()
    for item in config.get("restrictedAccessModelsInPickerConfig", []):
        codename = item.get("codename") if isinstance(item, dict) else None
        if codename:
            restricted.add(str(codename))
    for item in config.get("models", []):
        if not isinstance(item, dict) or not item.get("isDisabled"):
            continue
        model_name = item.get("model") if item.get("disabledReason") else None
        codename = item.get("restrictedAccessModelCodename")
        if model_name:
            restricted.add(str(model_name))
        if codename:
            restricted.add(str(codename))
    return restricted


def _shared_restricted_models(space_id: str, *, allow_stale: bool = False) -> set[str] | None:
    cached = _SHARED_RESTRICTION_CACHE.get(space_id, allow_stale=allow_stale)
    if not cached:
        return None
    payload = cached.get("payload") if isinstance(cached, dict) else None
    values = payload.get("restricted_models") if isinstance(payload, dict) else None
    if not isinstance(values, list):
        return None
    return {str(value) for value in values if str(value)}


def get_restricted_models_for_space(client) -> set[str]:
    now = time.time()
    space_id = str(client.space_id or "").strip()
    account_id = str(
        getattr(client, "user_id", "")
        or getattr(client, "account_key", "")
        or getattr(client, "user_email", "")
        or "unknown-account"
    ).strip()
    cache_key = f"{space_id}:{account_id}"
    ttl_seconds = _restriction_cache_ttl_seconds()
    local = _RESTRICTED_MODELS_CACHE.get(cache_key)
    if local and now - local[0] < ttl_seconds:
        return set(local[1])

    with _restriction_lock(cache_key):
        now = time.time()
        local = _RESTRICTED_MODELS_CACHE.get(cache_key)
        if local and now - local[0] < ttl_seconds:
            return set(local[1])
        shared = _shared_restricted_models(cache_key)
        if shared is not None:
            _RESTRICTED_MODELS_CACHE[cache_key] = (now, shared)
            return set(shared)

        stale = (
            set(local[1])
            if local
            else (_shared_restricted_models(cache_key, allow_stale=True) or set())
        )
        deadline = time.monotonic() + _restriction_refresh_wait_seconds()
        while True:
            lease_token = _SHARED_RESTRICTION_CACHE.claim_refresh(
                cache_key,
                owner_id=_RESTRICTION_CACHE_OWNER,
                lease_seconds=max(5.0, _restriction_refresh_wait_seconds()),
            )
            if lease_token:
                try:
                    restricted = _restricted_models_from_picker_config(
                        client.get_ai_model_picker_config()
                    )
                    stored = _SHARED_RESTRICTION_CACHE.store(
                        cache_key,
                        {"restricted_models": sorted(restricted)},
                        lease_token=lease_token,
                        ttl_seconds=ttl_seconds,
                    )
                    if not stored:
                        shared = _shared_restricted_models(cache_key)
                        return set(shared if shared is not None else stale)
                    _RESTRICTED_MODELS_CACHE[cache_key] = (
                        time.time(),
                        restricted,
                    )
                    return set(restricted)
                except Exception as exc:
                    _SHARED_RESTRICTION_CACHE.release_refresh(
                        cache_key, lease_token=lease_token
                    )
                    logger.warning(
                        "Failed to fetch restricted models for "
                        f"space/account {cache_key}: {exc}"
                    )
                    return stale

            shared = _shared_restricted_models(cache_key)
            if shared is not None:
                _RESTRICTED_MODELS_CACHE[cache_key] = (time.time(), shared)
                return set(shared)
            if time.monotonic() >= deadline:
                logger.warning(
                    "Timed out waiting for restricted model refresh for "
                    f"space/account {cache_key}"
                )
                return stale
            time.sleep(0.05)

def list_available_models_for_request(request) -> list[str]:
    try:
        pool = request.app.state.account_pool
        client = pool.get_client(wait_if_cooling=False)
        restricted = get_restricted_models_for_space(client)
    except Exception:
        restricted = set()

    filtered = []
    for model_id in EXPOSED_MODEL_IDS:
        notion_model = get_notion_model(model_id)
        if notion_model in restricted or model_id in restricted:
            continue
        filtered.append(model_id)
    return filtered
