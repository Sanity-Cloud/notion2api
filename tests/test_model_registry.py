from pathlib import Path

from app.model_registry import (
    get_display_name,
    get_model_metadata,
    get_model_route_resolution,
    get_notion_model,
    get_standard_model,
    get_thread_type,
    list_available_models,
    is_static_disabled_model,
    is_supported_model,
)


def test_captured_notion_backend_mappings_are_registered():
    expected = {
        "gpt-5.6-sol": "orange-mousse",
        "gpt-5.6-terra": "orchid-muffin",
        "gpt-5.6-luna": "olive-jellyroll",
        "gpt-5.2": "oatmeal-cookie",
        "gpt-5.4": "oval-kumquat-medium",
        "gpt-5.5": "opal-quince-medium",
        "gemini-2.5flash": "vertex-gemini-2.5-flash",
        "gemini-3.5flash": "vertex-gemini-3.5-flash",
        "claude-sonnet4.6": "almond-croissant-low",
        "claude-sonnet5": "angel-cake-high",
        "claude-opus4.6": "avocado-froyo-medium",
        "claude-opus4.7": "apricot-sorbet-high",
        "claude-opus4.8": "ambrosia-tart-high",
        "claude-opus5": "agave-flan",
        "gpt-5.4mini": "oregon-grape-medium",
        "gpt-5.4nano": "otaheite-apple-medium",
        "minimax-m2.5": "fireworks-minimax-m2.5",
        "kimi-2.6": "fireworks-kimi-k2.6",
        "kimi-2.7-code": "fireworks-kimi-k2.7",
        "kimi-3": "fireworks-kimi-k3",
        "deepseek-v4pro": "baseten-deepseek-v4-pro",
        "glm-5.2": "baseten-glm-5.2",
        "grok-4.3": "xigua-mochi-medium",
        "spacexai-4.5": "strawberry-whoopiepie",
        "grok-build0.1": "xinomavro-cake",
        "gemini-3.1pro": "galette-medium-thinking",
        "claude-haiku4.5": "anthropic-haiku-4.5",
        "gemini-3flash": "gingerbread",
        "claude-fable5": "acai-budino-high",
    }

    for public_name, notion_name in expected.items():
        assert get_notion_model(public_name) == notion_name
        if public_name == "claude-fable5":
            assert is_supported_model(public_name) is False
        else:
            assert is_supported_model(public_name)
        assert get_standard_model(notion_name) == public_name


def test_captured_display_names_are_registered():
    assert get_display_name("gpt-5.6-sol") == "GPT-5.6 Sol"
    assert get_display_name("orange-mousse") == "GPT-5.6 Sol"
    assert get_display_name("gpt-5.6-terra") == "GPT-5.6 Terra"
    assert get_display_name("gpt-5.6-luna") == "GPT-5.6 Luna"
    assert get_display_name("kimi-2.7-code") == "Kimi K2.7 Code"
    assert get_display_name("fireworks-kimi-k2.7") == "Kimi K2.7 Code"
    assert get_display_name("fireworks-kimi-k3") == "Kimi K3"
    assert get_display_name("claude-opus5") == "Claude Opus 5"
    assert get_display_name("grok-4.3") == "Grok 4.3"
    assert get_display_name("grok-4.5") == "SpaceXAI 4.5"
    assert get_display_name("spacexai-4.5") == "SpaceXAI 4.5"
    assert get_display_name("strawberry-whoopiepie") == "SpaceXAI 4.5"
    assert get_display_name("grok-build0.1") == "Grok Build 0.1"
    assert get_display_name("minimax-m2.5") == "MiniMax M2.5"
    assert get_display_name("claude-sonnet5") == "Claude Sonnet 5"
    assert get_display_name("claude-haiku4.5") == "Claude Haiku 4.5"
    assert get_display_name("claude-fable5") == "Fable 5"
    assert get_display_name("glm-5.2") == "GLM 5.2"


def test_gemini_3_5_flash_no_longer_uses_markdown_chat_route():
    assert get_thread_type("gemini-2.5flash") == "workflow"
    assert get_thread_type("gemini-3.5flash") == "workflow"


def test_available_models_expose_only_canonical_notion_ids():
    models = list_available_models()

    assert len(models) == 28
    assert len(models) == len(set(models))
    assert "orange-mousse" in models
    assert "orchid-muffin" in models
    assert "olive-jellyroll" in models
    assert "fireworks-kimi-k2.7" in models
    assert "fireworks-kimi-k3" in models
    assert "agave-flan" in models
    assert "acai-budino-high" not in models
    assert "angel-cake-high" in models
    assert "claude-sonnet5" not in models
    assert "apricot-sorbet-high" in models
    assert "claude-opus4.7" not in models
    assert "baseten-glm-5.2" in models
    assert "glm-5.2" not in models
    assert "strawberry-whoopiepie" in models
    assert "grok-4.5" not in models


def test_disabled_picker_models_have_metadata_but_are_not_selectable():
    fable = get_model_metadata("acai-budino-high")

    assert is_static_disabled_model("acai-budino-high") is True
    assert is_supported_model("acai-budino-high") is False
    assert fable["canonical_id"] == "acai-budino-high"
    assert fable["display_name"] == "Fable 5"
    assert fable["is_disabled"] is True
    assert fable["disabled_reason"] == "trial_not_allowed"
    assert "acai-budino-high" not in list_available_models()


def test_model_metadata_preserves_transport_and_underlying_family():
    sonnet = get_model_metadata("claude-sonnet5")
    opus = get_model_metadata("claude-opus4.7")
    glm = get_model_metadata("baseten-glm-5.2")
    gpt56 = get_model_metadata("orange-mousse")

    assert {
        "canonical_id": "angel-cake-high",
        "public_name": "claude-sonnet5",
        "display_name": "Sonnet 5",
        "model_family": "anthropic",
        "display_group": "intelligent",
        "transport": "notion2api",
        "upstream_host": "notion",
        "is_disabled": False,
    }.items() <= sonnet.items()
    assert sonnet["aliases"] == ["claude-sonnet5", "claude-sonnet-5", "sonnet-5", "sonnet5"]
    assert sonnet["model_card_attributes"] == {"speed": 3, "intelligence": 5, "cost": 3}

    assert {
        "canonical_id": "apricot-sorbet-high",
        "public_name": "claude-opus4.7",
        "display_name": "Opus 4.7",
        "model_family": "anthropic",
        "display_group": "intelligent",
        "transport": "notion2api",
        "upstream_host": "notion",
        "is_disabled": False,
    }.items() <= opus.items()
    assert opus["aliases"] == ["claude-opus4.7", "claude-opus-4.7", "opus-4.7", "opus4.7"]

    assert {
        "canonical_id": "baseten-glm-5.2",
        "public_name": "glm-5.2",
        "display_name": "GLM 5.2",
        "model_family": "glm",
        "display_group": "intelligent",
        "transport": "notion2api",
        "upstream_host": "baseten",
        "is_disabled": False,
    }.items() <= glm.items()
    assert glm["aliases"] == ["glm-5.2"]

    assert {
        "canonical_id": "orange-mousse",
        "public_name": "gpt-5.6-sol",
        "display_name": "GPT-5.6 Sol",
        "model_family": "openai",
        "display_group": "intelligent",
        "transport": "notion2api",
        "upstream_host": "notion",
        "is_disabled": False,
    }.items() <= gpt56.items()
    assert "gpt-5.6-sol" in gpt56["aliases"]
    assert gpt56["model_card_attributes"] == {"speed": 3, "intelligence": 5, "cost": 5}

def test_consumer_friendly_model_aliases_and_default():
    from app import model_registry

    assert model_registry.DEFAULT_MODEL == "terra"
    assert is_supported_model("terra")
    assert is_supported_model("Terra")
    assert get_notion_model("terra") == "orchid-muffin"
    assert get_notion_model("Terra") == "orchid-muffin"
    assert get_notion_model("sol") == "orange-mousse"
    assert get_notion_model("luna") == "olive-jellyroll"


def test_frontend_defaults_to_selectable_terra_model():
    root = Path(__file__).resolve().parents[1]
    for path in (root / "frontend/index.html", root / "frontend/js/core/constants.js"):
        source = path.read_text(encoding="utf-8")
        assert 'DEFAULT_MODEL:"terra"' in source.replace(" ", "")
        assert 'id:"terra"' in source.replace(" ", "")

    served_page = (root / "frontend/index.html").read_text(encoding="utf-8")
    assert 'id="modelTriggerProvider" class="model-provider">OpenAI</span>' in served_page
    assert 'id="modelTriggerText" class="model-name">GPT-5.6 Terra</span>' in served_page

def test_visible_selector_names_resolve_to_exact_backend_routes():
    expected = {
        "Sonnet 4.6": "almond-croissant-low",
        "Sonnet 5": "angel-cake-high",
        "Opus 4.7": "apricot-sorbet-high",
        "Opus 4.8": "ambrosia-tart-high",
        "Opus 5": "agave-flan",
        "Fable 5": "acai-budino-high",
        "Gemini 3.1 Pro": "galette-medium-thinking",
        "GPT-5.6 Sol": "orange-mousse",
        "GPT-5.6 Terra": "orchid-muffin",
        "GPT-5.2": "oatmeal-cookie",
        "GPT-5.4": "oval-kumquat-medium",
        "GPT-5.5": "opal-quince-medium",
        "Grok 4.3": "xigua-mochi-medium",
        "SpaceXAI 4.5": "strawberry-whoopiepie",
        "Grok Build 0.1": "xinomavro-cake",
        "Gemini 3.5 Flash": "vertex-gemini-3.5-flash",
        "Kimi K2.6": "fireworks-kimi-k2.6",
        "Kimi K2.7 Code": "fireworks-kimi-k2.7",
        "Kimi K3": "fireworks-kimi-k3",
        "DeepSeek V4 Pro": "baseten-deepseek-v4-pro",
        "GLM 5.2": "baseten-glm-5.2",
    }

    for selector_name, backend_route in expected.items():
        assert get_notion_model(selector_name) == backend_route
        if selector_name == "Fable 5":
            assert is_supported_model(selector_name) is False
        else:
            assert is_supported_model(selector_name), selector_name

    assert is_static_disabled_model("Sonnet 4.6") is False


def test_legacy_grok_4_5_alias_remains_compatible():
    assert is_supported_model("grok-4.5")
    assert get_notion_model("grok-4.5") == "strawberry-whoopiepie"
    assert get_standard_model("strawberry-whoopiepie") == "spacexai-4.5"


def test_terra_route_resolution_is_explicit_alias_not_substitution():
    resolution = get_model_route_resolution("terra")

    assert resolution == {
        "requested_model": "terra",
        "canonical_model": "terra",
        "resolved_model": "orchid-muffin",
        "public_model": "gpt-5.6-terra",
        "display_name": "GPT-5.6 Terra",
        "resolution_kind": "configured_alias",
        "is_alias": True,
    }


def test_concrete_terra_route_remains_a_direct_route():
    resolution = get_model_route_resolution("orchid-muffin")

    assert resolution["requested_model"] == "orchid-muffin"
    assert resolution["resolved_model"] == "orchid-muffin"
    assert resolution["public_model"] == "gpt-5.6-terra"
    assert resolution["resolution_kind"] == "direct_route"
    assert resolution["is_alias"] is False
