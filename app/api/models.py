from typing import Dict, Any

from fastapi import APIRouter, Request

from app.model_registry import list_model_metadata_for_request

router = APIRouter()


@router.get("/models", tags=["models"])
async def list_models(request: Request) -> Dict[str, Any]:
    """
    List available models in OpenAI-compatible format.
    """
    models, catalog = list_model_metadata_for_request(request)
    data = [
        {
            "id": str(metadata.get("canonical_id") or ""),
            "object": "model",
            "created": 0,
            "owned_by": str(metadata.get("model_family") or "unknown"),
            **metadata,
        }
        for metadata in models
    ]
    return {"object": "list", "data": data, "catalog": catalog}
