import time
from typing import Any, Dict, List, Literal, Optional
from pydantic import BaseModel, Field

# ================================
# Request schemas for chat completions.
# ================================


class ChatMessage(BaseModel):
    """Chat message."""
    role: Literal["user", "assistant", "system"]
    # OpenAI-compatible clients may send either plain text or structured content
    # parts such as input_text, image_url, input_image, file, input_file.
    content: Any
    thinking: Optional[str] = None
    reasoning_content: Optional[str] = None


class ChatCompletionRequest(BaseModel):
    """
    OpenAI-compatible request payload.
    Keep `conversation_id` as an extension field.

    Attachment handling note:
    - Keep top-level `attachments` separate from `messages` at the schema layer.
    - app.attachments.normalizer.normalize_chat_messages() is the single canonical
      place that merges/normalizes structured content parts and top-level attachments.
    - Mutating attachments into the last user message here caused heavy-mode code to
      see a hybrid request before normalization, which could duplicate screenshot/file
      content across message preparation, persistence, and upstream dispatch.
    """
    model: str = Field(default="terra", description="Requested model. Defaults to the consumer-friendly Terra alias.")
    messages: List[ChatMessage]
    stream: bool = Field(default=False, description="Whether to stream the response as SSE.")
    temperature: Optional[float] = Field(default=None, description="Sampling temperature.")
    conversation_id: Optional[str] = Field(default=None, description="Extension for stateful conversation tracking.")
    chat_title: Optional[str] = Field(default=None, description="Preferred persistent chat thread title.")
    title: Optional[str] = Field(default=None, description="Compatibility alias for chat_title.")
    session_name: Optional[str] = Field(default=None, description="Compatibility alias for chat_title.")
    attachments: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Optional attachment descriptors. Normalized by the chat handler.",
    )
    metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional caller metadata for per-request behavior such as remote chat persistence.",
    )

# ================================
# Non-streaming response schema.
# ================================

class ChatMessageResponseChoice(BaseModel):
    """Chat message."""
    index: int = 0
    message: ChatMessage
    finish_reason: str = "stop"

class ChatCompletionResponse(BaseModel):
    """
    OpenAI-compatible request payload.
    """
    id: str
    object: str = "chat.completion"
    created: int = Field(default_factory=lambda: int(time.time()))
    model: str
    choices: List[ChatMessageResponseChoice]
    requested_model: Optional[str] = Field(default=None)
    notion_requested_model: Optional[str] = Field(default=None)
    actual_model: Optional[str] = Field(default=None)
    model_metadata: Optional[Dict[str, Any]] = Field(default=None)
    hygiene: Optional[Dict[str, bool]] = Field(default=None)
    usage: Dict[str, int] = Field(
        default_factory=lambda: {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    )
    # Standard-mode extension fields.
    search_metadata: Optional[Dict[str, Any]] = Field(default=None)

# ================================
# Non-streaming response schema. (text)
# ================================

class ChatCompletionChunkDelta(BaseModel):
    content: Optional[str] = None
    role: Optional[str] = None

class ChatCompletionChunkChoice(BaseModel):
    index: int = 0
    delta: ChatCompletionChunkDelta
    finish_reason: Optional[str] = None

class ChatCompletionChunk(BaseModel):
    id: str
    object: str = "chat.completion.chunk"
    created: int = Field(default_factory=lambda: int(time.time()))
    model: str
    choices: List[ChatCompletionChunkChoice]
