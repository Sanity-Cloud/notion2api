"""Security policy for attachment ingestion."""

from __future__ import annotations

from dataclasses import dataclass, field
import ipaddress
import os
import socket
from urllib.parse import urlparse

from app.attachments.errors import AttachmentError

DEFAULT_EXTENSION_MIME_TYPES = {
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".markdown": "text/markdown",
    ".patch": "text/x-diff",
    ".diff": "text/x-diff",
    ".csv": "text/csv",
    ".tsv": "text/tab-separated-values",
    ".json": "application/json",
    ".jsonl": "application/x-ndjson",
    ".ndjson": "application/x-ndjson",
    ".xml": "application/xml",
    ".yaml": "application/yaml",
    ".yml": "application/yaml",
    ".toml": "application/toml",
    ".ini": "text/plain",
    ".cfg": "text/plain",
    ".conf": "text/plain",
    ".py": "text/x-python",
    ".js": "text/javascript",
    ".mjs": "text/javascript",
    ".cjs": "text/javascript",
    ".ts": "text/typescript",
    ".tsx": "text/tsx",
    ".jsx": "text/jsx",
    ".java": "text/x-java-source",
    ".cs": "text/x-csharp",
    ".c": "text/x-c",
    ".cc": "text/x-c++src",
    ".cpp": "text/x-c++src",
    ".cxx": "text/x-c++src",
    ".h": "text/x-c++hdr",
    ".hpp": "text/x-c++hdr",
    ".rs": "text/x-rust",
    ".go": "text/x-go",
    ".rb": "text/x-ruby",
    ".php": "application/x-httpd-php",
    ".sh": "application/x-sh",
    ".ps1": "text/x-powershell",
    ".psm1": "text/x-powershell",
    ".psd1": "text/x-powershell",
    ".sql": "application/sql",
    ".css": "text/css",
    ".html": "text/html",
    ".htm": "text/html",
    ".pdf": "application/pdf",
    ".rtf": "application/rtf",
    ".doc": "application/msword",
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xls": "application/vnd.ms-excel",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".ppt": "application/vnd.ms-powerpoint",
    ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ".odt": "application/vnd.oasis.opendocument.text",
    ".ods": "application/vnd.oasis.opendocument.spreadsheet",
    ".odp": "application/vnd.oasis.opendocument.presentation",
    ".epub": "application/epub+zip",
    ".zip": "application/zip",
    ".tar": "application/x-tar",
    ".gz": "application/gzip",
    ".tgz": "application/gzip",
    ".bz2": "application/x-bzip2",
    ".xz": "application/x-xz",
    ".7z": "application/x-7z-compressed",
    ".rar": "application/vnd.rar",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".heic": "image/heic",
    ".svg": "image/svg+xml",
    ".bmp": "image/bmp",
    ".tif": "image/tiff",
    ".tiff": "image/tiff",
}

CONTENT_TYPE_ALIASES = {
    "application/x-zip-compressed": "application/zip",
    "application/x-yaml": "application/yaml",
    "text/yaml": "application/yaml",
    "text/x-markdown": "text/markdown",
    "application/x-json": "application/json",
    "text/json": "application/json",
    "application/x-gzip": "application/gzip",
    "application/x-rar-compressed": "application/vnd.rar",
}

DEFAULT_ALLOWED_MIME_TYPES = set(DEFAULT_EXTENSION_MIME_TYPES.values())

_PRIVATE_HOSTNAMES = {"localhost", "localhost.localdomain"}
_DEFAULT_ALLOWED_PORTS = {80, 443}


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _env_csv_set(name: str, default: set[str]) -> set[str]:
    value = os.getenv(name)
    if not value:
        return set(default)
    parsed = {
        CONTENT_TYPE_ALIASES.get(item.strip().lower(), item.strip().lower())
        for item in value.split(",")
        if item.strip()
    }
    return parsed or set(default)


@dataclass(slots=True)
class AttachmentPolicy:
    """Configurable attachment safety limits."""

    enabled: bool = False
    max_attachments_per_request: int = 5
    max_attachment_bytes: int = 1000 * 1024 * 1024
    allow_remote_urls: bool = True
    allow_local_paths: bool = True
    allow_non_default_remote_ports: bool = False
    max_redirects: int = 3
    download_timeout_seconds: int = 20
    allowed_mime_types: set[str] = field(default_factory=lambda: set(DEFAULT_ALLOWED_MIME_TYPES))
    local_root: str = r"X:\Code"

    @classmethod
    def from_env(cls) -> "AttachmentPolicy":
        return cls(
            enabled=_env_bool("ENABLE_ATTACHMENTS", False),
            max_attachments_per_request=_env_int("MAX_ATTACHMENTS_PER_REQUEST", 5),
            max_attachment_bytes=_env_int("MAX_ATTACHMENT_BYTES", 1000 * 1024 * 1024),
            allow_remote_urls=_env_bool("ALLOW_REMOTE_ATTACHMENT_URLS", True),
            allow_local_paths=_env_bool("ALLOW_LOCAL_ATTACHMENT_PATHS", True),
            allow_non_default_remote_ports=_env_bool("ALLOW_NON_DEFAULT_ATTACHMENT_PORTS", False),
            max_redirects=_env_int("ATTACHMENT_MAX_REDIRECTS", 3),
            download_timeout_seconds=_env_int("ATTACHMENT_DOWNLOAD_TIMEOUT_SECONDS", 20),
            allowed_mime_types=_env_csv_set("ATTACHMENT_ALLOWED_MIME_TYPES", DEFAULT_ALLOWED_MIME_TYPES),
            local_root=os.getenv("ATTACHMENT_LOCAL_ROOT", r"X:\Code").strip(),
        )


def normalize_content_type(content_type: str) -> str:
    normalized = str(content_type or "").split(";", 1)[0].strip().lower()
    return CONTENT_TYPE_ALIASES.get(normalized, normalized)


def validate_attachment_count(count: int, policy: AttachmentPolicy | None = None) -> None:
    policy = policy or AttachmentPolicy.from_env()
    if count > policy.max_attachments_per_request:
        raise AttachmentError(
            f"Too many attachments: {count}. Maximum is {policy.max_attachments_per_request}.",
            code="too_many_attachments",
            param="attachments",
        )


def validate_content_type(content_type: str, policy: AttachmentPolicy | None = None) -> str:
    policy = policy or AttachmentPolicy.from_env()
    normalized = normalize_content_type(content_type)
    if not normalized:
        raise AttachmentError(
            "Attachment content type is required.",
            code="attachment_content_type_required",
            param="attachments.content_type",
        )
    allowed = {normalize_content_type(item) for item in policy.allowed_mime_types}
    if normalized not in allowed:
        raise AttachmentError(
            f"Unsupported attachment content type: {normalized}",
            code="unsupported_attachment_type",
            param="attachments.content_type",
        )
    return normalized


def validate_size(size_bytes: int, policy: AttachmentPolicy | None = None) -> None:
    policy = policy or AttachmentPolicy.from_env()
    if size_bytes > policy.max_attachment_bytes:
        raise AttachmentError(
            f"Attachment is too large: {size_bytes} bytes. Maximum is {policy.max_attachment_bytes} bytes.",
            code="attachment_too_large",
            param="attachments",
            status_code=413,
        )


def is_blocked_ip(ip_text: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_text)
    except ValueError:
        return True
    return any(
        (
            ip.is_private,
            ip.is_loopback,
            ip.is_link_local,
            ip.is_multicast,
            ip.is_reserved,
            ip.is_unspecified,
        )
    )


def _resolve_host(hostname: str) -> list[str]:
    try:
        infos = socket.getaddrinfo(hostname, None)
    except socket.gaierror as exc:
        raise AttachmentError(
            f"Could not resolve remote attachment host: {hostname}",
            code="attachment_url_resolution_failed",
            param="attachments.url",
        ) from exc

    addresses: list[str] = []
    for info in infos:
        sockaddr = info[4]
        if sockaddr:
            addresses.append(str(sockaddr[0]))
    return sorted(set(addresses))


def validate_remote_url(url: str, policy: AttachmentPolicy | None = None) -> str:
    policy = policy or AttachmentPolicy.from_env()
    if not policy.allow_remote_urls:
        raise AttachmentError(
            "Remote attachment URLs are disabled.",
            code="remote_attachment_urls_disabled",
            param="attachments.url",
        )

    parsed = urlparse(str(url or "").strip())
    if parsed.scheme.lower() not in {"http", "https"}:
        raise AttachmentError(
            "Remote attachment URL must use http or https.",
            code="attachment_url_scheme_blocked",
            param="attachments.url",
        )
    if parsed.username or parsed.password:
        raise AttachmentError(
            "Remote attachment URL must not contain embedded credentials.",
            code="attachment_url_credentials_blocked",
            param="attachments.url",
        )
    hostname = (parsed.hostname or "").strip().lower().rstrip(".")
    if not hostname:
        raise AttachmentError(
            "Remote attachment URL host is required.",
            code="attachment_url_host_required",
            param="attachments.url",
        )
    if hostname in _PRIVATE_HOSTNAMES:
        raise AttachmentError(
            "Remote attachment URL resolves to a private or loopback address.",
            code="attachment_url_blocked",
            param="attachments.url",
        )

    port = parsed.port
    if port and not policy.allow_non_default_remote_ports and port not in _DEFAULT_ALLOWED_PORTS:
        raise AttachmentError(
            "Remote attachment URL uses a blocked non-default port.",
            code="attachment_url_port_blocked",
            param="attachments.url",
        )

    addresses = _resolve_host(hostname)
    if not addresses or any(is_blocked_ip(address) for address in addresses):
        raise AttachmentError(
            "Remote attachment URL resolves to a private or loopback address.",
            code="attachment_url_blocked",
            param="attachments.url",
        )
    return parsed.geturl()


def validate_local_path_allowed(policy: AttachmentPolicy | None = None) -> None:
    policy = policy or AttachmentPolicy.from_env()
    if not policy.allow_local_paths:
        raise AttachmentError(
            "Local attachment paths are disabled.",
            code="local_attachment_paths_disabled",
            param="attachments.path",
        )
