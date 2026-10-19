from __future__ import annotations

import re
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, quote, unquote, urlparse

from acps_sdk.aic import validate_aic_format

INBOX_EXCHANGE_NAME = "inbox.topic"
INBOX_QUEUE_EXPIRES_MS = 60 * 24 * 60 * 60 * 1000
INBOX_MESSAGE_TTL_MS = 7 * 24 * 60 * 60 * 1000
DEFAULT_INVITATION_TIMEOUT_SECONDS = 300
INVITATION_CLOCK_SKEW_SECONDS = 30

_GROUP_ID_PATTERN = re.compile(r"^[A-Za-z0-9-]{1,128}$")
_GROUP_ID_INVALID_CHAR_PATTERN = re.compile(r"[^A-Za-z0-9-]+")


@dataclass(frozen=True)
class AMQPEndpoint:
    url: str
    host: str
    port: int
    vhost: str
    inbox: str


def ensure_valid_aic(aic: str) -> str:
    valid, error = validate_aic_format(aic)
    if not valid:
        raise ValueError(error or f"Invalid AIC: {aic}")
    return aic


def ensure_valid_group_id(group_id: str) -> str:
    if not _GROUP_ID_PATTERN.fullmatch(group_id):
        raise ValueError(
            "group_id must match [a-zA-Z0-9-] and be at most 128 characters"
        )
    return group_id


def normalize_group_id(group_id: str) -> str:
    normalized = _GROUP_ID_INVALID_CHAR_PATTERN.sub("-", group_id.strip()).strip("-")
    if not normalized:
        raise ValueError("group_id cannot be empty after normalization")
    if len(normalized) > 128:
        normalized = normalized[:128].rstrip("-")
    return ensure_valid_group_id(normalized)


def build_inbox_queue_name(aic: str) -> str:
    return f"inbox_{ensure_valid_aic(aic)}"


def build_group_exchange_name(leader_aic: str, group_id: str) -> str:
    return f"group_{ensure_valid_aic(leader_aic)}_{ensure_valid_group_id(group_id)}"


def build_group_queue_name(leader_aic: str, group_id: str, member_aic: str) -> str:
    return (
        f"{build_group_exchange_name(leader_aic, group_id)}_"
        f"{ensure_valid_aic(member_aic)}"
    )


def build_external_connection_url(
    *,
    host: str,
    port: int,
    vhost: str,
    connection_name: str,
) -> str:
    encoded_vhost = _encode_vhost(vhost)
    encoded_name = quote(connection_name, safe="")
    return (
        f"amqps://{host}:{port}/{encoded_vhost}" f"?auth=external&name={encoded_name}"
    )


def build_plain_connection_url(
    *,
    host: str,
    port: int,
    vhost: str,
    username: str,
    password: str,
    connection_name: Optional[str] = None,
) -> str:
    encoded_user = quote(username, safe="")
    encoded_password = quote(password, safe="")
    encoded_vhost = _encode_vhost(vhost)
    url = f"amqp://{encoded_user}:{encoded_password}@{host}:{port}/{encoded_vhost}"
    if connection_name:
        encoded_name = quote(connection_name, safe="")
        url = f"{url}?name={encoded_name}"
    return url


def parse_amqp_endpoint_url(url: str, *, aic: Optional[str] = None) -> AMQPEndpoint:
    resolved_url = replace_aic_placeholder(url, aic)
    parsed = urlparse(resolved_url)
    if parsed.scheme not in {"amqp", "amqps"}:
        raise ValueError(f"Unsupported AMQP endpoint scheme: {parsed.scheme}")
    if not parsed.hostname:
        raise ValueError(f"AMQP endpoint missing hostname: {resolved_url}")

    query = parse_qs(parsed.query)
    inbox = query.get("inbox", [None])[0]
    if not inbox:
        raise ValueError(f"AMQP endpoint missing inbox query parameter: {resolved_url}")

    host = parsed.hostname
    port = parsed.port or (5671 if parsed.scheme == "amqps" else 5672)
    path = parsed.path or "/"
    vhost = unquote(path[1:]) if path.startswith("/") else unquote(path)
    if not vhost:
        vhost = "/"

    return AMQPEndpoint(
        url=resolved_url,
        host=host,
        port=port,
        vhost=vhost,
        inbox=inbox,
    )


def get_endpoint_url(
    acs_data: Optional[Dict[str, Any]],
    transport: str,
    *,
    aic: Optional[str] = None,
) -> Optional[str]:
    if not isinstance(acs_data, dict):
        return None

    endpoints = acs_data.get("endPoints")
    if not isinstance(endpoints, list):
        return None

    target_transport = transport.upper()
    for endpoint in endpoints:
        if not isinstance(endpoint, dict):
            continue
        if str(endpoint.get("transport", "")).upper() != target_transport:
            continue
        raw_url = endpoint.get("url")
        if isinstance(raw_url, str) and raw_url.strip():
            return replace_aic_placeholder(raw_url.strip(), aic)
    return None


def replace_aic_placeholder(url: str, aic: Optional[str]) -> str:
    if "{AIC}" not in url:
        return url
    if not aic:
        raise ValueError("AMQP endpoint contains {AIC} placeholder but AIC is missing")
    return url.replace("{AIC}", ensure_valid_aic(aic))


def create_invitation_token() -> str:
    return secrets.token_urlsafe(32)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def calculate_invitation_expiry(timeout_seconds: int) -> str:
    return (utc_now() + timedelta(seconds=timeout_seconds)).isoformat()


def parse_utc_datetime(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def invitation_is_expired(
    expires_at: str, *, skew_seconds: int = INVITATION_CLOCK_SKEW_SECONDS
) -> bool:
    expiry = parse_utc_datetime(expires_at)
    return utc_now() >= expiry + timedelta(seconds=skew_seconds)


def _encode_vhost(vhost: str) -> str:
    normalized = vhost.strip() or "/"
    if normalized == "/":
        return "%2F"
    if normalized.startswith("/"):
        normalized = normalized[1:]
    return quote(normalized, safe="")
