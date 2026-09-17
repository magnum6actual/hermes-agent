"""Verify signed, author-correct voice transcript projection posts.

The wire format is narrowly adapted from ``hermes-deployment`` commit
``6d4ceface467ac1403d540ee9639e9d367c3fec3``. This module performs no
publication and owns no memory; it only lets the Mattermost adapter distinguish
an authenticated Wayne voice display copy from a new Wayne input.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path
import stat
from typing import Any

PROP = "hermes_origin"


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _encoded(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


def load_projection_config(path_value: Any) -> dict[str, str] | None:
    path_text = str(path_value or "").strip()
    if not path_text:
        return None
    path = Path(path_text)
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or stat.S_IMODE(info.st_mode) & 0o077:
        raise ValueError("private Mattermost voice projection configuration required")
    document = json.loads(path.read_text())
    required = {"profile", "channel_id", "user_id", "bot_id", "origin_key_file"}
    if set(document) != required or any(not str(document[key]).strip() for key in required):
        raise ValueError("invalid Mattermost voice projection configuration")
    key_path = Path(document.pop("origin_key_file"))
    key_info = key_path.lstat()
    if stat.S_ISLNK(key_info.st_mode) or stat.S_IMODE(key_info.st_mode) & 0o077:
        raise ValueError("private Mattermost voice projection key required")
    origin_key = key_path.read_text().strip()
    if not origin_key:
        raise ValueError("empty Mattermost voice projection key")
    return {**{key: str(value) for key, value in document.items()}, "origin_key": origin_key}


def sign_origin_for_test(
    key: str,
    *,
    profile: str,
    channel_id: str,
    user_id: str,
    record: dict,
    part: int,
    parts: int,
    content: str,
    operation_id: str,
) -> dict:
    """Wire-compatible signer used by focused cross-repository contract tests."""
    data = {
        "profile": profile,
        "channel_id": channel_id,
        "user_id": user_id,
        "record_id": record["id"],
        "source": record["source"],
        "source_id": record["source_id"],
        "role": record["role"],
        "version": record["version"],
        "part": part,
        "parts": parts,
        "content_hash": _digest(content),
        "operation_id": operation_id,
        "retired": False,
    }
    return {
        **data,
        "signature": hmac.new(key.encode(), _encoded(data), hashlib.sha256).hexdigest(),
    }


def valid_user_voice_projection(post: dict, config: dict[str, str] | None) -> bool:
    """True only for a signed Wayne-authored voice display copy in the fixed DM."""
    if config is None or post.get("original_id") or post.get("type"):
        return False
    props = post.get("props")
    marker = props.get(PROP) if isinstance(props, dict) else None
    if not isinstance(marker, dict):
        return False
    signature = marker.get("signature")
    data = {key: value for key, value in marker.items() if key != "signature"}
    if not isinstance(signature, str) or len(_encoded(data)) > 4096:
        return False
    expected = hmac.new(
        config["origin_key"].encode(), _encoded(data), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(expected, signature):
        return False
    source_id = str(data.get("source_id") or "")
    return bool(
        data.get("profile") == config["profile"]
        and data.get("channel_id") == config["channel_id"]
        and data.get("user_id") == config["user_id"]
        and data.get("role") == "user"
        and data.get("source") == "voice"
        and source_id.startswith("voice:")
        and data.get("record_id") == source_id
        and data.get("version") == 1
        and data.get("retired") is False
        and isinstance(data.get("part"), int)
        and isinstance(data.get("parts"), int)
        and 0 <= data["part"] < data["parts"]
        and post.get("user_id") == config["user_id"]
        and post.get("channel_id") == config["channel_id"]
        and data.get("content_hash") == _digest(str(post.get("message") or ""))
    )

