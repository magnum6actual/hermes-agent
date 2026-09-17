"""Private submit-only attachment for the deployed Mattermost voice worker.

The Unix socket is deliberately narrower than a session API: one configured
Mattermost DM session key, one ``submit`` operation, and one final response.
The gateway still owns authorization, FIFO admission, tools, approvals, session
rotation, persistence, and every non-final status message.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import dataclasses
import json
import logging
import os
import stat
import uuid
from pathlib import Path
from typing import Any

from gateway.platforms.event import MessageEvent, MessageType
from gateway.session import Platform

logger = logging.getLogger(__name__)

CAPTURE_ORIGIN = "voice_delegate"
MAX_REQUEST_BYTES = 64 * 1024
DEFAULT_RESULT_TIMEOUT_SECONDS = 1800.0

_capture_origin: contextvars.ContextVar[str] = contextvars.ContextVar(
    "hermes_voice_capture_origin", default=""
)


def current_capture_origin() -> str:
    """Immutable per-turn capture provenance visible to lifecycle hooks."""
    return _capture_origin.get()


@contextlib.contextmanager
def capture_scope(event: MessageEvent):
    """Bind voice provenance only for a server-created, internal voice event."""
    metadata = event.metadata if isinstance(event.metadata, dict) else {}
    origin = (
        CAPTURE_ORIGIN
        if event.internal
        and metadata.get("capture_origin") == CAPTURE_ORIGIN
        and metadata.get("voice_submission_id")
        else ""
    )
    token = _capture_origin.set(origin)
    try:
        yield origin
    finally:
        _capture_origin.reset(token)


def is_voice_delegate_event(event: MessageEvent) -> bool:
    metadata = event.metadata if isinstance(event.metadata, dict) else {}
    return bool(
        event.internal
        and metadata.get("capture_origin") == CAPTURE_ORIGIN
        and metadata.get("voice_submission_id")
        and getattr(event, "_voice_submission_id", None)
        == metadata.get("voice_submission_id")
    )


def capture_final_response(event: MessageEvent, response: Any) -> bool:
    """Resolve a voice request and suppress only that internal event's final body."""
    if not is_voice_delegate_event(event):
        return False
    future = getattr(event, "_voice_response_future", None)
    if not isinstance(future, asyncio.Future):
        return False
    if not future.done():
        future.set_result({"response": str(response or "")})
    return True


def fail_response(event: MessageEvent, error: BaseException) -> None:
    if not is_voice_delegate_event(event):
        return
    future = getattr(event, "_voice_response_future", None)
    if isinstance(future, asyncio.Future) and not future.done():
        future.set_exception(RuntimeError(f"native voice turn failed: {type(error).__name__}"))


def _config(runner: Any) -> dict[str, Any] | None:
    section = runner._gateway_cfg_section("voice_submission")
    if not section or section.get("enabled") is not True:
        return None
    socket_path = str(section.get("socket_path") or "").strip()
    session_key = str(section.get("session_key") or "").strip()
    if not socket_path or not session_key:
        raise RuntimeError(
            "gateway.voice_submission requires absolute socket_path and session_key"
        )
    path = Path(socket_path)
    if not path.is_absolute():
        raise RuntimeError("gateway.voice_submission.socket_path must be absolute")
    return {
        "socket_path": path,
        "session_key": session_key,
        "result_timeout_seconds": max(
            1.0,
            float(section.get("result_timeout_seconds") or DEFAULT_RESULT_TIMEOUT_SECONDS),
        ),
    }


class VoiceSubmissionServer:
    def __init__(self, runner: Any, config: dict[str, Any]) -> None:
        self.runner = runner
        self.socket_path: Path = config["socket_path"]
        self.session_key: str = config["session_key"]
        self.result_timeout_seconds: float = config["result_timeout_seconds"]
        self.server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self.socket_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.socket_path.parent, 0o700)
        if self.socket_path.exists() or self.socket_path.is_socket():
            mode = self.socket_path.lstat().st_mode
            if not stat.S_ISSOCK(mode) or self.socket_path.lstat().st_uid != os.getuid():
                raise RuntimeError(
                    f"refusing non-owned/non-socket voice endpoint: {self.socket_path}"
                )
            self.socket_path.unlink()
        self.server = await asyncio.start_unix_server(
            self._handle_connection, path=str(self.socket_path)
        )
        os.chmod(self.socket_path, 0o600)
        logger.info("Voice submission socket ready at %s", self.socket_path)

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        if self.socket_path.exists() and stat.S_ISSOCK(self.socket_path.lstat().st_mode):
            self.socket_path.unlink()

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        response: dict[str, Any]
        try:
            raw = await asyncio.wait_for(reader.readline(), timeout=10.0)
            if not raw or len(raw) > MAX_REQUEST_BYTES or not raw.endswith(b"\n"):
                raise ValueError("request must be one newline-terminated JSON object")
            payload = json.loads(raw)
            response = {"ok": True, **(await self.submit(payload))}
        except Exception as exc:
            response = {"ok": False, "error": str(exc)[:240]}
        writer.write((json.dumps(response, separators=(",", ":")) + "\n").encode())
        with contextlib.suppress(Exception):
            await writer.drain()
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()

    async def submit(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict) or payload.get("method") != "submit":
            raise ValueError("unsupported method")
        if set(payload) - {
            "method",
            "session_key",
            "text",
            "submission_id",
            "delegation_item_id",
            "call_id",
            "realtime_turn_id",
        }:
            raise ValueError("unsupported submit field")
        requested_key = str(payload.get("session_key") or "")
        if requested_key != self.session_key:
            raise PermissionError("voice submission session is not configured")
        text = str(payload.get("text") or "").strip()
        if not text or len(text) > 32_000:
            raise ValueError("text must contain 1..32000 characters")
        submission_id = str(payload.get("submission_id") or uuid.uuid4())
        if len(submission_id) > 160:
            raise ValueError("submission_id is too long")

        entry = await self.runner.async_session_store.lookup_by_session_key(self.session_key)
        if entry is None or entry.origin is None:
            raise RuntimeError("configured Mattermost DM session is unavailable")
        source = dataclasses.replace(entry.origin)
        if (
            source.platform != Platform.MATTERMOST
            or source.chat_type != "dm"
            or source.thread_id is not None
        ):
            raise PermissionError("configured voice session is not a flat Mattermost DM")
        if not self.runner._is_user_authorized_for_source(
            source, allow_adapter_delegation=False
        ):
            raise PermissionError("configured Mattermost DM is no longer authorized")
        adapter = self.runner._adapter_for_source(source)
        if adapter is None:
            raise RuntimeError("Mattermost adapter is unavailable")

        metadata = {
            "capture_origin": CAPTURE_ORIGIN,
            "voice_submission_id": submission_id,
            "voice_delegation_item_id": str(payload.get("delegation_item_id") or ""),
            "voice_call_id": str(payload.get("call_id") or ""),
            "voice_realtime_turn_id": str(payload.get("realtime_turn_id") or ""),
            "gateway_session_key": self.session_key,
            "gateway_session_id": entry.session_id,
            "gateway_session_strict": True,
        }
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            internal=True,
            allow_gateway_control=False,
            metadata=metadata,
        )
        event._voice_submission_id = submission_id
        event._voice_response_future = asyncio.get_running_loop().create_future()
        await adapter.handle_message(event)
        if event._gateway_accepted is not True:
            raise RuntimeError("native Mattermost queue did not accept the voice turn")
        result = await asyncio.wait_for(
            asyncio.shield(event._voice_response_future),
            timeout=self.result_timeout_seconds,
        )
        current = await self.runner.async_session_store.lookup_by_session_key(self.session_key)
        return {
            "submission_id": submission_id,
            "native_session_id": current.session_id if current is not None else entry.session_id,
            "response": result["response"],
        }


async def start_for_runner(runner: Any) -> VoiceSubmissionServer | None:
    config = _config(runner)
    if config is None:
        return None
    server = VoiceSubmissionServer(runner, config)
    await server.start()
    return server


async def stop_for_runner(runner: Any) -> None:
    server = getattr(runner, "_voice_submission_server", None)
    if server is not None:
        await server.stop()
        runner._voice_submission_server = None
