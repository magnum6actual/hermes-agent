import asyncio
import stat
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType
from gateway.session import SessionSource, build_session_key
from gateway.voice_submission import (
    CAPTURE_ORIGIN,
    VoiceSubmissionServer,
    capture_final_response,
)


class _Adapter(BasePlatformAdapter):
    def __init__(self):
        super().__init__(PlatformConfig(enabled=True, token="test"), Platform.MATTERMOST)
        self.sent = []

    async def connect(self, *, is_reconnect=False):
        return True

    async def disconnect(self):
        return None

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.sent.append(content)
        return SendResult(success=True, message_id="sent-1")

    async def send_typing(self, chat_id, metadata=None):
        return None

    async def stop_typing(self, chat_id, metadata=None):
        return None

    async def get_chat_info(self, chat_id):
        return {"id": chat_id}


def _source():
    return SessionSource(
        platform=Platform.MATTERMOST,
        chat_id="dm-channel",
        chat_type="dm",
        user_id="wayne",
        user_name="Wayne",
    )


def _voice_event(source=None):
    event = MessageEvent(
        text="Do the harmless lookup.",
        message_type=MessageType.TEXT,
        source=source or _source(),
        internal=True,
        allow_gateway_control=False,
        metadata={
            "capture_origin": CAPTURE_ORIGIN,
            "voice_submission_id": "voice-1",
        },
    )
    event._voice_submission_id = "voice-1"
    event._voice_response_future = asyncio.get_running_loop().create_future()
    return event


@pytest.mark.asyncio
async def test_internal_voice_final_is_returned_and_not_sent():
    adapter = _Adapter()
    adapter.set_message_handler(lambda _event: asyncio.sleep(0, result="native result"))
    event = _voice_event()
    session_key = build_session_key(event.source)
    adapter._active_sessions[session_key] = asyncio.Event()

    await adapter._process_message_background(event, session_key)

    assert (await event._voice_response_future)["response"] == "native result"
    assert adapter.sent == []


@pytest.mark.asyncio
async def test_non_voice_internal_event_keeps_normal_delivery():
    adapter = _Adapter()
    adapter.set_message_handler(lambda _event: asyncio.sleep(0, result="normal result"))
    event = MessageEvent(
        text="ordinary internal event",
        source=_source(),
        internal=True,
        allow_gateway_control=False,
    )
    session_key = build_session_key(event.source)
    adapter._active_sessions[session_key] = asyncio.Event()

    await adapter._process_message_background(event, session_key)

    assert adapter.sent == ["normal result"]


@pytest.mark.asyncio
async def test_submit_reuses_configured_current_dm_session_and_returns_rotated_session():
    source = _source()
    session_key = build_session_key(source)
    entries = [
        SimpleNamespace(session_id="native-before", origin=source),
        SimpleNamespace(session_id="native-after", origin=source),
    ]

    class Store:
        async def lookup_by_session_key(self, requested):
            assert requested == session_key
            return entries.pop(0)

    adapter = _Adapter()
    seen = []

    async def handle(event):
        seen.append(event)
        event._gateway_accepted = True
        assert capture_final_response(event, "same-session result") is True

    adapter.handle_message = handle
    runner = SimpleNamespace(
        async_session_store=Store(),
        _is_user_authorized_for_source=lambda *_args, **_kwargs: True,
        _adapter_for_source=lambda _source: adapter,
    )
    server = VoiceSubmissionServer(
        runner,
        {
            "socket_path": None,
            "session_key": session_key,
            "result_timeout_seconds": 1,
        },
    )

    result = await server.submit(
        {
            "method": "submit",
            "session_key": session_key,
            "text": "Do the harmless lookup.",
            "submission_id": "voice-1",
            "delegation_item_id": "delegation-1",
        }
    )

    assert seen[0].source == source
    assert seen[0].metadata["gateway_session_id"] == "native-before"
    assert seen[0].metadata["capture_origin"] == CAPTURE_ORIGIN
    assert result == {
        "submission_id": "voice-1",
        "native_session_id": "native-after",
        "response": "same-session result",
    }


@pytest.mark.asyncio
async def test_unix_socket_is_owner_only_and_submit_only():
    runner = SimpleNamespace()
    with tempfile.TemporaryDirectory(prefix="voice-submit-", dir="/tmp") as temp_dir:
        socket_path = Path(temp_dir) / "submit.sock"
        server = VoiceSubmissionServer(
            runner,
            {
                "socket_path": socket_path,
                "session_key": "configured-session",
                "result_timeout_seconds": 1,
            },
        )
        await server.start()
        try:
            assert stat.S_IMODE(socket_path.stat().st_mode) == 0o600
            reader, writer = await asyncio.open_unix_connection(str(socket_path))
            writer.write(b'{"method":"list"}\n')
            await writer.drain()
            response = await reader.readline()
            assert b'"ok":false' in response
            assert b"unsupported method" in response
            writer.close()
            await writer.wait_closed()
        finally:
            await server.stop()
