import json
from unittest.mock import AsyncMock

import pytest

from gateway.config import PlatformConfig
from gateway.mattermost_voice_projection import PROP, sign_origin_for_test
from plugins.platforms.mattermost.adapter import MattermostAdapter


def _adapter(tmp_path):
    key = tmp_path / "voice-origin.key"
    key.write_text("synthetic-origin-key\n")
    key.chmod(0o600)
    config_path = tmp_path / "voice-projection.json"
    config_path.write_text(
        json.dumps(
            {
                "profile": "main",
                "channel_id": "wayne-hermes-dm",
                "user_id": "wayne",
                "bot_id": "hermes",
                "origin_key_file": str(key),
            }
        )
    )
    config_path.chmod(0o600)
    adapter = MattermostAdapter(
        PlatformConfig(
            enabled=True,
            token="test",
            extra={
                "url": "https://mattermost.invalid",
                "voice_projection_config": str(config_path),
            },
        )
    )
    adapter._bot_user_id = "hermes"
    adapter.handle_message = AsyncMock()
    return adapter


def _event(message="Public spoken wording."):
    record = {
        "id": "voice:call-1:item-1:user",
        "source": "voice",
        "source_id": "voice:call-1:item-1:user",
        "role": "user",
        "version": 1,
    }
    marker = sign_origin_for_test(
        "synthetic-origin-key",
        profile="main",
        channel_id="wayne-hermes-dm",
        user_id="wayne",
        record=record,
        part=0,
        parts=1,
        content=message,
        operation_id="operation-1",
    )
    return {
        "event": "posted",
        "data": {
            "channel_type": "D",
            "sender_name": "wayne",
            "post": json.dumps(
                {
                    "id": "post-1",
                    "channel_id": "wayne-hermes-dm",
                    "user_id": "wayne",
                    "message": message,
                    "props": {PROP: marker},
                }
            ),
        },
    }


@pytest.mark.asyncio
async def test_valid_signed_wayne_voice_projection_is_egress_only(tmp_path):
    adapter = _adapter(tmp_path)

    await adapter._handle_ws_event(_event())

    adapter.handle_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_tampered_wayne_projection_remains_ordinary_input(tmp_path):
    adapter = _adapter(tmp_path)
    event = _event()
    post = json.loads(event["data"]["post"])
    post["message"] = "Tampered new prompt."
    event["data"]["post"] = json.dumps(post)

    await adapter._handle_ws_event(event)

    adapter.handle_message.assert_awaited_once()
    admitted = adapter.handle_message.await_args.args[0]
    assert admitted.text == "Tampered new prompt."
    assert admitted.source.user_id == "wayne"

