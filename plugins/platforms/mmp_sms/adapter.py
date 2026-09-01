"""MMPlus SMS platform — LAN HTTP ingest, LLM parse, WhatsApp delivery."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Optional

from aiohttp import web

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

from .processor import MmpSmsWebhookProcessor, notify_confirmation_result

logger = logging.getLogger(__name__)


class MmpSmsAdapter(BasePlatformAdapter):
    """HTTP POST /webhook for Termux → Hermes LLM. Replies go to WhatsApp."""

    interactive_resume = False

    def __init__(self, config: PlatformConfig, **kwargs: Any):
        super().__init__(config=config, platform=Platform("mmp_sms"))
        extra = getattr(config, "extra", {}) or {}
        self._host: str = str(extra.get("host") or "0.0.0.0")
        self._port: int = int(extra.get("port") or 8001)
        self._max_body_bytes: int = int(extra.get("max_body_bytes") or 1_000_000)
        self._processor = MmpSmsWebhookProcessor(extra)
        self._batch: list[dict[str, Any]] = []
        self._flush_task: Optional[asyncio.Task] = None
        self._runner = None
        self._background_tasks: set[asyncio.Task] = set()

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        app = web.Application(client_max_size=self._max_body_bytes)
        app.router.add_get("/health", self._handle_health)
        app.router.add_post("/webhook", self._handle_post)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        try:
            await site.start()
        except OSError as exc:
            await self._runner.cleanup()
            self._runner = None
            logger.error("[mmp-sms] Could not bind %s:%d: %s", self._host, self._port, exc)
            return False
        self._mark_connected()
        logger.info("[mmp-sms] Listening on %s:%d/webhook", self._host, self._port)
        return True

    async def disconnect(self) -> None:
        if self._flush_task is not None:
            self._flush_task.cancel()
            self._flush_task = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        self._mark_disconnected()
        logger.info("[mmp-sms] Disconnected")

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        runner = self.gateway_runner
        if runner is None:
            logger.warning("[mmp-sms] no gateway runner; dropping reply")
            return SendResult(success=False, error="gateway runner missing")
        wa = runner.adapters.get(Platform.WHATSAPP)
        target = self._processor.chat_id or chat_id
        if wa is None or not target:
            logger.info("[mmp-sms] WhatsApp unavailable; logging reply: %s", content[:200])
            return SendResult(success=True)
        return await wa.send(target, content, reply_to=reply_to, metadata=metadata)

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "platform": "mmp_sms"})

    async def _handle_post(self, request: web.Request) -> web.Response:
        remote = request.remote or ""
        if not self._processor.accepts_ip(remote):
            logger.warning("[mmp-sms] rejected source IP %s", remote)
            return web.json_response({"error": "Source IP not allowed"}, status=403)
        if (request.content_length or 0) > self._max_body_bytes:
            return web.json_response({"error": "Payload too large"}, status=413)
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        async def _process() -> None:
            try:
                raw = await asyncio.to_thread(self._processor.ingest_raw, payload)
                if raw.get("status") != "queued_for_agent":
                    logger.info(
                        "[mmp-sms] skip dispatch id=%s status=%s",
                        raw.get("id"),
                        raw.get("status"),
                    )
                    return
                self._batch.append(raw)
                if self._flush_task is not None:
                    self._flush_task.cancel()
                task = asyncio.create_task(self._flush_batch())
                self._flush_task = task
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
            except Exception:
                logger.exception("[mmp-sms] ingest failed")

        task = asyncio.create_task(_process())
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return web.json_response({"ok": True}, status=200)

    async def _flush_batch(self) -> None:
        try:
            await asyncio.sleep(8)
        except asyncio.CancelledError:
            return
        items = list(self._batch)
        self._batch = []
        self._flush_task = None
        if not items:
            return
        try:
            await self._dispatch(items)
        except Exception:
            logger.exception("[mmp-sms] LLM dispatch failed ids=%s", [i.get("id") for i in items])

    async def _dispatch(self, items: list[dict[str, Any]]) -> None:
        prompt = self._processor.llm_prompt(items)
        delivery_id = items[0]["id"] if len(items) == 1 else f"batch-{items[-1]['id']}"
        source = self.build_source(
            chat_id=f"mmp-sms:{delivery_id}",
            chat_name="mmp-sms",
            chat_type="webhook",
            user_id="mmp-sms",
            user_name="mmp-sms",
        )
        event = MessageEvent(
            text=prompt,
            message_type=MessageType.TEXT,
            source=source,
            raw_message={"items": items},
            message_id=delivery_id,
        )
        logger.info("[mmp-sms] dispatching %d SMS to LLM delivery=%s", len(items), delivery_id)
        await self.handle_message(event)
        await asyncio.to_thread(self._processor.mark_agent_dispatched, [i["id"] for i in items])


def _on_gateway_dispatch(event, gateway, **kwargs):
    text = getattr(event, "text", None) or ""
    adapter = None
    if gateway is not None:
        try:
            adapter = gateway.adapters.get(Platform("mmp_sms"))
        except Exception:
            adapter = None
    processor = getattr(adapter, "_processor", None)
    if processor is None:
        return None
    control = processor.classify_control(text)
    if control is None:
        return None
    action, candidate_id = control
    runner = gateway
    chat_id = getattr(getattr(event, "source", None), "chat_id", "") or processor.chat_id
    task = asyncio.create_task(
        notify_confirmation_result(processor, action, candidate_id, runner, chat_id)
    )
    task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
    logger.info("[mmp-sms] consumed WhatsApp control %s %s", action, candidate_id)
    return {"action": "skip", "reason": "mmp-sms-control"}


def register(ctx):
    ctx.register_platform(
        name="mmp_sms",
        label="MMPlus SMS",
        adapter_factory=lambda cfg: MmpSmsAdapter(cfg),
        check_fn=lambda: True,
        emoji="📩",
        platform_hint=(
            "You are handling MoneyManagerPlus bank SMS. Parse the raw text "
            "yourself; do not assume a Python bank parser exists."
        ),
    )
    ctx.register_hook("pre_gateway_dispatch", _on_gateway_dispatch)
