"""Synthetic provider contract: HMAC-SHA256(timestamp + '.' + exact request bytes)."""

import hashlib
import hmac
import os
import time
from collections.abc import Callable

from fastapi import FastAPI, HTTPException, Request
from pydantic import ValidationError
from starlette.concurrency import run_in_threadpool

from webhook_lab.service import Conflict, PaymentEvent, UnknownOrder, accept

MAX_BODY = 16_384
TOLERANCE_SECONDS = 300


def sign(secret: bytes, body: bytes, timestamp: int) -> str:
    digest = hmac.new(secret, str(timestamp).encode() + b"." + body, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={digest}"


def verify(secret: bytes, body: bytes, header: str, now: int) -> bool:
    try:
        # A deliberately narrow provider contract; duplicate fields are rejected.
        timestamp, signature = header.split(",")
        if not timestamp.startswith("t=") or not signature.startswith("v1="):
            return False
        value = int(timestamp[2:])
        if timestamp != f"t={value}" or abs(now - value) > TOLERANCE_SECONDS:
            return False
        return hmac.compare_digest(
            sign(secret, body, value).encode("ascii"), header.encode("ascii")
        )
    except (ValueError, OverflowError, UnicodeEncodeError):
        return False


def create_app(dsn: str, secret: bytes, clock: Callable[[], float] = time.time) -> FastAPI:
    if len(secret) < 32:
        raise ValueError("Use a webhook signing key of at least 32 bytes")
    app = FastAPI(title="Reliable Webhook Lab", version="0.1.0")

    @app.post("/webhooks/payments", status_code=202)
    async def webhook(request: Request):
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > MAX_BODY:
                raise HTTPException(413, "Request body too large")
        raw = bytes(body)
        header = request.headers.get("x-lab-signature", "")
        if not verify(secret, raw, header, int(clock())):
            raise HTTPException(401, "Invalid or expired signature")
        try:
            event = PaymentEvent.model_validate_json(raw)
        except ValidationError:
            raise HTTPException(422, "Invalid payment event") from None
        try:
            outcome = await run_in_threadpool(accept, dsn, event, raw)
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from None
        except UnknownOrder:
            raise HTTPException(409, "Order not available; retry later") from None
        return {"outcome": outcome}

    return app


def from_env() -> FastAPI:
    return create_app(os.environ["DATABASE_URL"], os.environ["WEBHOOK_SECRET"].encode())
