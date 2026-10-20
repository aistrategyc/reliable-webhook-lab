"""Persist the provider event, business transition and delivery intent atomically."""

import hashlib
import uuid
from typing import Literal

import psycopg
from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, Field


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    event_id: str = Field(min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9_-]+$")
    operation_id: str = Field(min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9_-]+$")
    order_id: str = Field(min_length=1, max_length=100)
    type: Literal["payment.succeeded"]
    amount_minor: int = Field(gt=0, le=10**12)
    currency: str = Field(pattern=r"^[A-Z]{3}$")


class Conflict(Exception):
    """An authenticated event contradicts an existing business record."""


class UnknownOrder(Exception):
    """The provider must retry after the order has been created."""


def accept(dsn: str, event: PaymentEvent, raw_body: bytes) -> str:
    digest = hashlib.sha256(raw_body).hexdigest()
    with psycopg.connect(dsn) as conn:
        inserted = conn.execute(
            """INSERT INTO inbox(event_id, body_sha256, outcome)
               VALUES (%s, %s, 'applied') ON CONFLICT DO NOTHING RETURNING event_id""",
            (event.event_id, digest),
        ).fetchone()
        if not inserted:
            prior = conn.execute(
                "SELECT body_sha256 FROM inbox WHERE event_id = %s", (event.event_id,)
            ).fetchone()
            if prior[0] != digest:
                raise Conflict("Event ID was reused with a different body")
            return "duplicate_event"

        order = conn.execute(
            "SELECT amount_minor, currency, status FROM orders WHERE id = %s FOR UPDATE",
            (event.order_id,),
        ).fetchone()
        if not order:
            raise UnknownOrder(event.order_id)
        if (event.amount_minor, event.currency) != order[:2]:
            raise Conflict("Payment amount or currency does not match the order")

        payment = conn.execute(
            """INSERT INTO payments(operation_id, order_id, amount_minor, currency)
               VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING operation_id""",
            (event.operation_id, event.order_id, event.amount_minor, event.currency),
        ).fetchone()
        if not payment:
            prior = conn.execute(
                "SELECT order_id, amount_minor, currency FROM payments WHERE operation_id = %s",
                (event.operation_id,),
            ).fetchone()
            if prior != (event.order_id, event.amount_minor, event.currency):
                raise Conflict("Order already paid or operation ID reused")
            conn.execute(
                "UPDATE inbox SET outcome = 'duplicate_operation' WHERE event_id = %s",
                (event.event_id,),
            )
            return "duplicate_operation"

        conn.execute("UPDATE orders SET status = 'paid' WHERE id = %s", (event.order_id,))
        conn.execute(
            "INSERT INTO outbox(id, operation_id, payload) VALUES (%s, %s, %s)",
            (
                uuid.uuid4(),
                event.operation_id,
                Jsonb({"order_id": event.order_id, "operation_id": event.operation_id}),
            ),
        )
    return "applied"
