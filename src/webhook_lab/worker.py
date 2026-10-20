"""At-least-once delivery with bounded retries, leases and fenced acknowledgements."""

import uuid
from collections.abc import Callable
from datetime import datetime, timedelta

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

MAX_ATTEMPTS = 5
LEASE_SECONDS = 30


def claim(dsn: str, now: datetime) -> dict | None:
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        # An exhausted last attempt can crash before recording its failure.
        conn.execute(
            """UPDATE outbox SET dead_at = %s, last_error = 'LeaseExpired'
               WHERE attempts >= %s AND delivered_at IS NULL AND dead_at IS NULL
                 AND lease_until <= %s""",
            (now, MAX_ATTEMPTS, now),
        )
        return conn.execute(
            """WITH candidate AS (
                 SELECT id FROM outbox
                 WHERE delivered_at IS NULL AND dead_at IS NULL AND attempts < %s
                   AND available_at <= %s AND (lease_until IS NULL OR lease_until <= %s)
                 ORDER BY available_at, id FOR UPDATE SKIP LOCKED LIMIT 1
               )
               UPDATE outbox o SET attempts = attempts + 1, lease_until = %s, claim_token = %s
               FROM candidate c WHERE o.id = c.id
               RETURNING o.*""",
            (MAX_ATTEMPTS, now, now, now + timedelta(seconds=LEASE_SECONDS), uuid.uuid4()),
        ).fetchone()


def finish(dsn: str, job: dict, now: datetime, error: Exception | None = None) -> bool:
    with psycopg.connect(dsn) as conn:
        if error is None:
            row = conn.execute(
                """UPDATE outbox SET delivered_at = %s, lease_until = NULL, last_error = NULL
                   WHERE id = %s AND claim_token = %s AND delivered_at IS NULL
                     AND dead_at IS NULL AND lease_until > %s RETURNING id""",
                (now, job["id"], job["claim_token"], now),
            ).fetchone()
        else:
            row = conn.execute(
                """UPDATE outbox SET last_error = %s, available_at = %s, lease_until = NULL,
                     dead_at = CASE WHEN attempts >= %s THEN %s ELSE NULL END
                   WHERE id = %s AND claim_token = %s AND delivered_at IS NULL
                     AND dead_at IS NULL AND lease_until > %s RETURNING id""",
                (
                    type(error).__name__,  # Don't persist potentially sensitive exception text.
                    now + timedelta(seconds=min(2 ** job["attempts"], 300)),
                    MAX_ATTEMPTS,
                    now,
                    job["id"],
                    job["claim_token"],
                    now,
                ),
            ).fetchone()
        return row is not None


def process_one(dsn: str, sink: Callable[[dict, str], None], clock: Callable[[], datetime]) -> str:
    job = claim(dsn, clock())
    if not job:
        return "idle"
    error = None
    try:
        sink(job["payload"], job["operation_id"])
    except Exception as exc:
        error = exc
    acknowledged = finish(dsn, job, clock(), error)
    if not acknowledged:
        return "lease_lost"
    return "delivered" if error is None else "failed"


def demo_sink(dsn: str) -> Callable[[dict, str], None]:
    def deliver(payload: dict, key: str) -> None:
        with psycopg.connect(dsn) as conn:
            conn.execute(
                """INSERT INTO demo_deliveries VALUES (%s, %s)
                   ON CONFLICT (operation_id) DO NOTHING""",
                (key, Jsonb(payload)),
            )

    return deliver
