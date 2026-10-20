import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from fastapi.testclient import TestClient

from webhook_lab.api import create_app, sign, verify
from webhook_lab.service import Conflict, PaymentEvent, UnknownOrder, accept
from webhook_lab.worker import MAX_ATTEMPTS, claim, demo_sink, finish, process_one

SECRET = b"synthetic-test-key-at-least-32-bytes"
NOW = 1_800_000_000


def test_non_ascii_signature_is_rejected_without_error():
    assert not verify(SECRET, b"{}", f"t={NOW},v1=é", NOW)


@pytest.fixture
def event(dsn):
    with psycopg.connect(dsn) as conn:
        conn.execute("INSERT INTO orders(id, amount_minor, currency) VALUES ('o1', 4900, 'PLN')")
    return PaymentEvent(
        event_id="e1",
        operation_id="p1",
        order_id="o1",
        type="payment.succeeded",
        amount_minor=4900,
        currency="PLN",
    )


def submit(dsn, event):
    return accept(dsn, event, event.model_dump_json().encode())


def counts(dsn):
    with psycopg.connect(dsn) as conn:
        return tuple(
            conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("inbox", "payments", "outbox")
        )


def test_signed_request_and_duplicate_are_acknowledged(dsn, event):
    body = event.model_dump_json().encode()
    with TestClient(create_app(dsn, SECRET, lambda: NOW)) as client:
        outcomes = [
            client.post(
                "/webhooks/payments",
                content=body,
                headers={"x-lab-signature": sign(SECRET, body, NOW)},
            )
            for _ in range(2)
        ]
    assert [r.status_code for r in outcomes] == [202, 202]
    assert [r.json()["outcome"] for r in outcomes] == ["applied", "duplicate_event"]
    assert counts(dsn) == (1, 1, 1)


@pytest.mark.parametrize("offset", [-301, 301])
def test_old_and_future_signatures_are_rejected(dsn, event, offset):
    body = event.model_dump_json().encode()
    with TestClient(create_app(dsn, SECRET, lambda: NOW)) as client:
        r = client.post(
            "/webhooks/payments",
            content=body,
            headers={"x-lab-signature": sign(SECRET, body, NOW + offset)},
        )
    assert r.status_code == 401
    assert counts(dsn) == (0, 0, 0)


@pytest.mark.parametrize("header", ["", "t=no,v1=x", "t=1800000000,v1=x", "t=1,v1=x,t=2"])
def test_malformed_signatures_are_rejected(dsn, event, header):
    with TestClient(create_app(dsn, SECRET, lambda: NOW)) as client:
        assert (
            client.post(
                "/webhooks/payments",
                content=event.model_dump_json(),
                headers={"x-lab-signature": header},
            ).status_code
            == 401
        )


def test_tampering_invalidates_signature(dsn, event):
    body = event.model_dump_json().encode()
    with TestClient(create_app(dsn, SECRET, lambda: NOW)) as client:
        r = client.post(
            "/webhooks/payments",
            content=body + b" ",
            headers={"x-lab-signature": sign(SECRET, body, NOW)},
        )
    assert r.status_code == 401


def test_body_limit_and_strict_contract(dsn, event):
    with TestClient(create_app(dsn, SECRET, lambda: NOW)) as client:
        assert client.post("/webhooks/payments", content=b"x" * 16385).status_code == 413
        payload = event.model_dump()
        payload["amount_minor"] = "4900"
        body = json.dumps(payload).encode()
        assert (
            client.post(
                "/webhooks/payments",
                content=body,
                headers={"x-lab-signature": sign(SECRET, body, NOW)},
            ).status_code
            == 422
        )


@pytest.mark.parametrize("same_event", [True, False])
def test_concurrent_callbacks_apply_business_transition_once(dsn, event, same_event):
    barrier = threading.Barrier(8)

    def callback(i):
        candidate = event if same_event else event.model_copy(update={"event_id": f"e{i}"})
        barrier.wait()
        return submit(dsn, candidate)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(callback, range(8)))
    assert results.count("applied") == 1
    assert counts(dsn) == (1 if same_event else 8, 1, 1)


def test_event_id_cannot_be_reused_with_a_new_body(dsn, event):
    submit(dsn, event)
    with pytest.raises(Conflict, match="different body"):
        submit(dsn, event.model_copy(update={"amount_minor": 5000}))
    assert counts(dsn) == (1, 1, 1)


def test_failed_validation_rolls_back_inbox_and_can_be_retried(dsn, event):
    with pytest.raises(Conflict, match="amount"):
        submit(dsn, event.model_copy(update={"amount_minor": 5000}))
    assert counts(dsn) == (0, 0, 0)
    assert submit(dsn, event) == "applied"


def test_unknown_order_is_not_consumed(dsn, event):
    with pytest.raises(UnknownOrder):
        submit(dsn, event.model_copy(update={"order_id": "missing"}))
    assert counts(dsn) == (0, 0, 0)


def test_operation_cannot_pay_another_order(dsn, event):
    submit(dsn, event)
    with psycopg.connect(dsn) as conn:
        conn.execute("INSERT INTO orders(id, amount_minor, currency) VALUES ('o2', 4900, 'PLN')")
    with pytest.raises(Conflict):
        submit(dsn, event.model_copy(update={"event_id": "e2", "order_id": "o2"}))
    assert counts(dsn) == (1, 1, 1)


def test_distinct_payment_cannot_pay_paid_order(dsn, event):
    submit(dsn, event)
    with pytest.raises(Conflict):
        submit(dsn, event.model_copy(update={"event_id": "e2", "operation_id": "p2"}))
    assert counts(dsn) == (1, 1, 1)


def test_outbox_failure_rolls_back_payment_and_order(dsn, event):
    with psycopg.connect(dsn) as conn:
        conn.execute("ALTER TABLE outbox ADD CONSTRAINT simulate_disk_failure CHECK (false)")
    with pytest.raises(psycopg.errors.CheckViolation):
        submit(dsn, event)
    assert counts(dsn) == (0, 0, 0)
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT status FROM orders").fetchone()[0] == "pending"


def test_unavailable_receiver_retries_without_losing_payment(dsn, event):
    submit(dsn, event)
    now = datetime.now(UTC) + timedelta(seconds=1)

    def offline(payload, key):
        raise ConnectionError("Provider unavailable")

    assert process_one(dsn, offline, lambda: now) == "failed"
    assert process_one(dsn, demo_sink(dsn), lambda: now) == "idle"
    assert process_one(dsn, demo_sink(dsn), lambda: now + timedelta(seconds=3)) == "delivered"
    assert counts(dsn) == (1, 1, 1)


def test_parallel_workers_do_not_claim_same_live_lease(dsn, event):
    submit(dsn, event)
    now = datetime.now(UTC) + timedelta(seconds=1)
    barrier = threading.Barrier(4)

    def take(_):
        barrier.wait()
        return claim(dsn, now)

    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs = list(pool.map(take, range(4)))
    assert len([j for j in jobs if j]) == 1


def test_crash_after_receiver_commit_is_safe_with_idempotent_receiver(dsn, event):
    submit(dsn, event)
    now = datetime.now(UTC) + timedelta(seconds=1)
    first = claim(dsn, now)
    demo_sink(dsn)(first["payload"], first["operation_id"])
    # Worker dies before acknowledging. The next lease repeats delivery.
    second = claim(dsn, now + timedelta(seconds=31))
    assert second["claim_token"] != first["claim_token"]
    assert not finish(dsn, first, now + timedelta(seconds=31))
    demo_sink(dsn)(second["payload"], second["operation_id"])
    assert finish(dsn, second, now + timedelta(seconds=32))
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT count(*) FROM demo_deliveries").fetchone()[0] == 1


def test_retry_budget_ends_in_dead_letter(dsn, event):
    submit(dsn, event)
    now = datetime.now(UTC) + timedelta(seconds=1)
    for _ in range(MAX_ATTEMPTS):
        job = claim(dsn, now)
        assert job
        assert finish(dsn, job, now, ConnectionError("outage"))
        now += timedelta(minutes=10)
    assert claim(dsn, now) is None
    with psycopg.connect(dsn) as conn:
        attempts, dead, delivered = conn.execute(
            "SELECT attempts, dead_at, delivered_at FROM outbox"
        ).fetchone()
    assert attempts == MAX_ATTEMPTS and dead is not None and delivered is None


def test_exhausted_crashed_attempt_is_dead_lettered(dsn, event):
    submit(dsn, event)
    now = datetime.now(UTC) + timedelta(seconds=1)
    for _ in range(MAX_ATTEMPTS):
        assert claim(dsn, now)
        now += timedelta(seconds=31)
    assert claim(dsn, now) is None
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT last_error FROM outbox").fetchone()[0] == "LeaseExpired"
