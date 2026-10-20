import argparse
import json
import os
import time
from datetime import UTC, datetime, timedelta

import psycopg
from fastapi.testclient import TestClient

from webhook_lab.api import create_app, sign
from webhook_lab.db import migrate
from webhook_lab.worker import demo_sink, process_one


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic payment → outbox → receiver")
    parser.add_argument("command", choices=["migrate", "demo", "worker-once"])
    args = parser.parse_args()
    dsn = os.environ["DATABASE_URL"]
    migrate(dsn)
    if args.command == "migrate":
        print("Schema is current.")
    elif args.command == "worker-once":
        print(process_one(dsn, demo_sink(dsn), lambda: datetime.now(UTC)))
    else:
        with psycopg.connect(dsn) as conn:
            conn.execute(
                """INSERT INTO orders(id, amount_minor, currency)
                   VALUES ('demo-order', 4900, 'PLN') ON CONFLICT DO NOTHING"""
            )
        secret = os.environ["WEBHOOK_SECRET"].encode()
        event = {
            "event_id": "demo-event",
            "operation_id": "demo-payment",
            "order_id": "demo-order",
            "type": "payment.succeeded",
            "amount_minor": 4900,
            "currency": "PLN",
        }
        with TestClient(create_app(dsn, secret)) as client:
            for label in ["first callback", "same callback", "same payment, new event"]:
                if label.startswith("same payment"):
                    event["event_id"] = "demo-event-2"
                body = json.dumps(event).encode()
                response = client.post(
                    "/webhooks/payments",
                    content=body,
                    headers={"x-lab-signature": sign(secret, body, int(time.time()))},
                )
                print(label, response.status_code, response.json())
        now = datetime.now(UTC)

        def offline(payload, key):
            raise ConnectionError("Synthetic outage")

        print("receiver offline:", process_one(dsn, offline, lambda: now))
        print(
            "receiver recovered:",
            process_one(dsn, demo_sink(dsn), lambda: now + timedelta(seconds=3)),
        )
        with psycopg.connect(dsn) as conn:
            counts = {
                table: conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in ("payments", "outbox", "demo_deliveries")
            }
            print(json.dumps(counts, indent=2))


if __name__ == "__main__":
    main()
