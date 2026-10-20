"""Small, transactional SQL migration runner; existing migrations are immutable."""

import hashlib
from importlib.resources import files

import psycopg


def migrate(dsn: str) -> None:
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))", ("reliable-webhook-lab:migrations",)
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS lab_migrations "
            "(name text PRIMARY KEY, checksum text NOT NULL)"
        )
        resources = files("webhook_lab").joinpath("sql")
        for item in sorted(resources.iterdir(), key=lambda p: p.name):
            if not item.name.endswith(".sql"):
                continue
            body = item.read_text()
            checksum = hashlib.sha256(body.encode()).hexdigest()
            prior = conn.execute(
                "SELECT checksum FROM lab_migrations WHERE name = %s", (item.name,)
            ).fetchone()
            if prior:
                if prior[0] != checksum:
                    raise RuntimeError(f"Migration changed after application: {item.name}")
                continue
            conn.execute(body)
            conn.execute("INSERT INTO lab_migrations VALUES (%s, %s)", (item.name, checksum))
