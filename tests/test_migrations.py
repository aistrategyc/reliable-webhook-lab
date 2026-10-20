import psycopg
import pytest

from webhook_lab.db import migrate


def test_migrations_are_repeatable_and_detect_drift(dsn):
    migrate(dsn)
    with psycopg.connect(dsn) as conn:
        conn.execute("UPDATE lab_migrations SET checksum = 'tampered'")
    with pytest.raises(RuntimeError, match="changed"):
        migrate(dsn)
