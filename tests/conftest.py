"""Every test owns a fresh schema. No production tables are truncated."""

import os
import uuid

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from webhook_lab.db import migrate


@pytest.fixture
def dsn():
    base = os.environ.get("TEST_DATABASE_URL")
    if not base:
        pytest.fail("Set TEST_DATABASE_URL to a disposable PostgreSQL database; see README.")
    schema = "test_" + uuid.uuid4().hex
    with psycopg.connect(base, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    isolated = make_conninfo(base, options=f"-c search_path={schema}")
    try:
        migrate(isolated)
        yield isolated
    finally:
        with psycopg.connect(base, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
