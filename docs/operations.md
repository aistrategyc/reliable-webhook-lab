# Running and changing the example

The default Compose database listens on loopback only. The credentials in
`.env.example` are for local synthetic data. Each repository uses its own port
and named volume.

`uv run lab migrate` applies numbered SQL files inside one transaction and records
their checksums. Reapplying an unchanged migration is safe. Editing an applied
migration fails; add the next file instead. The migration account needs DDL
permissions. A production service should use a separate, restricted runtime role.

Tests create a random schema per test and drop only that schema. They require a
dedicated test database and a role allowed to create schemas. There is no SQLite
fallback: row locking and constraints are part of what is being tested.

## Release checks

1. `uv sync --locked`
2. `uv run ruff check . && uv run ruff format --check .`
3. `uv run pytest`
4. `uv run lab demo`
5. `uv build`

CI runs this sequence on Python 3.11 and 3.13 with PostgreSQL 17. It has read-only
repository permissions and uses actions pinned to commit SHAs. `uv.lock` records
the dependency versions. Dependabot proposes updates for review.

The demo is a smoke check, not a production deployment. No workflow receives
client credentials or deploys to a server.

## Before using this in a service

Add the hosting-specific controls: secrets provisioning, TLS, connection pooling,
request and job budgets, structured operational metrics, backup/restore drills,
and a release procedure that handles schema compatibility. Load-test the actual
workload. The design notes list the domain-specific limits.

`docker compose down` stops the local database and preserves its volume.
`docker compose down -v` deletes that example's database; use it only when you
intend to reset the synthetic demo.
