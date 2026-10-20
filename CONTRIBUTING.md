# Contributing

Run the checks in the README before proposing a change. Changes to a contract,
access boundary or failure behaviour should include a regression test and a
short explanation of the trade-off. Use synthetic fixtures only.

Keep applied SQL migrations immutable; add the next numbered migration.
Commit `uv.lock` when dependencies change. The examples are deliberately small:
new infrastructure should solve a demonstrated problem.
