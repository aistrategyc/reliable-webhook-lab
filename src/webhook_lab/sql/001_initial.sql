CREATE TABLE orders (
    id text PRIMARY KEY,
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'paid'))
);

CREATE TABLE inbox (
    event_id text PRIMARY KEY,
    body_sha256 text NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    outcome text NOT NULL CHECK (outcome IN ('applied', 'duplicate_operation'))
);

CREATE TABLE payments (
    operation_id text PRIMARY KEY,
    order_id text NOT NULL UNIQUE REFERENCES orders(id),
    amount_minor bigint NOT NULL CHECK (amount_minor > 0),
    currency text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
    id uuid PRIMARY KEY,
    operation_id text NOT NULL UNIQUE REFERENCES payments(operation_id),
    payload jsonb NOT NULL,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at timestamptz NOT NULL DEFAULT now(),
    lease_until timestamptz,
    claim_token uuid,
    delivered_at timestamptz,
    dead_at timestamptz,
    last_error text
);
CREATE INDEX outbox_due ON outbox(available_at)
    WHERE delivered_at IS NULL AND dead_at IS NULL;

-- Synthetic receiver: models a downstream service honoring an idempotency key.
CREATE TABLE demo_deliveries (
    operation_id text PRIMARY KEY,
    payload jsonb NOT NULL
);
