CREATE TABLE IF NOT EXISTS outbox (
    id         BIGSERIAL PRIMARY KEY,
    payload    JSONB         NOT NULL,
    status     TEXT          NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    sent_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(created_at) WHERE status = 'pending';
