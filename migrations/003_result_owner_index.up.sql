-- A failed concurrent build may leave an invalid index; require explicit repair instead of IF NOT EXISTS.
-- squawk-ignore prefer-robust-stmts
CREATE INDEX CONCURRENTLY idempotency_keys_owner_idx ON idempotency_keys (owner_id) WHERE owner_id IS NOT NULL;
