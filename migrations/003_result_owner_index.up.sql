-- squawk-ignore prefer-robust-stmts: a failed concurrent build can leave an invalid index; require explicit repair, never skip it with IF NOT EXISTS.
CREATE INDEX CONCURRENTLY idempotency_keys_owner_idx ON idempotency_keys (owner_id) WHERE owner_id IS NOT NULL;
