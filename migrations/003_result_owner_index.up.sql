CREATE INDEX CONCURRENTLY idempotency_keys_owner_idx ON idempotency_keys (owner_id) WHERE owner_id IS NOT NULL;
