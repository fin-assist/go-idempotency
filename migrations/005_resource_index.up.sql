-- squawk-ignore prefer-robust-stmts
CREATE INDEX CONCURRENTLY idempotency_keys_resource_idx ON idempotency_keys (scope,resource_type,resource_id) WHERE resource_type IS NOT NULL;
