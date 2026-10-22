CREATE TABLE idempotency_keys (
    scope uuid NOT NULL,
    endpoint text NOT NULL CHECK (length(endpoint) BETWEEN 1 AND 256),
    key uuid NOT NULL,
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    done boolean NOT NULL DEFAULT false,
    payload bytea CHECK (octet_length(payload) <= 6000000),
    encrypted boolean NOT NULL DEFAULT false,
    reservation_id text NOT NULL DEFAULT '',
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (scope, endpoint, key),
    CHECK (NOT done OR payload IS NOT NULL)
);
CREATE INDEX idempotency_keys_expires_idx ON idempotency_keys (expires_at);
