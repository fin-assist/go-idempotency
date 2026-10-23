package idempotency

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type record struct {
	owner       string
	hash        [32]byte
	done        bool
	payload     []byte
	encrypted   bool
	expires     time.Time
	reservation string
}

func readRecord(ctx context.Context, tx pgx.Tx, r Request) (record, error) {
	var row record
	var hash []byte
	err := tx.QueryRow(ctx, `SELECT request_hash,done,payload,encrypted,expires_at,reservation_id,COALESCE(owner_id::text,'') FROM idempotency_keys WHERE scope=$1 AND endpoint=$2 AND key=$3 FOR UPDATE`, r.Scope, r.Endpoint, r.Key).Scan(&hash, &row.done, &row.payload, &row.encrypted, &row.expires, &row.reservation, &row.owner)
	if err != nil {
		return row, err
	}
	if len(hash) != 32 {
		return row, ErrStorage
	}
	copy(row.hash[:], hash)
	return row, nil
}

func (e *Engine) claim(ctx context.Context, tx pgx.Tx, r Request, ttl time.Duration, id string) (record, bool, error) {
	var row record
	var fresh bool
	err := e.locked(ctx, tx, func() error {
		for range 3 {
			var marker bool
			err := tx.QueryRow(ctx, `INSERT INTO idempotency_keys(scope,endpoint,key,request_hash,done,expires_at,reservation_id) VALUES($1,$2,$3,$4,false,$5,$6) ON CONFLICT DO NOTHING RETURNING true`, r.Scope, r.Endpoint, r.Key, r.Hash[:], e.now().Add(ttl), id).Scan(&marker)
			if err == nil {
				fresh = true
				return nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			row, err = readRecord(ctx, tx, r)
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return err
			}
			if row.expires.After(e.now()) {
				return nil
			}
			// The row lock fences both cleanup and an expired reservation owner.
			if _, err = tx.Exec(ctx, `DELETE FROM idempotency_keys WHERE scope=$1 AND endpoint=$2 AND key=$3`, r.Scope, r.Endpoint, r.Key); err != nil {
				return err
			}
		}
		return ErrStorage
	})
	return row, fresh, err
}

// Cleanup deletes at most limit expired rows in the service's primary transaction.
// Call repeatedly in a periodic worker, committing each batch; cancellation stops
// the job. Locked active keys are skipped. Limit must be between 1 and 10000.
func (e *Engine) Cleanup(ctx context.Context, limit int) (int64, error) {
	if limit < 1 || limit > 10000 {
		return 0, ErrInvalidRequest
	}
	tx, err := e.transaction(ctx)
	if err != nil {
		return 0, err
	}
	tag, err := tx.Exec(ctx, `WITH expired AS (SELECT scope,endpoint,key FROM idempotency_keys WHERE expires_at <= $1 ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED) DELETE FROM idempotency_keys k USING expired x WHERE (k.scope,k.endpoint,k.key)=(x.scope,x.endpoint,x.key)`, e.now(), limit)
	if err != nil {
		return 0, storageError(err)
	}
	return tag.RowsAffected(), nil
}
