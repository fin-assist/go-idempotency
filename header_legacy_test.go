package idempotency

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// legacyRow captures everything a replay must leave untouched.
type legacyRow struct {
	payload []byte
	done    bool
	expires time.Time
}

func readLegacyRow(t *testing.T, db *serviceDB, key string) legacyRow {
	t.Helper()
	var row legacyRow
	if err := db.pool.QueryRow(context.Background(), `SELECT payload,done,expires_at FROM idempotency_keys WHERE key=$1`, key).Scan(&row.payload, &row.done, &row.expires); err != nil {
		t.Fatal(err)
	}
	return row
}

// FIN-296: outcomes stored by an older version with headers gRPC cannot send
// (or JSON cannot preserve) fail closed on replay: no effect, row kept as is.
func TestIntegrationPostgresLegacyUnsendableHeaderReplayFailsClosed(t *testing.T) {
	for name, payload := range map[string]string{
		"non-ascii value": `{"Response":{"Body":"YW5zd2Vy","Headers":{"x-text":["ё"]}}}`,
		"binary key":      `{"Response":{"Body":"YW5zd2Vy","Headers":{"x-trace-bin":["�"]}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			e, db, _ := setup(t)
			r := request()
			if _, err := runDo(e, db, context.Background(), r, effect(db)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.pool.Exec(context.Background(), `UPDATE idempotency_keys SET payload=$1 WHERE key=$2`, []byte(payload), r.Key); err != nil {
				t.Fatal(err)
			}
			before := readLegacyRow(t, db, r.Key)
			calls := 0
			counting := func(ctx context.Context) (Response, error) { calls++; return effect(db)(ctx) }
			if _, err := runDo(e, db, context.Background(), r, counting); !errors.Is(err, ErrStorage) {
				t.Fatal("Do replay:", err)
			}
			var reserveErr error
			if err := db.Run(context.Background(), func(ctx context.Context) error {
				_, _, reserveErr = e.Reserve(ctx, r)
				return reserveErr
			}); !errors.Is(reserveErr, ErrStorage) || err == nil {
				t.Fatal("Reserve replay:", reserveErr, err)
			}
			after := readLegacyRow(t, db, r.Key)
			if calls != 0 || !bytes.Equal(before.payload, after.payload) || !after.done || !before.expires.Equal(after.expires) {
				t.Fatalf("legacy outcome changed or effect ran: calls=%d before=%+v after=%+v", calls, before, after)
			}
			assertCount(t, db, 1)
		})
	}
}
