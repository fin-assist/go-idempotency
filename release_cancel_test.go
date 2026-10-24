package idempotency

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestIntegrationPostgresConditionalReleaseCancelledWhileWaiting(t *testing.T) {
	e, db, _ := setup(t)
	e.lockTimeout = 3 * time.Second
	token := reserve(t, e, db, request())
	ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	lock, err := db.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback(context.Background()) }()
	if _, err = lock.Exec(ctx, `SELECT 1 FROM idempotency_keys FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	waiter, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- db.Run(waiter, func(ctx context.Context) error {
			yes, err := e.ReleaseIfCurrent(ctx, token)
			if yes {
				return errors.New("cancelled release authorized compensation")
			}
			return err
		})
	}()
	waitForReleaseLock(t, ctx, db)
	cancel()
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = finalize(e, db, token, effect(db)); err != nil {
		t.Fatal("cancelled release changed reservation", err)
	}
	assertCount(t, db, 1)
}
