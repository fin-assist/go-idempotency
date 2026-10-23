package idempotency

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestReleaseIfCurrentPreconditions(t *testing.T) {
	e, err := New(noTx{}, Options{Methods: map[string]Policy{testMethod: {}}, Clock: time.Now, Registerer: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	if yes, err := e.ReleaseIfCurrent(t.Context(), Reservation{}); yes || err != nil {
		t.Fatal(yes, err)
	}
	for _, token := range []Reservation{{id: "x"}, {request: request()}, {request: Request{Endpoint: "unknown"}, id: "x"}} {
		if yes, err := e.ReleaseIfCurrent(t.Context(), token); yes || !errors.Is(err, ErrInvalidRequest) {
			t.Fatal(yes, err)
		}
	}
	if yes, err := e.ReleaseIfCurrent(t.Context(), Reservation{request: request(), id: "x"}); yes || !errors.Is(err, ErrNoTransaction) {
		t.Fatal(yes, err)
	}
}

func TestPostgresConditionalReleaseAtomicCompensation(t *testing.T) {
	e, db, _ := setup(t)
	r := request()
	token := reserve(t, e, db, r)
	rolled := errors.New("service rollback")
	err := db.Run(t.Context(), func(ctx context.Context) error {
		yes, err := e.ReleaseIfCurrent(ctx, token)
		if err != nil || !yes {
			t.Fatal(yes, err)
		}
		if _, err = effect(db)(ctx); err != nil {
			return err
		}
		return rolled
	})
	if !errors.Is(err, rolled) {
		t.Fatal(err)
	}
	assertCount(t, db, 0)
	if _, err = finalize(e, db, token, func(context.Context) (Response, error) { return Response{}, errors.New("leave reservation unfinished") }); err == nil {
		t.Fatal("failure cached")
	}
	err = db.Run(t.Context(), func(ctx context.Context) error {
		yes, err := e.ReleaseIfCurrent(ctx, token)
		if err != nil || !yes {
			t.Fatal(yes, err)
		}
		_, err = effect(db)(ctx)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	assertCount(t, db, 1)
	err = db.Run(t.Context(), func(ctx context.Context) error {
		yes, err := e.ReleaseIfCurrent(ctx, token)
		if yes {
			t.Fatal("second compensation authorized")
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = reserve(t, e, db, r) // No error response was cached by compensation.
}

func TestPostgresConditionalReleaseFencesOwnership(t *testing.T) {
	e, db, clock := setup(t)
	r := request()
	first := reserve(t, e, db, r)
	check := func(token Reservation, want bool) {
		t.Helper()
		err := db.Run(t.Context(), func(ctx context.Context) error {
			yes, err := e.ReleaseIfCurrent(ctx, token)
			if yes != want {
				t.Fatalf("released=%v want=%v", yes, want)
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for name, mutate := range map[string]func(*Reservation){
		"scope":       func(v *Reservation) { v.request.Scope = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" },
		"endpoint":    func(v *Reservation) { v.request.Endpoint = askMethod },
		"key":         func(v *Reservation) { v.request.Key = "55555555-5555-4555-8555-555555555555" },
		"hash":        func(v *Reservation) { v.request.Hash[0] ^= 1 },
		"reservation": func(v *Reservation) { v.id = "forged" },
	} {
		t.Run(name, func(t *testing.T) { v := first; mutate(&v); check(v, false) })
	}
	clock.Add(15 * time.Second)
	check(first, false)
	// Expired old Release keeps its previous housekeeping semantics.
	if err := db.Run(t.Context(), func(ctx context.Context) error { return e.Release(ctx, first) }); err != nil {
		t.Fatal(err)
	}
	second := reserve(t, e, db, r)
	clock.Add(15 * time.Second)
	third := reserve(t, e, db, r)
	check(second, false)
	if _, err := finalize(e, db, third, effect(db)); err != nil {
		t.Fatal(err)
	}
	check(third, false)
	check(first, false)
	result, err := runDo(e, db, t.Context(), r, effect(db))
	if err != nil || !result.Replayed {
		t.Fatal(result, err)
	}
	assertCount(t, db, 1)
}

func TestPostgresConditionalReleaseWaitersCommitAndRollback(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "rollback"}[rollback], func(t *testing.T) {
			e, db, _ := setup(t)
			e.lockTimeout = time.Second
			token := reserve(t, e, db, request())
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			tx, err := db.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(context.Background()) }()
			owned, err := e.ReleaseIfCurrent(context.WithValue(ctx, testTxKey{}, tx), token)
			if err != nil || !owned {
				t.Fatal(owned, err)
			}
			started := make(chan struct{})
			answer := make(chan bool, 1)
			failure := make(chan error, 1)
			go func() {
				close(started)
				var released bool
				err := db.Run(ctx, func(ctx context.Context) error {
					var err error
					released, err = e.ReleaseIfCurrent(ctx, token)
					return err
				})
				answer <- released
				failure <- err
			}()
			<-started
			waitForReleaseLock(t, ctx, db)
			if rollback {
				err = tx.Rollback(ctx)
			} else {
				err = tx.Commit(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case yes := <-answer:
				if yes != rollback {
					t.Fatal("second claimant", yes, rollback)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err = <-failure; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func waitForReleaseLock(t *testing.T, ctx context.Context, db *serviceDB) {
	t.Helper()
	for {
		var waiting bool
		if err := db.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock')`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
	}
}

func TestPostgresConditionalReleaseChecksExpiryAfterLock(t *testing.T) {
	e, db, clock := setup(t)
	e.lockTimeout = time.Second
	token := reserve(t, e, db, request())
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	lock, err := db.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback(context.Background()) }()
	if _, err = lock.Exec(ctx, `SELECT 1 FROM idempotency_keys FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- db.Run(ctx, func(ctx context.Context) error {
			released, err := e.ReleaseIfCurrent(ctx, token)
			if released {
				return errors.New("expired waiter authorized compensation")
			}
			return err
		})
	}()
	waitForReleaseLock(t, ctx, db)
	clock.Add(15 * time.Second)
	if err = lock.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_ = reserve(t, e, db, request())
}

func TestPostgresConditionalReleaseErrors(t *testing.T) {
	e, db, _ := setup(t)
	token := reserve(t, e, db, request())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := db.Run(t.Context(), func(txCtx context.Context) error {
		tx, _ := db.Tx(txCtx)
		yes, err := e.ReleaseIfCurrent(context.WithValue(ctx, testTxKey{}, tx), token)
		if yes || !errors.Is(err, context.Canceled) {
			t.Fatal(yes, err)
		}
		return err
	}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	lock, err := db.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Rollback(context.Background()) }()
	if _, err = lock.Exec(t.Context(), `SELECT 1 FROM idempotency_keys FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	err = db.Run(t.Context(), func(ctx context.Context) error {
		yes, err := e.ReleaseIfCurrent(ctx, token)
		if yes {
			t.Fatal("lock timeout authorized compensation")
		}
		return err
	})
	assertReason(t, err, "REQUEST_IN_PROGRESS")
	_ = lock.Rollback(t.Context())
	if _, err = db.pool.Exec(t.Context(), `DROP TABLE idempotency_keys`); err != nil {
		t.Fatal(err)
	}
	err = db.Run(t.Context(), func(ctx context.Context) error {
		yes, err := e.ReleaseIfCurrent(ctx, token)
		if yes {
			t.Fatal("storage error authorized compensation")
		}
		return err
	})
	if !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
}
