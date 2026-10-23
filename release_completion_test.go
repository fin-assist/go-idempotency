package idempotency

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPostgresCompletionRacesConditionalCompensation(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		t.Run(map[bool]string{false: "completion commits", true: "completion rolls back"}[rollback], func(t *testing.T) {
			e, db, _ := setup(t)
			e.lockTimeout = time.Second
			r := request()
			token := reserve(t, e, db, r)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if _, err := db.pool.Exec(ctx, `CREATE TABLE budget (id integer PRIMARY KEY, hits integer NOT NULL); INSERT INTO budget VALUES (1,1)`); err != nil {
				t.Fatal(err)
			}
			prepared, finish := make(chan struct{}), make(chan struct{})
			completed := make(chan error, 1)
			rolledBack := errors.New("service rolls back completion")
			go func() {
				work, stop := e.WithCompletion(ctx)
				defer stop()
				completed <- db.Run(work, func(txCtx context.Context) error {
					if _, err := e.DoReserved(txCtx, token, effect(db)); err != nil {
						return err
					}
					// The response and effect have been written but remain uncommitted.
					close(prepared)
					select {
					case <-finish:
					case <-ctx.Done():
						return ctx.Err()
					}
					if rollback {
						return rolledBack
					}
					return nil
				})
			}()
			select {
			case <-prepared:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			type reply struct {
				released bool
				err      error
			}
			compensated := make(chan reply, 1)
			go func() {
				var released bool
				err := db.Run(ctx, func(txCtx context.Context) error {
					var err error
					released, err = e.ReleaseIfCurrent(txCtx, token)
					if err != nil || !released {
						return err
					}
					tx, _ := db.Tx(txCtx)
					_, err = tx.Exec(txCtx, `UPDATE budget SET hits=hits-1 WHERE id=1`)
					return err
				})
				compensated <- reply{released, err}
			}()
			waitForReleaseLock(t, ctx, db)
			close(finish)
			select {
			case err := <-completed:
				if rollback && !errors.Is(err, rolledBack) || !rollback && err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case got := <-compensated:
				if got.err != nil || got.released != rollback {
					t.Fatal(got)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var hits int
			if err := db.pool.QueryRow(ctx, `SELECT hits FROM budget WHERE id=1`).Scan(&hits); err != nil {
				t.Fatal(err)
			}
			if rollback {
				if hits != 0 {
					t.Fatal("rolled-back completion was not compensated", hits)
				}
				assertCount(t, db, 0)
				_ = reserve(t, e, db, r)
			} else {
				if hits != 1 {
					t.Fatal("committed completion was compensated", hits)
				}
				out, err := runDo(e, db, ctx, r, effect(db))
				if err != nil || !out.Replayed || string(out.Response.Body) != "answer" {
					t.Fatal("completion response lost", out, err)
				}
				assertCount(t, db, 1)
			}
		})
	}
}
