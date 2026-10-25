package idempotency_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"io/fs"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/fin-assist/contracts/gen/go/finerr"
	outbox "github.com/fin-assist/go-outbox"
	postgres "github.com/fin-assist/go-postgres"
	tombstone "github.com/fin-assist/go-tombstone"
	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"

	idem "github.com/fin-assist/go-idempotency"
	"github.com/fin-assist/go-idempotency/internal/pgtest"
)

const managerOwner = "4a25f514-a58d-4e4e-a722-98e27eaa4e70"
const managerPhoneEndpoint = "/finassist.user.v1.AuthService/StartPhoneVerification"

// This consumer uses only public library APIs. Every collaborator receives the
// same manager, while a second connection observes actual committed state.
type managerConsumer struct {
	pool      *pgxpool.Pool
	manager   *postgres.Manager
	engine    *idem.Engine
	publisher *outbox.Publisher
	guard     *tombstone.Store
}

func newManagerConsumer(t *testing.T) *managerConsumer {
	t.Helper()
	ctx := t.Context()
	dsn := pgtest.DSN(t)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	for _, source := range []struct {
		table string
		files fs.FS
	}{
		{idem.MigrationsTable, idem.Migrations},
		{outbox.MigrationsTable, outbox.Migrations},
		{tombstone.MigrationsTable, tombstone.Migrations},
	} {
		applyManagerMigrations(t, dsn, source.table, source.files)
	}
	for _, sql := range []string{
		`CREATE TABLE service_catalog(id integer PRIMARY KEY)`,
		`INSERT INTO service_catalog VALUES(1)`,
		`CREATE TABLE service_records(owner_id uuid PRIMARY KEY, catalog_id integer NOT NULL REFERENCES service_catalog(id) DEFERRABLE INITIALLY DEFERRED)`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	manager := postgres.New(pool)
	cipher, err := idem.NewCipher(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	engine, err := idem.New(manager, idem.Options{Methods: map[string]idem.Policy{"CreateRecord": {}, managerPhoneEndpoint: {Encrypt: true}}, Cipher: cipher, Clock: time.Now, Registerer: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	publisher, err := outbox.NewPublisher(manager, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := tombstone.New(manager, pool, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	return &managerConsumer{pool, manager, engine, publisher, guard}
}

// Each embedded source has its own version table, as in the service runner.
// golang-migrate applies the CONCURRENTLY steps outside a surrounding transaction.
func applyManagerMigrations(t *testing.T, dsn, table string, source fs.FS) {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	db := stdlib.OpenDB(*config)
	t.Cleanup(func() { _ = db.Close() })
	driver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{MigrationsTable: table})
	if err != nil {
		t.Fatal(err)
	}
	files, err := iofs.New(source, ".")
	if err != nil {
		t.Fatal(err)
	}
	runner, err := migrate.NewWithInstance("iofs", files, "pgx5", driver)
	if err != nil {
		_ = files.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		sourceErr, databaseErr := runner.Close()
		if err := errors.Join(sourceErr, databaseErr); err != nil {
			t.Error("close migrations", err)
		}
	})
	if err := runner.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		t.Fatal(err)
	}
}

func managerRequest() idem.Request {
	return idem.Request{Endpoint: "CreateRecord", Scope: managerOwner, Key: "2b67c9b0-46cc-49a9-86fe-46f1860ab7c7", Hash: sha256.Sum256([]byte("record"))}
}

func (c *managerConsumer) write(ctx context.Context, catalog int) (idem.Response, error) {
	tx, ok := c.manager.Tx(ctx)
	if !ok {
		return idem.Response{}, idem.ErrNoTransaction
	}
	if _, err := tx.Exec(ctx, `INSERT INTO service_records(owner_id,catalog_id) VALUES($1,$2)`, managerOwner, catalog); err != nil {
		return idem.Response{}, err
	}
	if err := c.publisher.Publish(ctx, "record.created", managerOwner, []byte("event")); err != nil {
		return idem.Response{}, err
	}
	return idem.Response{Body: []byte("created"), OwnerID: managerOwner}, nil
}

func (c *managerConsumer) execute(ctx context.Context, effect func(context.Context) (idem.Response, error)) (idem.Result, error) {
	return c.executeRequest(ctx, managerRequest(), effect)
}

func (c *managerConsumer) executeRequest(ctx context.Context, request idem.Request, effect func(context.Context) (idem.Response, error)) (idem.Result, error) {
	completion, cancel := c.engine.WithCompletion(ctx)
	defer cancel() // includes the real outer COMMIT
	var result idem.Result
	err := c.manager.Run(completion, func(ctx context.Context) error {
		// Scope is the verified user: guard before reading or decrypting any outcome,
		// including a saved refusal. The lock serializes this entire operation with Purge.
		alive, err := c.guard.Guard(ctx, request.Scope)
		if err != nil {
			return err
		}
		if !alive {
			return finerr.TokenRevoked()
		}
		result, err = c.engine.Do(ctx, request, effect)
		return err // cacheable result.Err is returned only after successful COMMIT
	})
	return result, err
}

func (c *managerConsumer) assertCounts(t *testing.T, records, outcomes, events int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var gotRecords, gotOutcomes, gotEvents int
	err := c.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM service_records),(SELECT count(*) FROM idempotency_keys),(SELECT count(*) FROM outbox_events)`).Scan(&gotRecords, &gotOutcomes, &gotEvents)
	if err != nil || gotRecords != records || gotOutcomes != outcomes || gotEvents != events {
		t.Fatalf("records/outcomes/events=%d/%d/%d want=%d/%d/%d err=%v", gotRecords, gotOutcomes, gotEvents, records, outcomes, events, err)
	}
	// A new transaction must acquire the guard after every terminal path. This
	// checks its transaction lock, as well as the manager's connection cleanup.
	err = c.manager.Run(ctx, func(ctx context.Context) error {
		tx, _ := c.manager.Tx(ctx)
		if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='300ms'`); err != nil {
			return err
		}
		_, err := c.guard.Guard(ctx, managerOwner)
		return err
	})
	if err != nil {
		t.Fatal("guard remained locked", err)
	}
}

func TestAPIManagerCompositionCommitsOnceAndReplays(t *testing.T) {
	c := newManagerConsumer(t)
	calls := 0
	effect := func(ctx context.Context) (idem.Response, error) {
		calls++
		var response idem.Response
		outer, _ := c.manager.Tx(ctx)
		err := c.manager.Run(ctx, func(nested context.Context) error {
			inner, _ := c.manager.Tx(nested)
			if inner != outer {
				return errors.New("nested transaction changed owner")
			}
			var err error
			response, err = c.write(nested, 1)
			return err
		})
		// Observe through another connection before the outer transaction commits.
		var records, outcomes, events int
		if err == nil {
			err = c.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM service_records),(SELECT count(*) FROM idempotency_keys),(SELECT count(*) FROM outbox_events)`).Scan(&records, &outcomes, &events)
		}
		if err == nil && (records != 0 || outcomes != 0 || events != 0) {
			err = errors.New("nested Run committed early")
		}
		return response, err
	}
	result, err := c.execute(t.Context(), effect)
	if err != nil || result.Err != nil || result.Replayed || string(result.Response.Body) != "created" {
		t.Fatal(result, err)
	}
	c.assertCounts(t, 1, 1, 1)
	result, err = c.execute(t.Context(), effect)
	if err != nil || !result.Replayed || calls != 1 || string(result.Response.Body) != "created" || result.Response.OwnerID != managerOwner {
		t.Fatal(result, calls, err)
	}
	c.assertCounts(t, 1, 1, 1)
}

func TestAPIManagerCompositionRollsBackRefusalAndSanitizesUnknownEffect(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "contract", true: "unknown SQL"}[unknown], func(t *testing.T) {
			c := newManagerConsumer(t)
			result, err := c.execute(t.Context(), func(ctx context.Context) (idem.Response, error) {
				if _, err := c.write(ctx, 1); err != nil {
					return idem.Response{}, err
				}
				if unknown {
					return idem.Response{}, &pgconn.PgError{Code: "23503", Detail: "private-payload", Message: "secret SQL"}
				}
				return idem.Response{}, finerr.NotFound()
			})
			if unknown {
				var pg *pgconn.PgError
				if !errors.Is(err, idem.ErrStorage) || errors.As(err, &pg) || result.Err != nil || len(result.Response.Body) != 0 || strings.Contains(err.Error(), "private-payload") || strings.Contains(err.Error(), "secret SQL") {
					t.Fatal("unknown callback cause was not sanitized", result, err)
				}
			} else if !errors.Is(err, idem.ErrNotStored) || result.Err == nil {
				t.Fatal(result, err)
			}
			c.assertCounts(t, 0, 0, 0)
			result, err = c.execute(t.Context(), func(ctx context.Context) (idem.Response, error) { return c.write(ctx, 1) })
			if err != nil || result.Replayed {
				t.Fatal("same key not retryable", result, err)
			}
			c.assertCounts(t, 1, 1, 1)
		})
	}
}

func TestIntegrationManagerCompositionDeferredCommitRollsBackAndRetries(t *testing.T) {
	c := newManagerConsumer(t)
	callbackFinished := false
	result, err := c.execute(t.Context(), func(ctx context.Context) (idem.Response, error) {
		response, err := c.write(ctx, 2)
		callbackFinished = err == nil
		return response, err
	})
	var pg *pgconn.PgError
	if !callbackFinished || !errors.As(err, &pg) || pg.Code != "23503" || string(result.Response.Body) != "created" {
		t.Fatal("expected failure only at actual COMMIT", callbackFinished, err)
	}
	// A successful callback is not a successful operation: do not return its body
	// or raw manager error to a client when COMMIT fails.
	c.assertCounts(t, 0, 0, 0)
	if _, err := c.pool.Exec(t.Context(), `INSERT INTO service_catalog VALUES(2)`); err != nil {
		t.Fatal(err)
	}
	result, err = c.execute(t.Context(), func(ctx context.Context) (idem.Response, error) { return c.write(ctx, 2) })
	if err != nil || result.Replayed {
		t.Fatal(result, err)
	}
	c.assertCounts(t, 1, 1, 1)
}

func TestAPIManagerCompositionRejectsForeignOwnerBeforeEffects(t *testing.T) {
	c := newManagerConsumer(t)
	foreign := postgres.New(c.pool) // Same pool still has a different transaction owner.
	completion, cancel := c.engine.WithCompletion(t.Context())
	defer cancel()
	called := false
	err := foreign.Run(completion, func(ctx context.Context) error {
		if err := c.manager.Run(ctx, func(context.Context) error { called = true; return nil }); !errors.Is(err, postgres.ErrForeignTransaction) {
			return errors.New("foreign Run was accepted")
		}
		if _, err := c.engine.Do(ctx, managerRequest(), func(context.Context) (idem.Response, error) { called = true; return idem.Response{}, nil }); !errors.Is(err, idem.ErrNoTransaction) {
			return errors.New("foreign idempotency transaction was accepted")
		}
		if err := c.publisher.Publish(ctx, "record.created", managerOwner, nil); !errors.Is(err, outbox.ErrNoTransaction) {
			return errors.New("foreign publisher transaction was accepted")
		}
		if _, err := c.guard.Guard(ctx, managerOwner); !errors.Is(err, tombstone.ErrTransactionRequired) {
			return errors.New("foreign guard transaction was accepted")
		}
		return nil
	})
	if err != nil || called {
		t.Fatal(called, err)
	}
	c.assertCounts(t, 0, 0, 0)
}

func TestIntegrationManagerCompositionAbnormalEffectReleasesEverything(t *testing.T) {
	for _, mode := range []string{"panic", "goexit", "completion canceled"} {
		t.Run(mode, func(t *testing.T) {
			c := newManagerConsumer(t)
			done := make(chan struct{})
			outcome := make(chan any, 1)
			go func() {
				defer close(done)
				defer func() {
					if p := recover(); p != nil {
						outcome <- p
					}
				}()
				completion, cancel := c.engine.WithCompletion(t.Context())
				defer cancel()
				err := c.manager.Run(completion, func(ctx context.Context) error {
					alive, err := c.guard.Guard(ctx, managerOwner)
					if err != nil {
						return err
					}
					if !alive {
						return finerr.TokenRevoked()
					}
					_, err = c.engine.Do(ctx, managerRequest(), func(ctx context.Context) (idem.Response, error) {
						response, err := c.write(ctx, 1)
						if err != nil {
							return response, err
						}
						switch mode {
						case "panic":
							panic("consumer panic")
						case "goexit":
							runtime.Goexit()
						case "completion canceled":
							cancel()
						}
						return response, nil
					})
					return err
				})
				outcome <- err
			}()
			select {
			case <-done:
			case <-time.After(8 * time.Second):
				t.Fatal("effect did not terminate")
			}
			switch mode {
			case "panic":
				if got := <-outcome; got != "consumer panic" {
					t.Fatal(got)
				}
			case "goexit":
				select {
				case got := <-outcome:
					t.Fatal("Goexit returned", got)
				default:
				}
			default:
				if got := <-outcome; got == nil {
					t.Fatal("canceled completion committed")
				}
			}
			c.assertCounts(t, 0, 0, 0)
			if result, err := c.execute(t.Context(), func(ctx context.Context) (idem.Response, error) { return c.write(ctx, 1) }); err != nil || result.Replayed {
				t.Fatal("retry after abnormal effect", result, err)
			}
			c.assertCounts(t, 1, 1, 1)
		})
	}
}

func TestAPIManagerCompositionClientCancellationAfterEffectStillCommits(t *testing.T) {
	c := newManagerConsumer(t)
	request, cancel := context.WithCancel(t.Context())
	defer cancel()
	result, err := c.execute(request, func(ctx context.Context) (idem.Response, error) {
		response, err := c.write(ctx, 1)
		cancel() // Only the request ends; WithCompletion still owns a live deadline.
		return response, err
	})
	if err != nil || result.Err != nil || string(result.Response.Body) != "created" {
		t.Fatal(result, err)
	}
	c.assertCounts(t, 1, 1, 1)
}

func TestAPIManagerCompositionDeletionAndReplayGuardStayAtomic(t *testing.T) {
	c := newManagerConsumer(t)
	if _, err := c.execute(t.Context(), func(ctx context.Context) (idem.Response, error) {
		response, err := c.write(ctx, 1)
		response.OwnerID = "" // This row must be removed through scope, not owner_id.
		return response, err
	}); err != nil {
		t.Fatal(err)
	}
	// Model a no-user auth outcome whose owner was verified by the service.
	// Its nil scope must not cause all no-user outcomes to be deleted.
	authRequest := managerRequest()
	authRequest.Scope = ""
	completion, cancel := c.engine.WithCompletion(t.Context())
	defer cancel()
	if err := c.manager.Run(completion, func(ctx context.Context) error {
		alive, err := c.guard.Guard(ctx, managerOwner)
		if err != nil {
			return err
		}
		if !alive {
			return finerr.TokenRevoked()
		}
		_, err = c.engine.Do(ctx, authRequest, func(context.Context) (idem.Response, error) {
			return idem.Response{Body: []byte("auth outcome"), OwnerID: managerOwner}, nil
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	c.assertCounts(t, 1, 2, 1)
	remove := func(ctx context.Context, owner string) error {
		tx, _ := c.manager.Tx(ctx)
		if _, err := tx.Exec(ctx, `DELETE FROM service_records WHERE owner_id=$1`, owner); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM idempotency_keys WHERE scope=$1::uuid OR owner_id=$1::uuid`, owner); err != nil {
			return err
		}
		return c.publisher.Publish(ctx, "record.deleted", owner, nil)
	}
	refusal := errors.New("service refuses deletion")
	err := c.manager.Run(t.Context(), func(ctx context.Context) error {
		if err := c.guard.Purge(ctx, managerOwner, remove); err != nil {
			return err
		}
		return refusal
	})
	if !errors.Is(err, refusal) {
		t.Fatal(err)
	}
	c.assertCounts(t, 1, 2, 1)
	if deleted, err := c.guard.Deleted(t.Context(), managerOwner); err != nil || deleted {
		t.Fatal("rollback kept tombstone", deleted, err)
	}
	if err := c.manager.Run(t.Context(), func(ctx context.Context) error { return c.guard.Purge(ctx, managerOwner, remove) }); err != nil {
		t.Fatal(err)
	}
	if deleted, err := c.guard.Deleted(t.Context(), managerOwner); err != nil || !deleted {
		t.Fatal("deletion was not committed", deleted, err)
	}
	for _, key := range []string{managerRequest().Key, "8f5301d3-3e0b-439a-a8d4-54832973f677"} {
		request := managerRequest()
		request.Key = key
		called := false
		result, err := c.executeRequest(t.Context(), request, func(context.Context) (idem.Response, error) { called = true; return idem.Response{}, nil })
		rest, ok := finerr.ToREST(err)
		if !ok || rest.Code != "TOKEN_REVOKED" || called || result.Replayed || result.Err != nil || len(result.Response.Body) != 0 {
			t.Fatal("deleted user reached outcome/effect", result, called, err)
		}
		c.assertCounts(t, 0, 0, 2)
	}
}

func TestAPIManagerCompositionGuardsBeforeReadingStaleEncryptedOutcomes(t *testing.T) {
	for _, savedRefusal := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "saved RATE_LIMITED"}[savedRefusal], func(t *testing.T) {
			c := newManagerConsumer(t)
			request := managerRequest()
			request.Endpoint = managerPhoneEndpoint
			calls := 0
			retry := time.Now().Add(time.Minute)
			effect := func(context.Context) (idem.Response, error) {
				calls++
				if savedRefusal {
					return idem.Response{DuplicateRetryAt: retry}, finerr.RateLimited(retry)
				}
				return idem.Response{Body: []byte("encrypted outcome"), OwnerID: managerOwner}, nil
			}
			result, err := c.executeRequest(t.Context(), request, effect)
			if err != nil || result.Replayed {
				t.Fatal(result, err)
			}
			if savedRefusal {
				rest, ok := finerr.ToREST(result.Err)
				if !ok || rest.Code != "RATE_LIMITED" {
					t.Fatal(result.Err)
				}
			}
			result, err = c.executeRequest(t.Context(), request, effect)
			if err != nil || !result.Replayed || calls != 1 {
				t.Fatal(result, calls, err)
			}
			var encrypted bool
			if err := c.pool.QueryRow(t.Context(), `SELECT encrypted FROM idempotency_keys`).Scan(&encrypted); err != nil || !encrypted {
				t.Fatal("fixture was not encrypted", encrypted, err)
			}
			// Artificial stale-outcome fixture, NOT normal Purge: intentionally retain
			// the old encrypted row to prove denial occurs before any key/payload read.
			// Normal service deletion removes scope OR owner_id as tested separately.
			if err := c.manager.Run(t.Context(), func(ctx context.Context) error {
				return c.guard.Purge(ctx, managerOwner, func(context.Context, string) error { return nil })
			}); err != nil {
				t.Fatal(err)
			}
			blocker, err := c.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = blocker.Rollback(ctx)
			}()
			if _, err := blocker.Exec(t.Context(), `LOCK TABLE idempotency_keys IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			// Any Do/key read would block behind this lock until the request deadline.
			// The verified-user Guard must return TOKEN_REVOKED without touching it.
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			result, err = c.executeRequest(ctx, request, effect)
			cancel()
			rest, ok := finerr.ToREST(err)
			if !ok || rest.Code != "TOKEN_REVOKED" || calls != 1 || result.Replayed || result.Err != nil || len(result.Response.Body) != 0 || result.Response.OwnerID != "" {
				t.Fatal("outcome was read before guard", result, calls, err)
			}
			if err := blocker.Rollback(t.Context()); err != nil {
				t.Fatal(err)
			}
			c.assertCounts(t, 0, 1, 0) // Only this explicitly stale fixture retains its old row.
		})
	}
}
