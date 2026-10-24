package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io/fs"
	"sync"
	"testing"
	"time"

	"github.com/fin-assist/contracts/gen/go/finerr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/fin-assist/go-idempotency/internal/pgtest"
)

type testTxKey struct{}

// serviceDB mirrors the service template: the service starts and commits the
// transaction using the context passed to Run, not the callback's context.
type serviceDB struct{ pool *pgxpool.Pool }

func (d *serviceDB) Tx(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(testTxKey{}).(pgx.Tx)
	return tx, ok
}
func (d *serviceDB) Run(ctx context.Context, fn func(context.Context) error) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = fn(context.WithValue(ctx, testTxKey{}, tx)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *testClock) Add(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.now = c.now.Add(d) }
func setup(t *testing.T) (*Engine, *serviceDB, *testClock) {
	t.Helper()
	e, db, clock, _ := setupWithRegistry(t)
	return e, db, clock
}
func setupWithRegistry(t *testing.T) (*Engine, *serviceDB, *testClock, *prometheus.Registry) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	files, err := fs.Glob(Migrations, "*.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		sql, err := fs.ReadFile(Migrations, f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, string(sql)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = pool.Exec(ctx, `CREATE TABLE effects (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	db := &serviceDB{pool: pool}
	clock := &testClock{now: time.Now().UTC().Truncate(time.Second)}
	aead, err := NewCipher(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewRegistry()
	e, err := New(db, Options{Methods: map[string]Policy{askMethod: {TTL: 24 * time.Hour}, testMethod: {}, "/finassist.user.v1.AuthService/StartPhoneVerification": {Encrypt: true}, "/finassist.user.v1.AuthService/RefreshTokens": {TTL: 60 * time.Second, Encrypt: true}}, Clock: clock.Now, Cipher: aead, LockTimeout: 40 * time.Millisecond, Registerer: registry})
	if err != nil {
		t.Fatal(err)
	}
	return e, db, clock, registry
}
func request() Request {
	return Request{Endpoint: testMethod, Key: testKey, Hash: sha256.Sum256([]byte("request"))}
}
func effect(db *serviceDB) func(context.Context) (Response, error) {
	return func(ctx context.Context) (Response, error) {
		tx, _ := db.Tx(ctx)
		if _, err := tx.Exec(ctx, `INSERT INTO effects DEFAULT VALUES`); err != nil {
			return Response{}, err
		}
		return Response{Body: []byte("answer"), Headers: map[string][]string{"x-test": {"same"}}}, nil
	}
}
func runDo(e *Engine, db *serviceDB, ctx context.Context, r Request, fn func(context.Context) (Response, error)) (Result, error) {
	work, cancel := e.WithCompletion(ctx)
	defer cancel()
	var result Result
	err := db.Run(work, func(ctx context.Context) error { var err error; result, err = e.Do(ctx, r, fn); return err })
	return result, err
}
func assertCount(t *testing.T, db *serviceDB, want int) {
	t.Helper()
	var count int
	if err := db.pool.QueryRow(context.Background(), `SELECT count(*) FROM effects`).Scan(&count); err != nil || count != want {
		t.Fatalf("effects=%d want=%d err=%v", count, want, err)
	}
}
func assertReason(t *testing.T, err error, want string) {
	t.Helper()
	rest, ok := finerr.ToREST(err)
	if !ok || rest.Code != want {
		t.Fatalf("reason=%s want=%s err=%v", rest.Code, want, err)
	}
	if want == "REQUEST_IN_PROGRESS" && rest.RetryAfter <= 0 {
		t.Fatal("missing retry")
	}
}

func TestIntegrationPostgresReplayConflictScopeExpiryRollback(t *testing.T) {
	e, db, clock := setup(t)
	ctx := context.Background()
	r := request()
	result, err := runDo(e, db, ctx, r, effect(db))
	if err != nil || result.Replayed {
		t.Fatal(result, err)
	}
	result, err = runDo(e, db, ctx, r, effect(db))
	if err != nil || !result.Replayed || string(result.Response.Body) != "answer" || result.Response.Headers["x-test"][0] != "same" {
		t.Fatal(result, err)
	}
	assertCount(t, db, 1)
	different := r
	different.Hash = sha256.Sum256([]byte("different"))
	_, err = runDo(e, db, ctx, different, effect(db))
	assertReason(t, err, "IDEMPOTENCY_CONFLICT")
	assertCount(t, db, 1)
	other := r
	other.Scope = "22222222-2222-4222-8222-222222222222"
	if _, err = runDo(e, db, ctx, other, effect(db)); err != nil {
		t.Fatal(err)
	}
	assertCount(t, db, 2)
	clock.Add(24 * time.Hour)
	if result, err = runDo(e, db, ctx, r, effect(db)); err != nil || result.Replayed {
		t.Fatal(result, err)
	}
	assertCount(t, db, 3)
	rollback := errors.New("service rollback")
	work, cancel := e.WithCompletion(ctx)
	defer cancel()
	r.Key = "33333333-3333-4333-8333-333333333333"
	err = db.Run(work, func(ctx context.Context) error {
		_, err := e.Do(ctx, r, effect(db))
		if err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	assertCount(t, db, 3)
	if result, err = runDo(e, db, ctx, r, effect(db)); err != nil || result.Replayed {
		t.Fatal(result, err)
	}
	assertCount(t, db, 4)
}
func TestIntegrationPostgresErrorsAndRateLimitedException(t *testing.T) {
	e, db, clock := setup(t)
	ctx := context.Background()
	for i, domain := range []error{finerr.FileTooLarge(), finerr.WrongFormat(), finerr.InvalidToken(), finerr.ConsentRequired(), finerr.NotFound(), finerr.RateLimited(time.Now().Add(time.Hour)), finerr.InternalError()} {
		r := request()
		r.Hash[0] = byte(i)
		r.Key = "11111111-1111-4111-8111-11111111111" + string("0123456"[i])
		fn := func(ctx context.Context) (Response, error) {
			response, err := effect(db)(ctx)
			if err != nil {
				return response, err
			}
			return response, domain
		}
		result, err := runDo(e, db, ctx, r, fn)
		if i < 2 {
			if err != nil || result.Err == nil {
				t.Fatal(result, err)
			}
			result, err = runDo(e, db, ctx, r, fn)
			if err != nil || !result.Replayed {
				t.Fatal(result, err)
			}
		} else {
			if !errors.Is(err, ErrNotStored) || result.Err == nil {
				t.Fatal(result, err)
			}
		}
	}
	assertCount(t, db, 2)
	r := request()
	r.Endpoint = "/finassist.user.v1.AuthService/StartPhoneVerification"
	retry := clock.Now().Add(time.Minute)
	fn := func(ctx context.Context) (Response, error) {
		response, err := effect(db)(ctx)
		if err != nil {
			return response, err
		}
		response.DuplicateRetryAt = retry
		return response, finerr.RateLimited(retry)
	}
	if _, err := runDo(e, db, ctx, r, fn); err != nil {
		t.Fatal(err)
	}
	clock.Add(20 * time.Second)
	result, err := runDo(e, db, ctx, r, fn)
	if err != nil || !result.Replayed {
		t.Fatal(result, err)
	}
	assertReason(t, result.Err, "RATE_LIMITED")
	assertCount(t, db, 3)
	clock.Add(40 * time.Second)
	if result, err = runDo(e, db, ctx, r, effect(db)); err != nil || result.Replayed {
		t.Fatal(result, err)
	}
	assertCount(t, db, 4)
}
func TestIntegrationPostgresConcurrentTimeoutAndWaitForCommit(t *testing.T) {
	for _, timeout := range []bool{true, false} {
		t.Run(map[bool]string{true: "timeout", false: "commit"}[timeout], func(t *testing.T) {
			e, db, _ := setup(t)
			if !timeout {
				e.lockTimeout = time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			started := make(chan struct{})
			finish := make(chan struct{})
			first := make(chan error, 1)
			go func() {
				_, err := runDo(e, db, ctx, request(), func(ctx context.Context) (Response, error) {
					close(started)
					select {
					case <-finish:
					case <-ctx.Done():
						return Response{}, ctx.Err()
					}
					return ownedEffect(db, resultOwner)(ctx)
				})
				first <- err
			}()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if timeout {
				_, err := runDo(e, db, ctx, request(), ownedEffect(db, foreignOwner))
				assertReason(t, err, "REQUEST_IN_PROGRESS")
				close(finish)
			} else {
				type reply struct {
					result Result
					err    error
				}
				duplicate := make(chan reply, 1)
				go func() {
					result, err := runDo(e, db, ctx, request(), ownedEffect(db, foreignOwner))
					duplicate <- reply{result, err}
				}()
				// Observe the real second connection waiting on the first key's lock.
				for {
					var waiting bool
					err := db.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock')`).Scan(&waiting)
					if err != nil {
						t.Fatal(err)
					}
					if waiting {
						break
					}
					if ctx.Err() != nil {
						t.Fatal(ctx.Err())
					}
				}
				close(finish)
				got := <-duplicate
				if got.err != nil || !got.result.Replayed || got.result.Response.OwnerID != resultOwner {
					t.Fatal(got)
				}
			}
			if err := <-first; err != nil {
				t.Fatal(err)
			}
			assertCount(t, db, 1)
		})
	}
}
func TestIntegrationPostgresCompletionDeadlineWhileWaitingForKey(t *testing.T) {
	for _, reserved := range []bool{false, true} {
		t.Run(map[bool]string{false: "Do", true: "DoReserved"}[reserved], func(t *testing.T) {
			e, db, _ := setup(t)
			e.effectTimeout = 150 * time.Millisecond
			e.lockTimeout = 3 * time.Second
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r := request()
			token := reserve(t, e, db, r)
			owner, err := db.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = owner.Rollback(context.Background()) }()
			if _, err = readRecord(ctx, owner, token.request); err != nil {
				t.Fatal(err)
			}
			work, finish := e.WithCompletion(ctx)
			defer finish()
			called := false
			fn := func(context.Context) (Response, error) {
				called = true
				return Response{}, nil
			}
			started := time.Now()
			err = db.Run(work, func(txCtx context.Context) error {
				var callErr error
				if reserved {
					_, callErr = e.DoReserved(txCtx, token, fn)
				} else {
					_, callErr = e.Do(txCtx, r, fn)
				}
				return callErr
			})
			if !errors.Is(err, context.DeadlineExceeded) || called {
				t.Fatalf("err=%v callback=%v", err, called)
			}
			if elapsed := time.Since(started); elapsed >= 1500*time.Millisecond {
				t.Fatalf("completion deadline did not bound the lock wait: %v", elapsed)
			}
			if ctx.Err() != nil {
				t.Fatalf("original request ended before completion: %v", ctx.Err())
			}
			assertCount(t, db, 0)
		})
	}
}

func TestIntegrationPostgresCancellationCompletionAndTimeout(t *testing.T) {
	e, db, _ := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	result, err := runDo(e, db, ctx, request(), func(ctx context.Context) (Response, error) {
		cancel()
		if ctx.Err() != nil {
			t.Fatal("effect cancelled")
		}
		return effect(db)(ctx)
	})
	if err != nil || result.Replayed {
		t.Fatal(result, err)
	}
	result, err = runDo(e, db, context.Background(), request(), effect(db))
	if err != nil || !result.Replayed {
		t.Fatal(result, err)
	}
	assertCount(t, db, 1)
	if _, err = runDo(e, db, ctx, request(), effect(db)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	e.effectTimeout = 20 * time.Millisecond
	r := request()
	r.Key = "44444444-4444-4444-8444-444444444444"
	_, err = runDo(e, db, context.Background(), r, func(ctx context.Context) (Response, error) { <-ctx.Done(); return Response{}, ctx.Err() })
	if err == nil {
		t.Fatal("timeout committed")
	}
	assertCount(t, db, 1)
}
func reserve(t *testing.T, e *Engine, db *serviceDB, r Request) Reservation {
	t.Helper()
	var token Reservation
	err := db.Run(context.Background(), func(ctx context.Context) error { var err error; token, _, err = e.Reserve(ctx, r); return err })
	if err != nil {
		t.Fatal(err)
	}
	return token
}
func finalize(e *Engine, db *serviceDB, token Reservation, fn func(context.Context) (Response, error)) (Result, error) {
	ctx, cancel := e.WithCompletion(context.Background())
	defer cancel()
	var result Result
	err := db.Run(ctx, func(ctx context.Context) error { var err error; result, err = e.DoReserved(ctx, token, fn); return err })
	return result, err
}
func TestIntegrationPostgresReservationFencingReleaseAndReplay(t *testing.T) {
	e, db, clock := setup(t)
	r := request()
	first := reserve(t, e, db, r)
	err := db.Run(context.Background(), func(ctx context.Context) error { _, _, err := e.Reserve(ctx, r); return err })
	assertReason(t, err, "REQUEST_IN_PROGRESS")
	clock.Add(15 * time.Second)
	second := reserve(t, e, db, r)
	if _, err = finalize(e, db, first, effect(db)); err == nil {
		t.Fatal("stale owner accepted")
	}
	assertReason(t, err, "REQUEST_IN_PROGRESS")
	assertCount(t, db, 0)
	if err = db.Run(context.Background(), func(ctx context.Context) error { return e.Release(ctx, first) }); err != nil {
		t.Fatal(err)
	}
	result, err := finalize(e, db, second, effect(db))
	if err != nil || result.Replayed {
		t.Fatal(result, err)
	}
	result, err = finalize(e, db, first, effect(db))
	if err != nil || !result.Replayed {
		t.Fatal(result, err)
	}
	assertCount(t, db, 1)
	err = db.Run(context.Background(), func(ctx context.Context) error { _, result, err = e.Reserve(ctx, r); return err })
	if err != nil || !result.Replayed {
		t.Fatal(result, err)
	}
	r.Key = "55555555-5555-4555-8555-555555555555"
	third := reserve(t, e, db, r)
	_, err = finalize(e, db, third, func(ctx context.Context) (Response, error) { clock.Add(15 * time.Second); return effect(db)(ctx) })
	assertReason(t, err, "REQUEST_IN_PROGRESS")
	assertCount(t, db, 1)
	fourth := reserve(t, e, db, r)
	if err = db.Run(context.Background(), func(ctx context.Context) error { return e.Release(ctx, fourth) }); err != nil {
		t.Fatal(err)
	}
	_ = reserve(t, e, db, r)
}
func TestIntegrationPostgresEncryptionCleanupAndLockSetting(t *testing.T) {
	e, db, clock := setup(t)
	r := request()
	r.Endpoint = "/finassist.user.v1.AuthService/RefreshTokens"
	if _, err := runDo(e, db, context.Background(), r, effect(db)); err != nil {
		t.Fatal(err)
	}
	var payload []byte
	if err := db.pool.QueryRow(context.Background(), `SELECT payload FROM idempotency_keys`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte("answer")) {
		t.Fatal("plaintext stored")
	}
	result, err := runDo(e, db, context.Background(), r, effect(db))
	if err != nil || !result.Replayed {
		t.Fatal(result, err)
	}
	clock.Add(60 * time.Second)
	var deleted int64
	if err = db.Run(context.Background(), func(ctx context.Context) error { var err error; deleted, err = e.Cleanup(ctx, 1); return err }); err != nil || deleted != 1 {
		t.Fatal(deleted, err)
	}
	work, cancel := e.WithCompletion(context.Background())
	defer cancel()
	if err = db.Run(work, func(ctx context.Context) error {
		tx, _ := db.Tx(ctx)
		if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='2s'`); err != nil {
			return err
		}
		_, err := e.Do(ctx, r, func(ctx context.Context) (Response, error) {
			var timeout string
			if err := tx.QueryRow(ctx, `SHOW lock_timeout`).Scan(&timeout); err != nil {
				return Response{}, err
			}
			if timeout != "2s" {
				t.Fatal(timeout)
			}
			return effect(db)(ctx)
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	assertCount(t, db, 2)
}

func TestIntegrationPostgresStorageFailureAndEncryptedCorruptionDoNotRunEffect(t *testing.T) {
	e, db, _ := setup(t)
	r := request()
	r.Endpoint = "/finassist.user.v1.AuthService/RefreshTokens"
	if _, err := runDo(e, db, context.Background(), r, effect(db)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.pool.Exec(context.Background(), `UPDATE idempotency_keys SET payload='broken'`); err != nil {
		t.Fatal(err)
	}
	if _, err := runDo(e, db, context.Background(), r, effect(db)); !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
	assertCount(t, db, 1)
	if _, err := db.pool.Exec(context.Background(), `DROP TABLE idempotency_keys`); err != nil {
		t.Fatal(err)
	}
	if _, err := runDo(e, db, context.Background(), request(), effect(db)); !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
	assertCount(t, db, 1)
}
func TestIntegrationPostgresCleanupSkipsLockedAndExpiredSavedReservationNeedsNewReserve(t *testing.T) {
	e, db, clock := setup(t)
	r := request()
	token := reserve(t, e, db, r)
	if _, err := finalize(e, db, token, effect(db)); err != nil {
		t.Fatal(err)
	}
	clock.Add(24 * time.Hour)
	if _, err := finalize(e, db, token, effect(db)); err == nil {
		t.Fatal("expired replay accepted")
	} else {
		assertReason(t, err, "REQUEST_IN_PROGRESS")
	}
	tx, err := db.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err = tx.Exec(context.Background(), `SELECT 1 FROM idempotency_keys FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	var count int64
	err = db.Run(context.Background(), func(ctx context.Context) error { var err error; count, err = e.Cleanup(ctx, 100); return err })
	if err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if err = tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	err = db.Run(context.Background(), func(ctx context.Context) error { var err error; count, err = e.Cleanup(ctx, 100); return err })
	if err != nil || count != 1 {
		t.Fatal(count, err)
	}
	if _, err = finalize(e, db, token, effect(db)); err == nil {
		t.Fatal("missing token accepted")
	} else {
		assertReason(t, err, "REQUEST_IN_PROGRESS")
	}
	assertCount(t, db, 1)
}

func TestIntegrationPostgresWaitingDuplicateAcquiresAfterOwnerRollback(t *testing.T) {
	e, db, _ := setup(t)
	e.lockTimeout = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan struct{})
	rollback := make(chan struct{})
	first := make(chan error, 1)
	discard := errors.New("rollback owner")
	go func() {
		_, err := runDo(e, db, ctx, request(), func(ctx context.Context) (Response, error) {
			response, err := effect(db)(ctx)
			if err != nil {
				return response, err
			}
			close(started)
			select {
			case <-rollback:
			case <-ctx.Done():
				return Response{}, ctx.Err()
			}
			return response, discard
		})
		first <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	type reply struct {
		result Result
		err    error
	}
	second := make(chan reply, 1)
	go func() { result, err := runDo(e, db, ctx, request(), effect(db)); second <- reply{result, err} }()
	for {
		var waiting bool
		err := db.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock')`).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
	}
	close(rollback)
	if err := <-first; !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
	got := <-second
	if got.err != nil || got.result.Replayed {
		t.Fatal(got)
	}
	assertCount(t, db, 1)
	replay, err := runDo(e, db, ctx, request(), effect(db))
	if err != nil || !replay.Replayed {
		t.Fatal(replay, err)
	}
	assertCount(t, db, 1)
}
func TestIntegrationUnknownEffectErrorNeverAppearsInResult(t *testing.T) {
	e := unitEngine(t)
	r := request()
	p := e.methods[r.Endpoint]
	result, err := e.perform(context.Background(), unavailableTx{}, r, p, "", Resource{}, func(context.Context) (Response, error) { return Response{}, errors.New("private SQL and credentials") })
	if !errors.Is(err, ErrStorage) || result.Err != nil {
		t.Fatal(result, err)
	}
}

func TestIntegrationPostgresMetricsCountDecisionsOnceAndHaveBoundedLabels(t *testing.T) {
	e, db, _, registry := setupWithRegistry(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := request()
	if _, err := runDo(e, db, ctx, r, effect(db)); err != nil {
		t.Fatal(err)
	}
	if _, err := runDo(e, db, ctx, r, effect(db)); err != nil {
		t.Fatal(err)
	}
	different := r
	different.Hash[0] ^= 1
	_, err := runDo(e, db, ctx, different, effect(db))
	assertReason(t, err, "IDEMPOTENCY_CONFLICT")
	reserved := r
	reserved.Key = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	_ = reserve(t, e, db, reserved)
	err = db.Run(ctx, func(ctx context.Context) error { _, _, err := e.Reserve(ctx, reserved); return err })
	assertReason(t, err, "REQUEST_IN_PROGRESS")
	// Also verify the lock-timeout path doesn't count the same refusal twice.
	locked := r
	locked.Key = "ffffffff-ffff-4fff-8fff-ffffffffffff"
	started := make(chan struct{})
	finish := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		_, err := runDo(e, db, ctx, locked, func(ctx context.Context) (Response, error) {
			close(started)
			select {
			case <-finish:
			case <-ctx.Done():
				return Response{}, ctx.Err()
			}
			return effect(db)(ctx)
		})
		finished <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_, err = runDo(e, db, ctx, locked, effect(db))
	assertReason(t, err, "REQUEST_IN_PROGRESS")
	close(finish)
	if err = <-finished; err != nil {
		t.Fatal(err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 1 || families[0].GetName() != "finassist_idempotency_requests_total" {
		t.Fatal(families)
	}
	want := map[string]float64{"new": 2, "replay": 1, "conflict": 1, "in_progress": 2}
	for _, metric := range families[0].GetMetric() {
		labels := map[string]string{}
		for _, label := range metric.GetLabel() {
			labels[label.GetName()] = label.GetValue()
		}
		if len(labels) != 2 || labels["endpoint"] != testMethod {
			t.Fatal("unbounded labels", labels)
		}
		expected, ok := want[labels["outcome"]]
		if !ok || metric.GetCounter().GetValue() != expected {
			t.Fatal(labels, metric.GetCounter().GetValue(), want)
		}
		delete(want, labels["outcome"])
	}
	if len(want) != 0 {
		t.Fatal("missing outcomes", want)
	}
}

func TestIntegrationPostgresInvalidStoredHashRejectedByCode(t *testing.T) {
	e, db, _ := setup(t)
	if _, err := runDo(e, db, context.Background(), request(), effect(db)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.pool.Exec(context.Background(), "UPDATE idempotency_keys SET request_hash=$1", []byte("invalid hash")); err != nil {
		t.Fatal(err)
	}
	if _, err := runDo(e, db, context.Background(), request(), effect(db)); !errors.Is(err, ErrStorage) {
		t.Fatal("invalid stored hash accepted", err)
	}
	assertCount(t, db, 1)
}
