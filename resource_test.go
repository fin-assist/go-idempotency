package idempotency

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/fin-assist/contracts/gen/go/finerr"
)

func TestResourceInheritanceWithoutDatabase(t *testing.T) {
	bound := Resource{Type: "verification", ID: "42"}
	e := unitEngine(t)
	r := request()
	r.Endpoint = "/finassist.user.v1.AuthService/StartPhoneVerification"
	p := Policy{TTL: time.Hour}
	e.methods[r.Endpoint] = p
	for _, outcome := range []string{"success", "validation", "busy"} {
		for _, explicit := range []Resource{{}, bound, {Type: bound.Type, ID: "other"}, {Type: "other", ID: bound.ID}} {
			input := Response{Resource: explicit, OwnerID: resultOwner, Body: []byte("original"), Headers: map[string][]string{"x-test": {"saved"}}}
			var domainErr error
			if outcome == "validation" {
				domainErr = finerr.WrongFormat()
			}
			if outcome == "busy" {
				input.DuplicateRetryAt = e.now().Add(time.Minute)
				domainErr = finerr.RateLimited(input.DuplicateRetryAt)
			}
			got, err := inheritResource(input, bound)
			if explicit != (Resource{}) && explicit != bound {
				if !errors.Is(err, ErrInvalidRequest) || len(got.Body) != 0 || got.Resource != (Resource{}) {
					t.Fatal("replacement accepted", got, err)
				}
				continue
			}
			if err != nil || got.Resource != bound || got.OwnerID != input.OwnerID || !bytes.Equal(got.Body, input.Body) || got.Headers["x-test"][0] != "saved" || !got.DuplicateRetryAt.Equal(input.DuplicateRetryAt) || input.Resource != explicit {
				t.Fatal("inheritance changed response", got, err)
			}
			payload, _, err := e.encode(r, p, Result{Response: got, Err: domainErr})
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := e.decode(r, record{payload: payload, resource: bound, owner: resultOwner})
			if err != nil || replayed.Response.Resource != bound {
				t.Fatal(replayed, err)
			}
		}
	}
	for _, explicit := range []Resource{{}, bound} {
		got, err := inheritResource(Response{Resource: explicit}, Resource{})
		if err != nil || got.Resource != explicit {
			t.Fatal("unbound response changed", got, err)
		}
	}
}

func TestResourceBoundsAndCodecBinding(t *testing.T) {
	good := Resource{Type: "phone_verification", ID: "42"}
	for _, r := range []Resource{{}, good, {Type: "a" + strings.Repeat("_", 63), ID: strings.Repeat("A", 256)}, {Type: "a.1-x", ID: "UUID:0000-1234_a.b"}} {
		if !validResource(r) {
			t.Fatal("valid resource rejected", r)
		}
	}
	for _, r := range []Resource{{Type: "only"}, {ID: "42"}, {Type: "Upper", ID: "42"}, {Type: "1bad", ID: "42"}, {Type: strings.Repeat("a", 65), ID: "42"}, {Type: "ok", ID: strings.Repeat("x", 257)}, {Type: "ok", ID: "+79001234567"}, {Type: "ok", ID: "\x00"}, {Type: "ok", ID: " x"}, {Type: "ok", ID: "x\n"}, {Type: "a/b", ID: "1"}, {Type: "ok", ID: "тест"}} {
		if validResource(r) {
			t.Fatal("invalid resource accepted")
		}
	}
	e := unitEngine(t)
	e.cipher, _ = NewCipher(make([]byte, 32))
	for _, encrypt := range []bool{false, true} {
		p := Policy{TTL: time.Hour, Encrypt: encrypt}
		e.methods[testMethod] = p
		body, _, err := e.encode(request(), p, Result{Response: Response{Resource: good, Body: []byte("secret")}})
		if err != nil {
			t.Fatal(err)
		}
		if encrypt && bytes.Contains(body, []byte(good.Type)) {
			t.Fatal("resource leaked from encrypted payload")
		}
		row := record{resource: good, payload: body, encrypted: encrypt}
		got, err := e.decode(request(), row)
		if err != nil || got.Response.Resource != good {
			t.Fatal(got, err)
		}
		for _, altered := range []Resource{{}, {Type: good.Type, ID: "43"}, {Type: "other", ID: good.ID}, {Type: "partial"}} {
			row.resource = altered
			if _, err = e.decode(request(), row); !errors.Is(err, ErrStorage) {
				t.Fatal("resource transplant accepted", err)
			}
		}
		if _, _, err = e.encode(request(), p, Result{Response: Response{Resource: Resource{ID: "partial"}}}); !errors.Is(err, ErrInvalidRequest) {
			t.Fatal(err)
		}
	}
	e.methods[testMethod] = Policy{TTL: time.Hour}
	legacy := []byte(`{"Response":{"Body":"b2xk"},"Status":null}`)
	got, err := e.decode(request(), record{payload: legacy})
	if err != nil || got.Response.Resource != (Resource{}) || string(got.Response.Body) != "old" {
		t.Fatal(got, err)
	}
	if _, err = e.decode(request(), record{payload: []byte(`{"Response":{"Resource":{"Type":"partial"}}}`)}); !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
	valid := Reservation{request: request(), id: "own"}
	for _, tc := range []struct {
		token    Reservation
		resource Resource
	}{{Reservation{}, good}, {valid, Resource{}}, {valid, Resource{Type: "partial"}}} {
		if err = e.BindResource(t.Context(), tc.token, tc.resource); !errors.Is(err, ErrInvalidRequest) {
			t.Fatal(err)
		}
	}
	if err = e.BindResource(t.Context(), valid, good); !errors.Is(err, ErrNoTransaction) {
		t.Fatal(err)
	}
}

func TestPostgresResourceAtomicBindingReplayAndPurge(t *testing.T) {
	e, db, clock := setup(t)
	r := request()
	ref := Resource{Type: "upload", ID: "42"}
	first, err := runDo(e, db, t.Context(), r, func(ctx context.Context) (Response, error) {
		out, err := effect(db)(ctx)
		out.Resource = ref
		return out, err
	})
	if err != nil || first.Response.Resource != ref {
		t.Fatal(first, err)
	}
	replay, err := runDo(e, db, t.Context(), r, func(context.Context) (Response, error) { t.Fatal("replay ran effect"); return Response{}, nil })
	if err != nil || !replay.Replayed || replay.Response.Resource != ref {
		t.Fatal(replay, err)
	}
	// Identical IDs in another scope/type remain independent.
	for _, scoped := range []Request{{Scope: resultOwner, Endpoint: testMethod, Key: r.Key, Hash: r.Hash}, {Endpoint: testMethod, Key: "22222222-2222-4222-8222-222222222222", Hash: r.Hash}} {
		if _, err = runDo(e, db, t.Context(), scoped, func(context.Context) (Response, error) {
			other := ref
			if scoped.Scope == "" {
				other.Type = "answer"
			}
			return Response{Resource: other}, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	rolled := errors.New("service rollback")
	err = db.Run(t.Context(), func(ctx context.Context) error {
		tx, _ := db.Tx(ctx)
		_, err := tx.Exec(ctx, `DELETE FROM idempotency_keys WHERE scope=$1 AND resource_type=$2 AND resource_id=$3`, nilScope, ref.Type, ref.ID)
		if err != nil {
			return err
		}
		return rolled
	})
	if !errors.Is(err, rolled) {
		t.Fatal(err)
	}
	if _, err = runDo(e, db, t.Context(), r, effect(db)); err != nil {
		t.Fatal(err)
	}
	assertCount(t, db, 1)
	if err = db.Run(t.Context(), func(ctx context.Context) error {
		tx, _ := db.Tx(ctx)
		tag, err := tx.Exec(ctx, `DELETE FROM idempotency_keys WHERE scope=$1 AND resource_type=$2 AND resource_id=$3`, nilScope, ref.Type, ref.ID)
		if tag.RowsAffected() != 1 {
			t.Fatal("purge crossed resource/scope boundary")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = db.pool.QueryRow(t.Context(), `SELECT count(*) FROM idempotency_keys`).Scan(&count); err != nil || count != 2 {
		t.Fatal(count, err)
	}
	clock.Add(25 * time.Hour)
	if err = db.Run(t.Context(), func(ctx context.Context) error { _, err := e.Cleanup(ctx, 100); return err }); err != nil {
		t.Fatal(err)
	}
	if err = db.pool.QueryRow(t.Context(), `SELECT count(*) FROM idempotency_keys`).Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
}

func TestPostgresResourceReservationInheritanceAndFencing(t *testing.T) {
	e, db, clock := setup(t)
	r := request()
	ref := Resource{Type: "phone_verification", ID: "12"}
	token := reserve(t, e, db, r)
	bind := func(token Reservation, ref Resource) error {
		return db.Run(t.Context(), func(ctx context.Context) error { return e.BindResource(ctx, token, ref) })
	}
	for range 2 {
		if err := bind(token, ref); err != nil {
			t.Fatal(err)
		}
	}
	if err := bind(token, Resource{Type: ref.Type, ID: "13"}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal("rebound reservation", err)
	}
	wrong := token
	wrong.request.Hash[0] ^= 1
	assertReason(t, bind(wrong, ref), "REQUEST_IN_PROGRESS")
	wrong = token
	wrong.request.Scope = resultOwner
	assertReason(t, bind(wrong, ref), "REQUEST_IN_PROGRESS")
	wrong = token
	wrong.id = "foreign"
	assertReason(t, bind(wrong, ref), "REQUEST_IN_PROGRESS")
	clock.Add(reservationTTL)
	assertReason(t, bind(token, ref), "REQUEST_IN_PROGRESS")
	newToken := reserve(t, e, db, r)
	assertReason(t, bind(token, ref), "REQUEST_IN_PROGRESS")
	if err := bind(newToken, ref); err != nil {
		t.Fatal(err)
	}
	if _, err := finalize(e, db, newToken, func(ctx context.Context) (Response, error) {
		out, err := effect(db)(ctx)
		out.Resource = Resource{Type: ref.Type, ID: "other"}
		return out, err
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal("changed resource committed", err)
	}
	assertCount(t, db, 0)
	first, err := finalize(e, db, newToken, effect(db))
	if err != nil || first.Response.Resource != ref {
		t.Fatal("link not inherited", first, err)
	}
	assertReason(t, bind(newToken, ref), "REQUEST_IN_PROGRESS")
	for _, call := range []func() (Result, error){
		func() (Result, error) { return finalize(e, db, newToken, effect(db)) },
		func() (Result, error) { return runDo(e, db, t.Context(), r, effect(db)) },
		func() (Result, error) {
			var out Result
			err := db.Run(t.Context(), func(ctx context.Context) error { var err error; _, out, err = e.Reserve(ctx, r); return err })
			return out, err
		},
	} {
		got, err := call()
		if err != nil || !got.Replayed || got.Response.Resource != ref {
			t.Fatal(got, err)
		}
	}
	assertCount(t, db, 1)
}

func TestPostgresResourceStoredErrorsAndRollback(t *testing.T) {
	for _, outcome := range []string{"success", "validation", "busy", "noncacheable", "invalid", "storage"} {
		t.Run(outcome, func(t *testing.T) {
			e, db, clock := setup(t)
			r := request()
			r.Endpoint = "/finassist.user.v1.AuthService/StartPhoneVerification"
			ref := Resource{Type: "phone_verification", ID: "1"}
			token := reserve(t, e, db, r)
			if err := db.Run(t.Context(), func(ctx context.Context) error { return e.BindResource(ctx, token, ref) }); err != nil {
				t.Fatal(err)
			}
			got, err := finalize(e, db, token, func(ctx context.Context) (Response, error) {
				out, err := effect(db)(ctx)
				if err != nil {
					return out, err
				}
				switch outcome {
				case "validation":
					return out, finerr.WrongFormat()
				case "busy":
					out.DuplicateRetryAt = clock.Now().Add(time.Minute)
					return out, finerr.RateLimited(out.DuplicateRetryAt)
				case "noncacheable":
					return out, finerr.InvalidToken()
				case "invalid":
					out.Resource = Resource{Type: "partial"}
				case "storage":
					tx, _ := db.Tx(ctx)
					_, err = tx.Exec(ctx, `ALTER TABLE idempotency_keys RENAME COLUMN resource_type TO unavailable`)
				}
				return out, err
			})
			if outcome == "noncacheable" || outcome == "invalid" || outcome == "storage" {
				want := ErrInvalidRequest
				if outcome == "noncacheable" {
					want = ErrNotStored
				}
				if outcome == "storage" {
					want = ErrStorage
				}
				if !errors.Is(err, want) {
					t.Fatal(err, want)
				}
				assertCount(t, db, 0)
				return
			}
			if err != nil || got.Response.Resource != ref {
				t.Fatal(got, err)
			}
			replay, err := runDo(e, db, t.Context(), r, effect(db))
			if err != nil || !replay.Replayed || replay.Response.Resource != ref {
				t.Fatal(replay, err)
			}
			if outcome == "busy" {
				assertReason(t, replay.Err, "RATE_LIMITED")
			}
			if outcome == "validation" {
				assertReason(t, replay.Err, "WRONG_FORMAT")
			}
			assertCount(t, db, 1)
		})
	}
}

func TestPostgresResourceCorruptionAndUpgrade(t *testing.T) {
	e, db, _ := setup(t)
	for _, file := range []string{"005_resource_index.down.sql", "004_resource.down.sql"} {
		sql, err := fs.ReadFile(Migrations, file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.pool.Exec(t.Context(), string(sql)); err != nil {
			t.Fatal(err)
		}
	}
	r := request()
	legacy := []byte(`{"Response":{"Body":"b2xk"},"Status":null}`)
	if _, err := db.pool.Exec(t.Context(), `INSERT INTO idempotency_keys(scope,endpoint,key,request_hash,done,payload,expires_at)VALUES($1,$2,$3,$4,true,$5,$6)`, nilScope, r.Endpoint, r.Key, r.Hash[:], legacy, e.now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"004_resource.up.sql", "005_resource_index.up.sql"} {
		sql, err := fs.ReadFile(Migrations, file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.pool.Exec(t.Context(), string(sql)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := runDo(e, db, t.Context(), r, effect(db))
	if err != nil || !got.Replayed || got.Response.Resource != (Resource{}) || string(got.Response.Body) != "old" {
		t.Fatal(got, err)
	}
	for _, sql := range []string{`UPDATE idempotency_keys SET resource_type='upload',resource_id=NULL`, `UPDATE idempotency_keys SET resource_type=NULL,resource_id='1'`, `UPDATE idempotency_keys SET resource_type='',resource_id=''`, `UPDATE idempotency_keys SET resource_type='upload',resource_id='1'`} {
		if _, err = db.pool.Exec(t.Context(), sql); err != nil {
			t.Fatal(err)
		}
		if _, err = runDo(e, db, t.Context(), r, effect(db)); !errors.Is(err, ErrStorage) {
			t.Fatal("corrupt resource ran effect", err)
		}
	}
	assertCount(t, db, 0)
}

func TestPostgresBindResourceCancellationAndExpiryAfterWait(t *testing.T) {
	for _, mode := range []string{"cancel", "expire", "purge", "lock timeout"} {
		t.Run(mode, func(t *testing.T) {
			e, db, clock := setup(t)
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
			if mode == "lock timeout" {
				e.lockTimeout = 100 * time.Millisecond
			}
			waiter, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- db.Run(waiter, func(ctx context.Context) error { return e.BindResource(ctx, token, Resource{Type: "upload", ID: "1"}) })
			}()
			waitForReleaseLock(t, ctx, db)
			switch mode {
			case "cancel":
				cancel()
			case "expire":
				clock.Add(reservationTTL)
			case "purge":
				if _, err = lock.Exec(ctx, `DELETE FROM idempotency_keys`); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "lock timeout" {
				if err = lock.Commit(ctx); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err = <-done:
				if mode == "cancel" {
					if !errors.Is(err, context.Canceled) {
						t.Fatal(err)
					}
				} else {
					assertReason(t, err, "REQUEST_IN_PROGRESS")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestPostgresBindResourceRollbackAndStorageFailures(t *testing.T) {
	e, db, _ := setup(t)
	token := reserve(t, e, db, request())
	ref := Resource{Type: "upload", ID: "1"}
	rolled := errors.New("resource claim failed")
	err := db.Run(t.Context(), func(ctx context.Context) error {
		if err := e.BindResource(ctx, token, ref); err != nil {
			return err
		}
		return rolled
	})
	if !errors.Is(err, rolled) {
		t.Fatal(err)
	}
	err = db.Run(t.Context(), func(ctx context.Context) error {
		tx, _ := db.Tx(ctx)
		if _, err := tx.Exec(ctx, `ALTER TABLE idempotency_keys RENAME COLUMN resource_type TO unavailable`); err != nil {
			return err
		}
		return e.BindResource(ctx, token, ref)
	})
	if !errors.Is(err, ErrStorage) {
		t.Fatal("storage failure not closed", err)
	}
	var count int
	if err = db.pool.QueryRow(t.Context(), `SELECT count(*) FROM idempotency_keys WHERE resource_type IS NULL AND resource_id IS NULL`).Scan(&count); err != nil || count != 1 {
		t.Fatal("rolled back link survived", count, err)
	}
	if err = db.Run(t.Context(), func(ctx context.Context) error { return e.BindResource(ctx, token, ref) }); err != nil {
		t.Fatal("failed binding prevented retry", err)
	}
}
