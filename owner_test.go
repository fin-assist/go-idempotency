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

const resultOwner = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
const foreignOwner = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

func TestCodecOwnerValidationAndBinding(t *testing.T) {
	e := unitEngine(t)
	e.cipher, _ = NewCipher(make([]byte, 32))
	r := request()
	r.Scope = nilScope
	for _, encrypted := range []bool{false, true} {
		p := Policy{TTL: time.Hour, Encrypt: encrypted}
		e.methods[r.Endpoint] = p
		for _, owner := range []string{"", resultOwner} {
			want := Response{Body: []byte("secret"), OwnerID: owner}
			b, _, err := e.encode(r, p, Result{Response: want})
			if err != nil {
				t.Fatal(err)
			}
			if encrypted && bytes.Contains(b, []byte(resultOwner)) {
				t.Fatal("owner leaked in encrypted payload")
			}
			decoded, err := e.decode(r, record{payload: b, encrypted: encrypted, owner: owner})
			if err != nil || decoded.Response.OwnerID != owner {
				t.Fatal(decoded, err)
			}
			for _, altered := range []string{foreignOwner, nilScope} {
				if _, err = e.decode(r, record{payload: b, encrypted: encrypted, owner: altered}); !errors.Is(err, ErrStorage) {
					t.Fatal("owner transplant accepted", err)
				}
			}
			if owner != "" {
				if _, err = e.decode(r, record{payload: b, encrypted: encrypted}); !errors.Is(err, ErrStorage) {
					t.Fatal("removed owner accepted", err)
				}
			}
		}
	}
	p := Policy{TTL: time.Hour}
	for _, owner := range []string{"bad", nilScope, strings.ToUpper(resultOwner)} {
		if _, _, err := e.encode(r, p, Result{Response: Response{OwnerID: owner}}); !errors.Is(err, ErrInvalidRequest) {
			t.Fatal(owner, err)
		}
	}
	r.Scope = foreignOwner
	if _, _, err := e.encode(r, p, Result{Response: Response{OwnerID: resultOwner}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal("foreign scoped owner accepted", err)
	}
	if _, _, err := e.encode(r, p, Result{Response: Response{OwnerID: foreignOwner}}); err != nil {
		t.Fatal(err)
	}
	e.methods[r.Endpoint] = p
	// A response written by v0.1.0 has no OwnerID and its new column defaults to NULL.
	old := []byte(`{"Response":{"Body":"b2xk","Headers":null,"DuplicateRetryAt":"0001-01-01T00:00:00Z"},"Status":null,"RetryAt":"0001-01-01T00:00:00Z"}`)
	decoded, err := e.decode(r, record{payload: old})
	if err != nil || string(decoded.Response.Body) != "old" || decoded.Response.OwnerID != "" {
		t.Fatal(decoded, err)
	}
	if _, err = e.decode(r, record{payload: []byte(`{"Response":{"OwnerID":"bad"}}`)}); !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
}

func ownedEffect(db *serviceDB, owner string) func(context.Context) (Response, error) {
	return func(ctx context.Context) (Response, error) {
		response, err := effect(db)(ctx)
		response.OwnerID = owner
		return response, err
	}
}

func TestPostgresOwnerAtomicReplayPurgeAndCleanup(t *testing.T) {
	e, db, clock := setup(t)
	ctx := t.Context()
	r := request()
	r.Endpoint = "/finassist.user.v1.AuthService/RefreshTokens"
	first, err := runDo(e, db, ctx, r, ownedEffect(db, strings.ToUpper(resultOwner)))
	if err != nil || first.Replayed || first.Response.OwnerID != resultOwner {
		t.Fatal(first, err)
	}
	noEffect := func(context.Context) (Response, error) { t.Fatal("replay ran effect"); return Response{}, nil }
	replay, err := runDo(e, db, ctx, r, noEffect)
	if err != nil || !replay.Replayed || replay.Response.OwnerID != resultOwner || !bytes.Equal(first.Response.Body, replay.Response.Body) {
		t.Fatal(replay, err)
	}
	var owner, scope string
	var payload []byte
	if err = db.pool.QueryRow(ctx, `SELECT owner_id::text,scope::text,payload FROM idempotency_keys`).Scan(&owner, &scope, &payload); err != nil || owner != resultOwner || scope != nilScope || bytes.Contains(payload, []byte(resultOwner)) {
		t.Fatal(owner, scope, err)
	}
	other := r
	other.Key = "22222222-2222-4222-8222-222222222222"
	if _, err = runDo(e, db, ctx, other, ownedEffect(db, foreignOwner)); err != nil {
		t.Fatal(err)
	}
	scoped := request()
	scoped.Scope = resultOwner
	if _, err = runDo(e, db, ctx, scoped, effect(db)); err != nil {
		t.Fatal(err)
	}
	// Mirrors the service's PurgeUserData: never delete the entire nil scope.
	if err = db.Run(ctx, func(ctx context.Context) error {
		tx, _ := db.Tx(ctx)
		tag, err := tx.Exec(ctx, `DELETE FROM idempotency_keys WHERE scope=$1::uuid OR owner_id=$1::uuid`, resultOwner)
		if err == nil && tag.RowsAffected() != 2 {
			t.Fatalf("purged %d", tag.RowsAffected())
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if replay, err = runDo(e, db, ctx, other, noEffect); err != nil || !replay.Replayed || replay.Response.OwnerID != foreignOwner {
		t.Fatal(replay, err)
	}
	// Owner disappears with the row; no orphan mapping or second cleanup is needed.
	clock.Add(time.Minute)
	if err = db.Run(ctx, func(ctx context.Context) error {
		n, err := e.Cleanup(ctx, 1)
		if n != 1 {
			t.Fatalf("cleaned %d", n)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = db.pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_keys`).Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	assertCount(t, db, 3)
}

func TestPostgresOwnerInvalidAndStorageFailureRollBack(t *testing.T) {
	for _, mode := range []string{"invalid", "nil", "foreign-scope", "storage", "noncacheable", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			e, db, _ := setup(t)
			r := request()
			responseOwner := resultOwner
			switch mode {
			case "invalid":
				responseOwner = "bad"
			case "nil":
				responseOwner = nilScope
			case "foreign-scope":
				r.Scope = foreignOwner
			}
			parent, stop := context.WithCancel(t.Context())
			defer stop()
			_, err := runDo(e, db, parent, r, func(ctx context.Context) (Response, error) {
				response, effectErr := ownedEffect(db, responseOwner)(ctx)
				if effectErr != nil {
					return response, effectErr
				}
				tx, _ := db.Tx(ctx)
				switch mode {
				case "storage":
					_, effectErr = tx.Exec(ctx, `ALTER TABLE idempotency_keys RENAME COLUMN owner_id TO unavailable`)
				case "noncacheable":
					effectErr = finerr.InvalidToken()
				case "cancelled":
					stop() // After the effect starts, cancellation must not lose ownership.
				}
				return response, effectErr
			})
			want := ErrInvalidRequest
			switch mode {
			case "storage":
				want = ErrStorage
			case "noncacheable":
				want = ErrNotStored
			case "cancelled":
				want = nil
			}
			if !errors.Is(err, want) {
				t.Fatal(err, want)
			}
			count := 0
			if mode == "cancelled" {
				count = 1
			}
			assertCount(t, db, count)
			var keys int
			if err = db.pool.QueryRow(t.Context(), `SELECT count(*) FROM idempotency_keys`).Scan(&keys); err != nil || keys != count {
				t.Fatal(keys, err)
			}
			if mode == "cancelled" {
				replay, err := runDo(e, db, t.Context(), r, ownedEffect(db, foreignOwner))
				if err != nil || !replay.Replayed || replay.Response.OwnerID != resultOwner {
					t.Fatal(replay, err)
				}
			}
		})
	}
}

func TestPostgresReservedOwnerReplaysThroughEveryEntryPoint(t *testing.T) {
	e, db, _ := setup(t)
	r := request()
	r.Endpoint = askMethod
	var token Reservation
	if err := db.Run(t.Context(), func(ctx context.Context) error { var err error; token, _, err = e.Reserve(ctx, r); return err }); err != nil {
		t.Fatal(err)
	}
	work, cancel := e.WithCompletion(t.Context())
	defer cancel()
	var first Result
	if err := db.Run(work, func(ctx context.Context) error {
		var err error
		first, err = e.DoReserved(ctx, token, ownedEffect(db, resultOwner))
		return err
	}); err != nil || first.Response.OwnerID != resultOwner {
		t.Fatal(first, err)
	}
	if err := db.Run(work, func(ctx context.Context) error {
		got, err := e.DoReserved(ctx, token, ownedEffect(db, foreignOwner))
		if err != nil || !got.Replayed || got.Response.OwnerID != resultOwner {
			t.Fatal(got, err)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Run(t.Context(), func(ctx context.Context) error {
		next, got, err := e.Reserve(ctx, r)
		if err != nil || next != (Reservation{}) || !got.Replayed || got.Response.OwnerID != resultOwner {
			t.Fatal(next, got, err)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	got, err := runDo(e, db, t.Context(), r, ownedEffect(db, foreignOwner))
	if err != nil || !got.Replayed || got.Response.OwnerID != resultOwner {
		t.Fatal(got, err)
	}
	assertCount(t, db, 1)
}

func TestPostgresOwnedCacheableErrorPreservesOwner(t *testing.T) {
	e, db, _ := setup(t)
	first, err := runDo(e, db, t.Context(), request(), func(ctx context.Context) (Response, error) {
		response, effectErr := ownedEffect(db, resultOwner)(ctx)
		if effectErr != nil {
			return response, effectErr
		}
		return response, finerr.WrongFormat()
	})
	if err != nil || first.Response.OwnerID != resultOwner {
		t.Fatal(first, err)
	}
	assertReason(t, first.Err, "WRONG_FORMAT")
	replay, err := runDo(e, db, t.Context(), request(), ownedEffect(db, foreignOwner))
	if err != nil || !replay.Replayed || replay.Response.OwnerID != resultOwner {
		t.Fatal(replay, err)
	}
	assertReason(t, replay.Err, "WRONG_FORMAT")
	assertCount(t, db, 1)
}

func TestPostgresUpgradeKeepsLegacyOwnerlessReplay(t *testing.T) {
	e, db, _ := setup(t)
	// Recreate v0.1.0's live schema and save a result in its original JSON format.
	for _, file := range []string{"003_result_owner_index.down.sql", "002_result_owner.down.sql"} {
		sql, err := fs.ReadFile(Migrations, file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.pool.Exec(t.Context(), string(sql)); err != nil {
			t.Fatal(err)
		}
	}
	r := request()
	legacy := []byte(`{"Response":{"Body":"b2xk","Headers":null,"DuplicateRetryAt":"0001-01-01T00:00:00Z"},"Status":null,"RetryAt":"0001-01-01T00:00:00Z"}`)
	if _, err := db.pool.Exec(t.Context(), `INSERT INTO idempotency_keys(scope,endpoint,key,request_hash,done,payload,expires_at) VALUES($1,$2,$3,$4,true,$5,$6)`, nilScope, r.Endpoint, r.Key, r.Hash[:], legacy, e.now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"002_result_owner.up.sql", "003_result_owner_index.up.sql"} {
		sql, err := fs.ReadFile(Migrations, file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.pool.Exec(t.Context(), string(sql)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := runDo(e, db, t.Context(), r, ownedEffect(db, resultOwner))
	if err != nil || !got.Replayed || got.Response.OwnerID != "" || string(got.Response.Body) != "old" {
		t.Fatal(got, err)
	}
	assertCount(t, db, 0)
}
