package idempotency_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/fin-assist/contracts/gen/go/finerr"
	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"

	idem "github.com/fin-assist/go-idempotency"
	"github.com/fin-assist/go-idempotency/internal/pgtest"
)

type consumerKey struct{}
type consumerDB struct{ pool *pgxpool.Pool }

func (d consumerDB) Tx(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(consumerKey{}).(pgx.Tx)
	return tx, ok
}
func (d consumerDB) Run(ctx context.Context, fn func(context.Context) error) error {
	tx, err := d.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err = fn(context.WithValue(ctx, consumerKey{}, tx)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

const publicOperation = "consumer.create"
const publicOwner = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
const publicForeign = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
const publicKey = "11111111-1111-4111-8111-111111111111"

func publicEngine(t *testing.T) (context.Context, consumerDB, *idem.Engine, *time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	dsn := pgtest.DSN(t)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	migrationDB := stdlib.OpenDB(*config)
	t.Cleanup(func() { _ = migrationDB.Close() })
	driver, err := pgxmigrate.WithInstance(migrationDB, &pgxmigrate.Config{MigrationsTable: idem.MigrationsTable})
	if err != nil {
		t.Fatal(err)
	}
	source, err := iofs.New(idem.Migrations, ".")
	if err != nil {
		t.Fatal(err)
	}
	runner, err := migrate.NewWithInstance("iofs", source, "pgx5", driver)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = runner.Close() })
	if err = runner.Up(); err != nil {
		t.Fatal(err)
	}
	version, dirty, err := runner.Version()
	if err != nil || dirty || version != 5 {
		t.Fatalf("service migration state version=%d dirty=%v err=%v", version, dirty, err)
	}
	if _, err = pool.Exec(ctx, "CREATE TABLE consumer_effects(owner_id uuid NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	db := consumerDB{pool}
	now := time.Now().UTC()
	cipher, err := idem.NewCipher(bytes.Repeat([]byte{42}, 32))
	if err != nil {
		t.Fatal(err)
	}
	engine, err := idem.New(db, idem.Options{Methods: map[string]idem.Policy{publicOperation: {TTL: time.Minute, Encrypt: true}}, Clock: func() time.Time { return now }, Cipher: cipher, Registerer: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	return ctx, db, engine, &now
}
func consumerRequest() idem.Request {
	return idem.Request{Scope: publicOwner, Endpoint: publicOperation, Key: publicKey, Hash: sha256.Sum256([]byte("input"))}
}
func consumerDo(ctx context.Context, db consumerDB, e *idem.Engine, r idem.Request, fn func(context.Context) (idem.Response, error)) (idem.Result, error) {
	work, cancel := e.WithCompletion(ctx)
	defer cancel()
	var result idem.Result
	err := db.Run(work, func(ctx context.Context) error { var err error; result, err = e.Do(ctx, r, fn); return err })
	return result, err
}
func consumerEffect(db consumerDB, owner string) func(context.Context) (idem.Response, error) {
	return func(ctx context.Context) (idem.Response, error) {
		tx, ok := db.Tx(ctx)
		if !ok {
			return idem.Response{}, idem.ErrNoTransaction
		}
		_, err := tx.Exec(ctx, "INSERT INTO consumer_effects VALUES($1)", owner)
		return idem.Response{OwnerID: owner, Body: []byte("secret serialized reply"), Resource: idem.Resource{Type: "operation", ID: "resource-1"}, Headers: map[string][]string{"x-result": {"created"}}}, err
	}
}
func requireEffects(t *testing.T, ctx context.Context, db consumerDB, want int) {
	t.Helper()
	var count int
	if err := db.pool.QueryRow(ctx, "SELECT count(*) FROM consumer_effects").Scan(&count); err != nil || count != want {
		t.Fatalf("effects=%d want=%d %v", count, want, err)
	}
}
func TestAPIConsumerCommitReplayScopeAndRollback(t *testing.T) {
	ctx, db, e, now := publicEngine(t)
	r := consumerRequest()
	result, err := consumerDo(ctx, db, e, r, consumerEffect(db, publicOwner))
	if err != nil || result.Replayed || result.Response.OwnerID != publicOwner {
		t.Fatalf("new=%+v %v", result, err)
	}
	var payload []byte
	if err = db.pool.QueryRow(ctx, "SELECT payload FROM idempotency_keys").Scan(&payload); err != nil || bytes.Contains(payload, result.Response.Body) {
		t.Fatalf("plaintext persisted %v", err)
	}
	replay, err := consumerDo(ctx, db, e, r, func(context.Context) (idem.Response, error) {
		t.Error("replay ran effect")
		return idem.Response{}, nil
	})
	if err != nil || !replay.Replayed || !bytes.Equal(replay.Response.Body, result.Response.Body) || replay.Response.Resource != result.Response.Resource || replay.Response.OwnerID != publicOwner {
		t.Fatalf("replay=%+v %v", replay, err)
	}
	conflict := r
	conflict.Hash = sha256.Sum256([]byte("changed"))
	_, err = consumerDo(ctx, db, e, conflict, consumerEffect(db, publicOwner))
	rest, ok := finerr.ToREST(err)
	if !ok || rest.Code != "IDEMPOTENCY_CONFLICT" {
		t.Fatalf("conflict %v", err)
	}
	requireEffects(t, ctx, db, 1)
	foreign := r
	foreign.Scope = publicForeign
	if _, err = consumerDo(ctx, db, e, foreign, consumerEffect(db, publicForeign)); err != nil {
		t.Fatal(err)
	}
	requireEffects(t, ctx, db, 2)
	rollback := r
	rollback.Key = "22222222-2222-4222-8222-222222222222"
	failure := errors.New("consumer transaction failed")
	work, cancel := e.WithCompletion(ctx)
	defer cancel()
	err = db.Run(work, func(ctx context.Context) error {
		if _, err := e.Do(ctx, rollback, consumerEffect(db, publicOwner)); err != nil {
			return err
		}
		return failure
	})
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	requireEffects(t, ctx, db, 2)
	if retry, err := consumerDo(ctx, db, e, rollback, consumerEffect(db, publicOwner)); err != nil || retry.Replayed {
		t.Fatalf("rolled-back replay %v %v", retry.Replayed, err)
	}
	requireEffects(t, ctx, db, 3)
	*now = now.Add(2 * time.Minute)
	if err = db.Run(ctx, func(ctx context.Context) error {
		n, err := e.Cleanup(ctx, 100)
		if n != 3 {
			t.Errorf("expired rows=%d", n)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if retry, err := consumerDo(ctx, db, e, r, consumerEffect(db, publicOwner)); err != nil || retry.Replayed {
		t.Fatalf("expiry %v %v", retry.Replayed, err)
	}
	requireEffects(t, ctx, db, 4)
}
func TestAPIConsumerReservationBindingAndCompensation(t *testing.T) {
	ctx, db, e, _ := publicEngine(t)
	r := consumerRequest()
	var token idem.Reservation
	if err := db.Run(ctx, func(ctx context.Context) error {
		var err error
		token, _, err = e.Reserve(ctx, r)
		if err != nil {
			return err
		}
		return e.BindResource(ctx, token, idem.Resource{Type: "operation", ID: "resource-1"})
	}); err != nil {
		t.Fatal(err)
	}
	_, err := consumerDo(ctx, db, e, r, consumerEffect(db, publicOwner))
	rest, ok := finerr.ToREST(err)
	if !ok || rest.Code != "REQUEST_IN_PROGRESS" {
		t.Fatalf("inflight %v", err)
	}
	requireEffects(t, ctx, db, 0)
	work, cancel := e.WithCompletion(ctx)
	defer cancel()
	var result idem.Result
	if err = db.Run(work, func(ctx context.Context) error {
		var err error
		result, err = e.DoReserved(ctx, token, consumerEffect(db, publicOwner))
		return err
	}); err != nil || result.Replayed || result.Response.Resource.ID != "resource-1" {
		t.Fatalf("complete %+v %v", result, err)
	}
	if err = db.Run(ctx, func(ctx context.Context) error {
		released, err := e.ReleaseIfCurrent(ctx, token)
		if released {
			t.Error("completed work authorized refund")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.Run(work, func(ctx context.Context) error {
		var err error
		result, err = e.DoReserved(ctx, token, func(context.Context) (idem.Response, error) {
			t.Error("reserved replay ran effect")
			return idem.Response{}, nil
		})
		return err
	}); err != nil || !result.Replayed {
		t.Fatalf("reserved replay %v %v", result.Replayed, err)
	}
	requireEffects(t, ctx, db, 1)
	r.Key = "33333333-3333-4333-8333-333333333333"
	if err = db.Run(ctx, func(ctx context.Context) error { var err error; token, _, err = e.Reserve(ctx, r); return err }); err != nil {
		t.Fatal(err)
	}
	abort := errors.New("refund transaction failed")
	if err = db.Run(ctx, func(ctx context.Context) error {
		released, err := e.ReleaseIfCurrent(ctx, token)
		if err != nil {
			return err
		}
		if !released {
			t.Error("owner denied compensation")
		}
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	if err = db.Run(ctx, func(ctx context.Context) error {
		released, err := e.ReleaseIfCurrent(ctx, token)
		if !released {
			t.Error("rollback lost reservation")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.Run(ctx, func(ctx context.Context) error {
		released, err := e.ReleaseIfCurrent(ctx, token)
		if released {
			t.Error("double refund authorized")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = consumerDo(ctx, db, e, r, consumerEffect(db, publicOwner)); err != nil {
		t.Fatal(err)
	}
	requireEffects(t, ctx, db, 2)
}
func TestAPIConsumerStorageAndOutcomeErrors(t *testing.T) {
	ctx, db, e, _ := publicEngine(t)
	r := consumerRequest()
	domain := finerr.InvalidToken()
	result, err := consumerDo(ctx, db, e, r, func(ctx context.Context) (idem.Response, error) {
		_, err := consumerEffect(db, publicOwner)(ctx)
		if err != nil {
			return idem.Response{}, err
		}
		return idem.Response{}, domain
	})
	if !errors.Is(err, idem.ErrNotStored) || result.Err == nil {
		t.Fatalf("domain %+v %v", result, err)
	}
	requireEffects(t, ctx, db, 0)
	unknown := errors.New("SQL secret")
	result, err = consumerDo(ctx, db, e, r, func(context.Context) (idem.Response, error) { return idem.Response{}, unknown })
	if !errors.Is(err, idem.ErrStorage) || result.Err != nil {
		t.Fatalf("unsafe result %+v %v", result, err)
	}
	if _, err = consumerDo(ctx, db, e, r, consumerEffect(db, publicOwner)); err != nil {
		t.Fatal(err)
	}
	if _, err = db.pool.Exec(ctx, "UPDATE idempotency_keys SET payload=$1", []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	_, err = consumerDo(ctx, db, e, r, func(context.Context) (idem.Response, error) {
		t.Error("corruption re-executed effect")
		return idem.Response{}, nil
	})
	if !errors.Is(err, idem.ErrStorage) {
		t.Fatal(err)
	}
	requireEffects(t, ctx, db, 1)
	if _, err = e.Cleanup(ctx, 0); !errors.Is(err, idem.ErrInvalidRequest) {
		t.Fatal(err)
	}
	if _, err = e.Cleanup(ctx, 1); !errors.Is(err, idem.ErrNoTransaction) {
		t.Fatal(err)
	}
	if _, err = db.pool.Exec(ctx, "DROP TABLE idempotency_keys"); err != nil {
		t.Fatal(err)
	}
	r.Key = "44444444-4444-4444-8444-444444444444"
	_, err = consumerDo(ctx, db, e, r, func(context.Context) (idem.Response, error) {
		t.Error("failed storage ran effect")
		return idem.Response{}, nil
	})
	if !errors.Is(err, idem.ErrStorage) {
		t.Fatal(err)
	}
}

func TestAPIConsumerConfiguredAdmissionAndStoredErrors(t *testing.T) {
	ctx, db, e, _ := publicEngine(t)
	interceptor := e.UnaryInterceptor(func(context.Context) (string, error) { return publicOwner, nil })
	called := 0
	handler := func(ctx context.Context, input any) (any, error) {
		called++
		r, ok := idem.FromContext(ctx)
		if !ok {
			t.Fatal("configured call missing request metadata")
		}
		hash, err := idem.HashProto(&emptypb.Empty{})
		if err != nil {
			return nil, err
		}
		r.Hash = hash
		result, err := consumerDo(ctx, db, e, r, func(context.Context) (idem.Response, error) { return idem.Response{}, finerr.WrongFormat() })
		if err != nil {
			return nil, err
		}
		return result, result.Err
	}
	info := &grpc.UnaryServerInfo{FullMethod: publicOperation}
	for _, values := range [][]string{nil, {"invalid"}, {publicKey, publicKey}} {
		md := metadata.MD{}
		md.Set("idempotency-key", values...)
		_, err := interceptor(metadata.NewIncomingContext(ctx, md), nil, info, handler)
		rest, ok := finerr.ToREST(err)
		if !ok || rest.Code != "VALIDATION_ERROR" {
			t.Fatalf("invalid metadata admitted: %v", err)
		}
	}
	if called != 0 {
		t.Fatal("invalid admission reached consumer")
	}
	incoming := metadata.NewIncomingContext(ctx, metadata.Pairs("idempotency-key", publicKey))
	for i := 0; i < 2; i++ {
		value, err := interceptor(incoming, nil, info, handler)
		rest, ok := finerr.ToREST(err)
		if !ok || rest.Code != "WRONG_FORMAT" {
			t.Fatalf("saved domain: %v", err)
		}
		result, ok := value.(idem.Result)
		if !ok || result.Replayed != (i == 1) {
			t.Fatalf("saved error replay: %+v", value)
		}
	}
	if called != 2 {
		t.Fatalf("consumer calls=%d", called)
	}
	bypass := false
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "unconfigured"}, func(ctx context.Context, _ any) (any, error) {
		bypass = true
		if _, ok := idem.FromContext(ctx); ok {
			t.Error("unconfigured call acquired key")
		}
		return nil, nil
	})
	if err != nil || !bypass {
		t.Fatalf("unconfigured method: %v", err)
	}
	denied := errors.New("identity denied")
	_, err = e.UnaryInterceptor(func(context.Context) (string, error) { return "", denied })(incoming, nil, info, handler)
	if !errors.Is(err, denied) {
		t.Fatal(err)
	}
	_, err = e.UnaryInterceptor(nil)(incoming, nil, info, handler)
	if !errors.Is(err, idem.ErrInvalidRequest) {
		t.Fatal(err)
	}
	if _, err := idem.HashFile(strings.NewReader("abc"), 2); err == nil {
		t.Fatal("oversize file accepted")
	}
	if _, err := idem.HashFile(nil, 1); !errors.Is(err, idem.ErrInvalidRequest) {
		t.Fatal(err)
	}
	if _, err := idem.HashProto(nil); !errors.Is(err, idem.ErrInvalidRequest) {
		t.Fatal(err)
	}
	if _, err := idem.NewCipher([]byte("invalid")); !errors.Is(err, idem.ErrInvalidRequest) {
		t.Fatal(err)
	}
}

func TestAPIConsumerRejectsInvalidPoliciesRequestsAndResponses(t *testing.T) {
	ctx, db, e, _ := publicEngine(t)
	good := func() idem.Options {
		return idem.Options{Methods: map[string]idem.Policy{publicOperation: {}}, Clock: time.Now, Registerer: prometheus.NewRegistry()}
	}
	for _, change := range []func(*idem.Options){func(o *idem.Options) { o.Clock = nil }, func(o *idem.Options) { o.Registerer = nil }, func(o *idem.Options) { o.Methods = nil }, func(o *idem.Options) { o.LockTimeout = time.Nanosecond }, func(o *idem.Options) { o.EffectTimeout = -1 }, func(o *idem.Options) { o.Methods = map[string]idem.Policy{"": {}} }, func(o *idem.Options) { o.Methods = map[string]idem.Policy{"bad\nmethod": {}} }, func(o *idem.Options) { o.Methods = map[string]idem.Policy{publicOperation: {TTL: -1}} }, func(o *idem.Options) { o.Methods = map[string]idem.Policy{publicOperation: {Encrypt: true}} }, func(o *idem.Options) { o.Methods = map[string]idem.Policy{"/consumer/RefreshTokens": {}} }, func(o *idem.Options) {
		o.Methods = map[string]idem.Policy{"/finassist.user.v1.AuthService/Register": {}}
	}} {
		options := good()
		change(&options)
		if _, err := idem.New(db, options); !errors.Is(err, idem.ErrInvalidRequest) {
			t.Fatalf("invalid policy=%+v err=%v", options, err)
		}
	}
	if _, err := idem.New(nil, good()); !errors.Is(err, idem.ErrInvalidRequest) {
		t.Fatal(err)
	}
	opts := good()
	if _, err := idem.New(db, opts); err != nil {
		t.Fatal(err)
	}
	if _, err := idem.New(db, opts); err == nil {
		t.Fatal("duplicate metric registration accepted")
	}
	r := consumerRequest()
	unexpected := func(context.Context) (idem.Response, error) {
		t.Error("invalid request executed effect")
		return idem.Response{}, nil
	}
	for _, change := range []func(*idem.Request){func(r *idem.Request) { r.Endpoint = "unknown" }, func(r *idem.Request) { r.Hash = [32]byte{} }, func(r *idem.Request) { r.Key = "invalid" }, func(r *idem.Request) { r.Scope = "untrusted" }} {
		invalid := r
		change(&invalid)
		if _, err := consumerDo(ctx, db, e, invalid, unexpected); !errors.Is(err, idem.ErrInvalidRequest) {
			t.Fatal(err)
		}
	}
	if _, err := e.Do(ctx, r, unexpected); !errors.Is(err, idem.ErrCompletionContext) {
		t.Fatal(err)
	}
	completion, cancel := e.WithCompletion(ctx)
	defer cancel()
	if _, err := e.Do(completion, r, unexpected); !errors.Is(err, idem.ErrNoTransaction) {
		t.Fatal(err)
	}
	if _, _, err := e.Reserve(ctx, r); !errors.Is(err, idem.ErrNoTransaction) {
		t.Fatal(err)
	}
	if _, err := e.DoReserved(ctx, idem.Reservation{}, unexpected); !errors.Is(err, idem.ErrCompletionContext) {
		t.Fatal(err)
	}
	if _, err := e.DoReserved(completion, idem.Reservation{}, unexpected); !errors.Is(err, idem.ErrInvalidRequest) {
		t.Fatal(err)
	}
	if err := e.BindResource(ctx, idem.Reservation{}, idem.Resource{Type: "operation", ID: "1"}); !errors.Is(err, idem.ErrInvalidRequest) {
		t.Fatal(err)
	}
	if err := e.Release(ctx, idem.Reservation{}); err != nil {
		t.Fatal(err)
	}
	if released, err := e.ReleaseIfCurrent(ctx, idem.Reservation{}); err != nil || released {
		t.Fatalf("zero token=%v %v", released, err)
	}
	responses := []idem.Response{{Body: make([]byte, (4<<20)+1)}, {Headers: map[string][]string{"BAD": {"value"}}}, {Headers: map[string][]string{"grpc-status": {"value"}}}, {Headers: map[string][]string{"x-result": {"bad\r\nheader"}}}, {Headers: map[string][]string{"x-result": {strings.Repeat("x", 16385)}}}, {OwnerID: publicForeign}, {OwnerID: "invalid"}, {Resource: idem.Resource{Type: "only"}}, {Resource: idem.Resource{Type: "Invalid", ID: "1"}}, {Resource: idem.Resource{Type: "operation", ID: "unsafe id"}}, {DuplicateRetryAt: time.Now().Add(time.Hour)}}
	for _, response := range responses {
		_, err := consumerDo(ctx, db, e, r, func(ctx context.Context) (idem.Response, error) {
			tx, _ := db.Tx(ctx)
			_, err := tx.Exec(ctx, "INSERT INTO consumer_effects VALUES($1)", publicOwner)
			return response, err
		})
		if !errors.Is(err, idem.ErrInvalidRequest) {
			t.Fatalf("invalid response persisted: %v", err)
		}
	}
	requireEffects(t, ctx, db, 0)
	var count int
	if err := db.pool.QueryRow(ctx, "SELECT count(*) FROM idempotency_keys").Scan(&count); err != nil || count != 0 {
		t.Fatalf("rejected outcomes left keys=%d %v", count, err)
	}
}

func TestAPIConsumerExpiredFencesCannotCompleteOrCompensateNewOwner(t *testing.T) {
	ctx, db, e, now := publicEngine(t)
	r := consumerRequest()
	var old, current idem.Reservation
	reserve := func(target *idem.Reservation) error {
		return db.Run(ctx, func(ctx context.Context) error { var err error; *target, _, err = e.Reserve(ctx, r); return err })
	}
	if err := reserve(&old); err != nil {
		t.Fatal(err)
	}
	ref := idem.Resource{Type: "operation", ID: "original"}
	if err := db.Run(ctx, func(ctx context.Context) error { return e.BindResource(ctx, old, ref) }); err != nil {
		t.Fatal(err)
	}
	if err := db.Run(ctx, func(ctx context.Context) error {
		return e.BindResource(ctx, old, idem.Resource{Type: "operation", ID: "replacement"})
	}); !errors.Is(err, idem.ErrInvalidRequest) {
		t.Fatalf("binding replacement %v", err)
	}
	work, cancel := e.WithCompletion(ctx)
	defer cancel()
	err := db.Run(work, func(ctx context.Context) error {
		_, err := e.DoReserved(ctx, old, consumerEffect(db, publicOwner))
		return err
	})
	if !errors.Is(err, idem.ErrInvalidRequest) {
		t.Fatalf("result replaced binding %v", err)
	}
	requireEffects(t, ctx, db, 0)
	*now = now.Add(16 * time.Second)
	if err := db.Run(ctx, func(ctx context.Context) error {
		released, err := e.ReleaseIfCurrent(ctx, old)
		if released {
			t.Error("expired owner authorized refund")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := reserve(&current); err != nil {
		t.Fatal(err)
	}
	if current == old {
		t.Fatal("new reservation reused fencing token")
	}
	if err := db.Run(ctx, func(ctx context.Context) error { return e.Release(ctx, old) }); err != nil {
		t.Fatal(err)
	}
	err = db.Run(work, func(ctx context.Context) error {
		_, err := e.DoReserved(ctx, old, func(context.Context) (idem.Response, error) {
			t.Error("old owner executed effect")
			return idem.Response{}, nil
		})
		return err
	})
	rest, ok := finerr.ToREST(err)
	if !ok || rest.Code != "REQUEST_IN_PROGRESS" {
		t.Fatalf("old fence %v", err)
	}
	if err := db.Run(ctx, func(ctx context.Context) error {
		return e.BindResource(ctx, current, idem.Resource{Type: "operation", ID: "resource-1"})
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Run(work, func(ctx context.Context) error {
		_, err := e.DoReserved(ctx, current, consumerEffect(db, publicOwner))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	requireEffects(t, ctx, db, 1)
	if err := db.Run(ctx, func(ctx context.Context) error {
		token, replay, err := e.Reserve(ctx, r)
		if token != (idem.Reservation{}) || !replay.Replayed {
			t.Error("completed reservation was reclaimed")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
