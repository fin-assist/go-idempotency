package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/fin-assist/contracts/gen/go/finerr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

const testMethod = "/test.Service/Create"
const testKey = "11111111-1111-4111-8111-111111111111"

type noTx struct{}

func (noTx) Tx(context.Context) (pgx.Tx, bool) { return nil, false }
func unitEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := New(noTx{}, Options{Methods: map[string]Policy{testMethod: {}}, Clock: time.Now, Registerer: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func TestNewValidation(t *testing.T) {
	good := func() Options {
		return Options{Methods: map[string]Policy{testMethod: {}}, Clock: time.Now, Registerer: prometheus.NewRegistry()}
	}
	for _, tc := range []struct {
		name   string
		change func(*Options)
	}{
		{"methods", func(o *Options) { o.Methods = nil }}, {"clock", func(o *Options) { o.Clock = nil }}, {"registry", func(o *Options) { o.Registerer = nil }},
		{"method", func(o *Options) { o.Methods = map[string]Policy{"bad": {}} }}, {"ttl", func(o *Options) { o.Methods[testMethod] = Policy{TTL: -1} }},
		{"cipher", func(o *Options) { o.Methods[testMethod] = Policy{Encrypt: true} }}, {"refresh", func(o *Options) { o.Methods = map[string]Policy{"/x.S/RefreshTokens": {}} }},
		{"auth secret", func(o *Options) { o.Methods = map[string]Policy{"/finassist.user.v1.AuthService/Register": {}} }},
		{"lock", func(o *Options) { o.LockTimeout = time.Nanosecond }}, {"effect", func(o *Options) { o.EffectTimeout = -1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := good()
			tc.change(&o)
			if _, err := New(noTx{}, o); !errors.Is(err, ErrInvalidRequest) {
				t.Fatal(err)
			}
		})
	}
	o := good()
	if _, err := New(nil, o); err == nil {
		t.Fatal("nil tx accepted")
	}
	e, err := New(noTx{}, o)
	if err != nil {
		t.Fatal(err)
	}
	o.Methods[testMethod] = Policy{TTL: time.Second}
	if e.methods[testMethod].TTL != 24*time.Hour {
		t.Fatal("policy not copied")
	}
	if _, err = New(noTx{}, o); err == nil {
		t.Fatal("duplicate metrics accepted")
	}
	if _, err = NewCipher([]byte("short")); err == nil {
		t.Fatal("short key accepted")
	}
}
func TestHashAndMetadata(t *testing.T) {
	a, _ := structpb.NewStruct(map[string]any{"b": 2, "a": 1})
	b, _ := structpb.NewStruct(map[string]any{"a": 1, "b": 2})
	ha, err := HashProto(a)
	if err != nil {
		t.Fatal(err)
	}
	hb, _ := HashProto(b)
	if ha != hb {
		t.Fatal("map order changes hash")
	}
	if _, err = HashProto(nil); err == nil {
		t.Fatal("nil proto accepted")
	}
	hash, err := HashFile(strings.NewReader("abc"), 3)
	if err != nil || hash != sha256.Sum256([]byte("abc")) {
		t.Fatal(hash, err)
	}
	if _, err = HashFile(strings.NewReader("abcd"), 3); status.Code(err) != codes.OutOfRange {
		t.Fatal(err)
	}
	if _, err = HashFile(nil, 0); err == nil {
		t.Fatal("nil reader accepted")
	}
	if _, err = HashFile(brokenReader{}, 4); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	e := unitEngine(t)
	calls := 0
	next := func(ctx context.Context, _ any) (any, error) {
		calls++
		r, ok := FromContext(ctx)
		if !ok || r.Key != testKey || r.Endpoint != testMethod {
			t.Fatal(r, ok)
		}
		return "ok", nil
	}
	interceptor := e.UnaryInterceptor(func(context.Context) (string, error) { return "", nil })
	for _, keys := range [][]string{nil, {"bad"}, {testKey, testKey}} {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{"idempotency-key": keys})
		if _, err = interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: testMethod}, next); status.Code(err) != codes.InvalidArgument {
			t.Fatal(err)
		}
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("idempotency-key", testKey))
	if _, err = interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: testMethod}, next); err != nil || calls != 1 {
		t.Fatal(err, calls)
	}
	fail := e.UnaryInterceptor(func(context.Context) (string, error) { return "", finerr.InvalidToken() })
	if _, err = fail(ctx, nil, &grpc.UnaryServerInfo{FullMethod: testMethod}, next); status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
	fail = e.UnaryInterceptor(func(context.Context) (string, error) { return "forged", nil })
	if _, err = fail(ctx, nil, &grpc.UnaryServerInfo{FullMethod: testMethod}, next); err == nil {
		t.Fatal("forged scope accepted")
	}
	called := false
	_, err = interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/other/Read"}, func(context.Context, any) (any, error) { called = true; return nil, nil })
	if err != nil || !called {
		t.Fatal(err)
	}
}

type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestCodecPoliciesAndEncryption(t *testing.T) {
	e := unitEngine(t)
	r := Request{Scope: nilScope, Endpoint: testMethod, Key: testKey}
	validation := finerr.ValidationError(&errdetails.BadRequest_FieldViolation{Field: "x", Reason: "REQUIRED"})
	for _, domain := range []error{nil, validation, finerr.FileTooLarge(), finerr.WrongFormat()} {
		for _, encrypted := range []bool{false, true} {
			e.cipher, _ = NewCipher(make([]byte, 32))
			p := Policy{TTL: time.Hour, Encrypt: encrypted}
			e.methods[testMethod] = p
			result := Result{Response: Response{Body: []byte("secret"), Headers: map[string][]string{"x-test": {"v"}}}, Err: domain}
			b, expires, err := e.encode(r, p, result)
			if err != nil || !expires.After(e.now()) {
				t.Fatal(err)
			}
			if encrypted && bytes.Contains(b, []byte("secret")) {
				t.Fatal("plaintext persisted")
			}
			decoded, err := e.decode(r, record{payload: b, encrypted: encrypted})
			if err != nil || !bytes.Equal(decoded.Response.Body, result.Response.Body) || status.Code(decoded.Err) != status.Code(domain) {
				t.Fatal(decoded, err)
			}
			if encrypted {
				b[len(b)-1] ^= 1
				if _, err = e.decode(r, record{payload: b, encrypted: true}); !errors.Is(err, ErrStorage) {
					t.Fatal("tampering accepted", err)
				}
				b, _, _ = e.encode(r, p, result)
				other := r
				other.Key = "22222222-2222-4222-8222-222222222222"
				if _, err = e.decode(other, record{payload: b, encrypted: true}); err == nil {
					t.Fatal("ciphertext transplant accepted")
				}
			}
		}
	}
	for _, domain := range []error{finerr.InvalidToken(), finerr.ConsentRequired(), finerr.NotFound(), finerr.IdempotencyConflict(), finerr.RateLimited(time.Now().Add(time.Hour)), errors.New("secret sql"), finerr.InternalError()} {
		if _, _, err := e.encode(r, Policy{TTL: time.Hour}, Result{Err: domain}); !errors.Is(err, ErrNotStored) {
			t.Fatal(domain, err)
		}
	}
	for _, response := range []Response{{Body: make([]byte, maxBody+1)}, {Headers: map[string][]string{"bad\n": {"x"}}}, {Headers: map[string][]string{"grpc-test": {"x"}}}, {Headers: map[string][]string{"x": {"bad\n"}}}, {Headers: map[string][]string{"x": {strings.Repeat("x", 16385)}}}} {
		if _, _, err := e.encode(r, Policy{}, Result{Response: response}); err == nil {
			t.Fatal("oversized/invalid response accepted")
		}
	}
	for _, b := range [][]byte{nil, []byte("broken"), make([]byte, maxPayload+1)} {
		if _, err := e.decode(r, record{payload: b}); err == nil {
			t.Fatal("corrupt accepted")
		}
	}
}

func TestPreconditionsAndStorageErrors(t *testing.T) {
	e := unitEngine(t)
	r := Request{Endpoint: testMethod, Key: testKey}
	ctx, cancel := e.WithCompletion(context.Background())
	defer cancel()
	effect := func(context.Context) (Response, error) { t.Fatal("effect called"); return Response{}, nil }
	if _, err := e.Do(context.Background(), r, effect); !errors.Is(err, ErrCompletionContext) {
		t.Fatal(err)
	}
	if _, err := e.Do(ctx, r, effect); !errors.Is(err, ErrNoTransaction) {
		t.Fatal(err)
	}
	if _, _, err := e.Reserve(ctx, r); !errors.Is(err, ErrNoTransaction) {
		t.Fatal(err)
	}
	if _, err := e.DoReserved(ctx, Reservation{}, effect); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	if err := e.Release(ctx, Reservation{}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	if _, err := e.Cleanup(ctx, 0); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	if _, err := e.Cleanup(ctx, 1); !errors.Is(err, ErrNoTransaction) {
		t.Fatal(err)
	}
	for _, input := range []error{context.Canceled, context.DeadlineExceeded, pgx.ErrNoRows} {
		if !errors.Is(storageError(input), input) {
			t.Fatal(input)
		}
	}
	err := storageError(&pgconn.PgError{Code: "55P03", Message: "secret"})
	rest, ok := finerr.ToREST(err)
	if !ok || rest.Code != "REQUEST_IN_PROGRESS" || rest.RetryAfter <= 0 {
		t.Fatal(err)
	}
	if !errors.Is(storageError(errors.New("private SQL")), ErrStorage) {
		t.Fatal("storage leak")
	}
}

func TestCodecInvalidOutcomesFailClosed(t *testing.T) {
	e := unitEngine(t)
	r := Request{Scope: nilScope, Endpoint: testMethod, Key: testKey}
	p := Policy{TTL: time.Hour}
	retry := time.Now().Add(time.Hour)
	for _, result := range []Result{
		{Response: Response{DuplicateRetryAt: retry}},
		{Response: Response{DuplicateRetryAt: retry}, Err: finerr.RateLimited(retry)},
		{Response: Response{DuplicateRetryAt: retry}, Err: finerr.WrongFormat()},
	} {
		if _, _, err := e.encode(r, p, result); !errors.Is(err, ErrInvalidRequest) {
			t.Fatal(err)
		}
	}
	invalidStatus := finerr.ValidationError(&errdetails.BadRequest_FieldViolation{Field: string([]byte{255}), Reason: "INVALID_FORMAT"})
	if _, _, err := e.encode(r, p, Result{Err: invalidStatus}); !errors.Is(err, ErrNotStored) {
		t.Fatal(err)
	}
	e.methods[testMethod] = p
	for _, payload := range []string{"broken", `{"Status":"AQ=="}`, `{"Status":"CA4="}`, `{"Response":{"Headers":{"BAD":["v"]}}}`} {
		if _, err := e.decode(r, record{payload: []byte(payload)}); !errors.Is(err, ErrStorage) {
			t.Fatal(payload, err)
		}
	}
	e.methods[testMethod] = Policy{Encrypt: true}
	if _, err := e.decode(r, record{encrypted: true}); !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
	if _, err := HashProto(structpb.NewStringValue(strings.Repeat("x", maxBody+1))); err == nil {
		t.Fatal("oversize proto accepted")
	}
	if _, err := HashProto(structpb.NewStringValue(string([]byte{255}))); err == nil {
		t.Fatal("invalid utf8 accepted")
	}
}

type unavailableRow struct{}

func (unavailableRow) Scan(...any) error { return errors.New("private database failure") }

type unavailableTx struct{ pgx.Tx }

func (unavailableTx) QueryRow(context.Context, string, ...any) pgx.Row { return unavailableRow{} }
func (unavailableTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("private database failure")
}

type unavailableTransactions struct{}

func (unavailableTransactions) Tx(context.Context) (pgx.Tx, bool) { return unavailableTx{}, true }
func TestDatabaseUnavailabilityNeverRunsAnEffect(t *testing.T) {
	e := unitEngine(t)
	e.txs = unavailableTransactions{}
	r := request()
	token := Reservation{request: r, id: "owner"}
	ctx, cancel := e.WithCompletion(context.Background())
	defer cancel()
	fn := func(context.Context) (Response, error) { t.Fatal("effect ran without storage"); return Response{}, nil }
	if _, err := e.Do(ctx, r, fn); !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
	if _, _, err := e.Reserve(ctx, r); !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
	if _, err := e.DoReserved(ctx, token, fn); !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
	if err := e.Release(ctx, token); !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
	if _, err := e.Cleanup(ctx, 10); !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
	parent, stop := context.WithCancel(context.Background())
	stop()
	cancelled, finish := e.WithCompletion(parent)
	defer finish()
	if _, err := e.Do(cancelled, r, fn); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := e.DoReserved(cancelled, token, fn); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := e.Do(ctx, Request{}, fn); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	if _, err := e.DoReserved(context.Background(), token, fn); !errors.Is(err, ErrCompletionContext) {
		t.Fatal(err)
	}
	if _, _, err := e.Reserve(ctx, Request{}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	e.txs = noTx{}
	if _, err := e.DoReserved(ctx, token, fn); !errors.Is(err, ErrNoTransaction) {
		t.Fatal(err)
	}
	if err := e.Release(ctx, token); !errors.Is(err, ErrNoTransaction) {
		t.Fatal(err)
	}
}
