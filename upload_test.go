package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/fin-assist/contracts/gen/go/finerr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestUnitHashUploadFollowsContractFormula(t *testing.T) {
	content := sha256.Sum256([]byte("OFXHEADER:100"))
	// grpc-api.md 6: SHA-256(SHA-256(content) ‖ file_name), no separator.
	want := sha256.Sum256(append(content[:], []byte("Выписка.ofx")...))
	if got := HashUpload(content, "Выписка.ofx"); got != want {
		t.Fatalf("hash=%x want=%x", got, want)
	}
	// A fixed vector pins the formula across releases: a change would make
	// replays that cross a deploy conflict falsely.
	vector := HashUpload(sha256.Sum256([]byte("abc")), "a.ofx")
	if hex.EncodeToString(vector[:]) != "d9fb384452d7a77b146b8b18cf9aa78f81f5b0e02b1d35e62b8fd1c26441b4d7" {
		t.Fatalf("vector changed: %x", vector)
	}
	if HashUpload(content, "") != sha256.Sum256(content[:]) {
		t.Fatal("empty name must add no bytes")
	}
	// The name is used exactly as received: case, Unicode form and spaces matter.
	names := []string{"a.ofx", "A.ofx", "a.ofx ", "\u0439.ofx", "\u0438\u0306.ofx"}
	seen := map[[32]byte]string{}
	for _, name := range names {
		h := HashUpload(content, name)
		if other, dup := seen[h]; dup {
			t.Fatalf("%q and %q share a hash", name, other)
		}
		seen[h] = name
	}
	if HashUpload(sha256.Sum256([]byte("other")), "a.ofx") == HashUpload(content, "a.ofx") {
		t.Fatal("content ignored")
	}
}

type contextStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s contextStream) Context() context.Context { return s.ctx }

func TestUnitStreamInterceptorMatchesUnaryRules(t *testing.T) {
	e := unitEngine(t)
	info := &grpc.StreamServerInfo{FullMethod: testMethod, IsClientStream: true}
	calls := 0
	next := func(_ any, ss grpc.ServerStream) error {
		calls++
		r, ok := FromContext(ss.Context())
		if !ok || r.Key != testKey || r.Endpoint != testMethod || r.Scope != "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" || r.Hash != ([32]byte{}) {
			t.Fatal(r, ok)
		}
		// The original metadata stays visible to the handler.
		if md, _ := metadata.FromIncomingContext(ss.Context()); md.Get("idempotency-key")[0] != testKey {
			t.Fatal(md)
		}
		return nil
	}
	user := func(context.Context) (string, error) { return "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA", nil }
	interceptor := e.StreamInterceptor(user)
	for _, keys := range [][]string{nil, {"bad"}, {testKey, testKey}} {
		ss := contextStream{ctx: metadata.NewIncomingContext(context.Background(), metadata.MD{"idempotency-key": keys})}
		err := interceptor(nil, ss, info, next)
		if rest, ok := finerr.ToREST(err); !ok || rest.Code != "VALIDATION_ERROR" || status.Code(err) != codes.InvalidArgument {
			t.Fatal(keys, err)
		}
	}
	if calls != 0 {
		t.Fatal("handler ran for an invalid key")
	}
	valid := contextStream{ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs("idempotency-key", "11111111-1111-4111-8111-111111111111"))}
	if err := interceptor(nil, valid, info, next); err != nil || calls != 1 {
		t.Fatal(err, calls)
	}
	if err := e.StreamInterceptor(func(context.Context) (string, error) { return "", finerr.InvalidToken() })(nil, valid, info, next); status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
	// A malformed scope is a server error even when the key is also missing.
	forged := e.StreamInterceptor(func(context.Context) (string, error) { return "forged", nil })
	if err := forged(nil, contextStream{ctx: context.Background()}, info, next); status.Code(err) != codes.Internal {
		t.Fatal(err)
	}
	if err := e.StreamInterceptor(nil)(nil, valid, info, next); !errors.Is(err, ErrInvalidRequest) {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("handler ran after a refusal:", calls)
	}
	// Other methods pass through with the original stream and no Request.
	var passed grpc.ServerStream
	plain := contextStream{ctx: context.Background()}
	err := interceptor(nil, plain, &grpc.StreamServerInfo{FullMethod: "/other/Watch"}, func(_ any, ss grpc.ServerStream) error {
		passed = ss
		_, ok := FromContext(ss.Context())
		if ok {
			t.Fatal("request outside the allowlist")
		}
		return nil
	})
	if err != nil || passed != plain {
		t.Fatal(err, passed)
	}
}
