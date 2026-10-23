package idempotency

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	userv1 "github.com/fin-assist/contracts/gen/go/finassist/user/v1"
	"github.com/fin-assist/contracts/gen/go/finerr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type ownerUsageServer struct {
	userv1.UnimplementedAuthServiceServer
	e       *Engine
	db      *serviceDB
	deleted atomic.Bool
}

func (s *ownerUsageServer) Register(ctx context.Context, in *userv1.RegisterRequest) (*userv1.RegisterResponse, error) {
	request, ok := FromContext(ctx)
	if !ok {
		return nil, finerr.InternalError()
	}
	var err error
	request.Hash, err = HashProto(in)
	if err != nil {
		return nil, finerr.InternalError()
	}
	work, cancel := s.e.WithCompletion(ctx)
	defer cancel()
	var result Result
	err = s.db.Run(work, func(ctx context.Context) error {
		var doErr error
		result, doErr = s.e.Do(ctx, request, func(ctx context.Context) (Response, error) {
			if _, err := effect(s.db)(ctx); err != nil {
				return Response{}, err
			}
			// The account is discovered only after verifying the registration secret.
			if in.GetRegistrationToken() != "verified-secret" {
				return Response{}, finerr.InvalidToken()
			}
			body, err := proto.Marshal(&userv1.RegisterResponse{UserId: resultOwner, Tokens: &userv1.TokenPair{AccessToken: "access-secret", RefreshToken: "refresh-secret"}})
			return Response{Body: body, OwnerID: resultOwner}, err
		})
		if doErr != nil {
			return doErr
		}
		if result.Replayed && result.Err == nil {
			// Production uses its durable tombstone guard in this same transaction.
			if result.Response.OwnerID != resultOwner || s.deleted.Load() {
				return finerr.InvalidToken()
			}
		}
		return nil
	})
	if errors.Is(err, ErrNotStored) {
		return nil, result.Err
	}
	if err != nil {
		return nil, contractError(err)
	}
	if result.Err != nil {
		return nil, result.Err
	}
	out := new(userv1.RegisterResponse)
	if err = proto.Unmarshal(result.Response.Body, out); err != nil {
		return nil, finerr.InternalError()
	}
	return out, nil
}

func TestUsageNoUserOwnerGuardsCachedSecrets(t *testing.T) {
	e, db, _ := setup(t)
	e.methods[userv1.AuthService_Register_FullMethodName] = Policy{TTL: 24 * time.Hour, Encrypt: true}
	handler := &ownerUsageServer{e: e, db: db}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(e.UnaryInterceptor(func(context.Context) (string, error) { return "", nil })))
	userv1.RegisterAuthServiceServer(server, handler)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///owned-auth", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := userv1.NewAuthServiceClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "idempotency-key", testKey)
	in := &userv1.RegisterRequest{RegistrationToken: "verified-secret", PdConsentAccepted: true, PdConsentVersion: "v1"}
	first, err := client.Register(ctx, in)
	if err != nil || first.GetUserId() != resultOwner || first.GetTokens().GetRefreshToken() != "refresh-secret" {
		t.Fatal(first, err)
	}
	second, err := client.Register(ctx, in)
	if err != nil || !proto.Equal(first, second) {
		t.Fatal(second, err)
	}
	assertCount(t, db, 1)
	handler.deleted.Store(true)
	for range 2 {
		out, err := client.Register(ctx, in)
		assertReason(t, err, "INVALID_TOKEN")
		if out != nil {
			t.Fatal("deleted owner received cached credentials")
		}
	}
	assertCount(t, db, 1)
	var owner string
	if err = db.pool.QueryRow(t.Context(), `SELECT owner_id::text FROM idempotency_keys WHERE scope=$1`, nilScope).Scan(&owner); err != nil || owner != resultOwner {
		t.Fatal(owner, err)
	}
}
