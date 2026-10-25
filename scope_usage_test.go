package idempotency

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	userv1 "github.com/fin-assist/contracts/gen/go/finassist/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type countingUsageServer struct {
	*usageServer
	calls atomic.Int32
}

func (s *countingUsageServer) StartPhoneVerification(ctx context.Context, in *userv1.StartPhoneVerificationRequest) (*userv1.StartPhoneVerificationResponse, error) {
	s.calls.Add(1)
	return s.usageServer.StartPhoneVerification(ctx, in)
}

// FIN-296: a malformed scope from the service's identity function is a server
// fault. Through a real gRPC chain the client sees INTERNAL_ERROR, never a
// VALIDATION_ERROR on its key, and the handler never runs or writes a key.
func TestIntegrationUsageMalformedScopeIsInternalBeforeKeyCheck(t *testing.T) {
	e, db, clock := setup(t)
	server := &countingUsageServer{usageServer: &usageServer{e: e, db: db, clock: clock}}
	listener := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(e.UnaryInterceptor(func(context.Context) (string, error) { return "not-a-uuid", nil })))
	userv1.RegisterAuthServiceServer(srv, server)
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///usage", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := userv1.NewAuthServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	in := &userv1.StartPhoneVerificationRequest{Phone: "+79001234567"}
	for name, call := range map[string]context.Context{
		"valid key":   metadata.AppendToOutgoingContext(ctx, "idempotency-key", testKey),
		"missing key": ctx,
	} {
		_, err = client.StartPhoneVerification(call, in)
		if status.Code(err) != codes.Internal {
			t.Fatalf("%s: code=%v err=%v", name, status.Code(err), err)
		}
		assertReason(t, err, "INTERNAL_ERROR")
	}
	if n := server.calls.Load(); n != 0 {
		t.Fatalf("handler ran %d times", n)
	}
	var keys int
	if err = db.pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_keys`).Scan(&keys); err != nil || keys != 0 {
		t.Fatalf("keys=%d err=%v", keys, err)
	}
	assertCount(t, db, 0)
}
