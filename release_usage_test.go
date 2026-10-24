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
)

// This service exercises the documented compensation transaction via a generated
// client. A peer refusal is intentionally not cached; the same key can retry.
type releaseUsageServer struct {
	userv1.UnimplementedAuthServiceServer
	e      *Engine
	db     *serviceDB
	reject atomic.Bool
}

func (s *releaseUsageServer) StartPhoneVerification(ctx context.Context, in *userv1.StartPhoneVerificationRequest) (*userv1.StartPhoneVerificationResponse, error) {
	r, ok := FromContext(ctx)
	if !ok {
		return nil, finerr.InternalError()
	}
	var err error
	r.Hash, err = HashProto(in)
	if err != nil {
		return nil, finerr.InternalError()
	}
	var token Reservation
	var result Result
	err = s.db.Run(ctx, func(ctx context.Context) error {
		var err error
		token, result, err = s.e.Reserve(ctx, r)
		if err != nil || result.Replayed {
			return err
		}
		_, err = effect(s.db)(ctx) // Reserve one service-owned budget slot atomically.
		return err
	})
	if err != nil {
		return nil, contractError(err)
	}
	if result.Replayed {
		return &userv1.StartPhoneVerificationResponse{VerificationToken: "test-secret", CallbackNumber: "+74951234567"}, result.Err
	}
	if s.reject.Load() {
		var open int
		if err = s.db.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state='idle in transaction'`).Scan(&open); err != nil || open != 0 {
			return nil, finerr.InternalError()
		}
		// This is where the service calls its provider, outside a transaction.
		err = s.db.Run(ctx, func(ctx context.Context) error {
			yes, err := s.e.ReleaseIfCurrent(ctx, token)
			if err != nil {
				return err
			}
			if !yes {
				return finerr.RequestInProgress(time.Second)
			}
			tx, _ := s.db.Tx(ctx)
			_, err = tx.Exec(ctx, `DELETE FROM effects`)
			return err
		})
		if err != nil {
			return nil, contractError(err)
		}
		return nil, finerr.InternalError()
	}
	result, err = finalize(s.e, s.db, token, func(context.Context) (Response, error) { return Response{Body: []byte("saved")}, nil })
	if err != nil {
		return nil, contractError(err)
	}
	if result.Err != nil {
		return nil, result.Err
	}
	return &userv1.StartPhoneVerificationResponse{VerificationToken: "test-secret", CallbackNumber: "+74951234567"}, nil
}

func TestIntegrationUsageConditionalReleaseAllowsSameKeyRetryAfterCompensation(t *testing.T) {
	e, db, _ := setup(t)
	handler := &releaseUsageServer{e: e, db: db}
	handler.reject.Store(true)
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(e.UnaryInterceptor(func(context.Context) (string, error) { return "", nil })))
	userv1.RegisterAuthServiceServer(server, handler)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///release", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "idempotency-key", testKey)
	client := userv1.NewAuthServiceClient(conn)
	in := &userv1.StartPhoneVerificationRequest{Phone: "+79001234567"}
	if _, err = client.StartPhoneVerification(ctx, in); err == nil {
		t.Fatal("peer refusal succeeded")
	}
	assertCount(t, db, 0)
	var count int
	if err = db.pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_keys`).Scan(&count); err != nil || count != 0 {
		t.Fatal("rejection cached", count, err)
	}
	handler.reject.Store(false)
	out, err := client.StartPhoneVerification(ctx, in)
	if err != nil || out.GetVerificationToken() == "" {
		t.Fatal(out, err)
	}
	assertCount(t, db, 1)
	handler.reject.Store(true)
	if out, err = client.StartPhoneVerification(ctx, in); err != nil || out.GetVerificationToken() == "" {
		t.Fatal("replay called provider", out, err)
	}
	assertCount(t, db, 1)
	// Successful completion prevents any later compensation by its old token.
	r := request()
	r.Endpoint = userv1.AuthService_StartPhoneVerification_FullMethodName
	r.Hash, err = HashProto(in)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Run(ctx, func(ctx context.Context) error {
		_, result, err := e.Reserve(ctx, r)
		if err != nil {
			return err
		}
		if !result.Replayed {
			return errors.New("success was not cached")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
