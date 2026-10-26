package idempotency

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	userv1 "github.com/fin-assist/contracts/gen/go/finassist/user/v1"
	"github.com/fin-assist/contracts/gen/go/finerr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// usageServer follows README: metadata -> protobuf hash -> service transaction
// -> Do -> commit -> restore domain error/headers -> generated response.
type usageServer struct {
	userv1.UnimplementedAuthServiceServer
	e     *Engine
	db    *serviceDB
	clock *testClock
}

func (s *usageServer) StartPhoneVerification(ctx context.Context, in *userv1.StartPhoneVerificationRequest) (*userv1.StartPhoneVerificationResponse, error) {
	r, ok := FromContext(ctx)
	if !ok {
		return nil, finerr.InternalError()
	}
	var err error
	r.Hash, err = HashProto(in)
	if err != nil {
		return nil, contractError(err)
	}
	result, err := runDo(s.e, s.db, ctx, r, func(ctx context.Context) (Response, error) {
		if _, err := effect(s.db)(ctx); err != nil {
			return Response{}, err
		}
		if in.GetPhone() == "wrong" {
			return Response{}, finerr.WrongFormat()
		}
		if in.GetPhone() == "wrapped" {
			return Response{}, fmt.Errorf("private SQL users_phone_key: %w", finerr.WrongFormat())
		}
		if in.GetPhone() == "unsendable-header" {
			// A valid body: without FIN-296 the only failure left is grpc.SetHeader.
			body, err := proto.Marshal(&userv1.StartPhoneVerificationResponse{VerificationToken: "opaque", CallbackNumber: "+78001234567", ExpiresAt: timestamppb.New(s.clock.Now().Add(5 * time.Minute))})
			return Response{Body: body, Headers: map[string][]string{"x-result": {"не ASCII"}}}, err
		}
		if in.GetPhone() == "busy" {
			retry := s.clock.Now().Add(time.Minute)
			return Response{DuplicateRetryAt: retry}, finerr.RateLimited(retry)
		}
		out := &userv1.StartPhoneVerificationResponse{VerificationToken: "opaque", CallbackNumber: "+78001234567", ExpiresAt: timestamppb.New(s.clock.Now().Add(5 * time.Minute))}
		body, err := proto.Marshal(out)
		return Response{Body: body, Headers: map[string][]string{"x-result": {"saved"}}}, err
	})
	if err != nil {
		if errors.Is(err, ErrNotStored) {
			return nil, result.Err
		}
		return nil, contractError(err)
	}
	if result.Err != nil {
		return nil, result.Err
	}
	if err = grpc.SetHeader(ctx, metadata.MD(result.Response.Headers)); err != nil {
		return nil, finerr.InternalError()
	}
	out := new(userv1.StartPhoneVerificationResponse)
	if err = proto.Unmarshal(result.Response.Body, out); err != nil {
		return nil, finerr.InternalError()
	}
	remaining := int32(out.GetExpiresAt().AsTime().Sub(s.clock.Now()) / time.Second)
	if remaining < 0 {
		remaining = 0
	}
	out.ExpiresIn = &remaining
	return out, nil
}
func TestIntegrationUsageGeneratedClientAndContractErrors(t *testing.T) {
	e, db, clock := setup(t)
	listener := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(e.UnaryInterceptor(func(context.Context) (string, error) { return "", nil })))
	userv1.RegisterAuthServiceServer(srv, &usageServer{e: e, db: db, clock: clock})
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
	if _, err = client.StartPhoneVerification(ctx, in); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "idempotency-key", testKey)
	var headers metadata.MD
	first, err := client.StartPhoneVerification(ctx, in, grpc.Header(&headers))
	if err != nil || headers.Get("x-result")[0] != "saved" {
		t.Fatal(first, headers, err)
	}
	clock.Add(time.Minute)
	second, err := client.StartPhoneVerification(ctx, in)
	if err != nil || second.GetVerificationToken() != first.GetVerificationToken() || second.GetExpiresIn() != 240 {
		t.Fatal(second, err)
	}
	assertCount(t, db, 1)
	_, err = client.StartPhoneVerification(ctx, &userv1.StartPhoneVerificationRequest{Phone: "different"})
	assertReason(t, err, "IDEMPOTENCY_CONFLICT")
	if status.Code(err) != codes.AlreadyExists {
		t.Fatal(err)
	}
	other := metadata.NewOutgoingContext(ctx, metadata.Pairs("idempotency-key", "66666666-6666-4666-8666-666666666666"))
	for range 2 {
		_, err = client.StartPhoneVerification(other, &userv1.StartPhoneVerificationRequest{Phone: "wrong"})
		assertReason(t, err, "WRONG_FORMAT")
		if status.Code(err) != codes.FailedPrecondition {
			t.Fatal(err)
		}
	}
	assertCount(t, db, 2)
	// A header grpc.SetHeader would reject must roll back with the effect instead
	// of committing an outcome no attempt can ever deliver.
	unsendable := metadata.NewOutgoingContext(ctx, metadata.Pairs("idempotency-key", "88888888-8888-4888-8888-888888888888"))
	for range 2 {
		_, err = client.StartPhoneVerification(unsendable, &userv1.StartPhoneVerificationRequest{Phone: "unsendable-header"})
		assertReason(t, err, "INTERNAL_ERROR")
	}
	assertCount(t, db, 2)
	var stored int
	if err = db.pool.QueryRow(ctx, "SELECT count(*) FROM idempotency_keys WHERE key='88888888-8888-4888-8888-888888888888'").Scan(&stored); err != nil || stored != 0 {
		t.Fatal("unsendable outcome stored:", stored, err)
	}
	token := reserve(t, e, db, Request{Endpoint: "/finassist.user.v1.AuthService/StartPhoneVerification", Key: "77777777-7777-4777-8777-777777777777", Hash: func() [32]byte { h, _ := HashProto(in); return h }()})
	_ = token
	pending := metadata.NewOutgoingContext(ctx, metadata.Pairs("idempotency-key", "77777777-7777-4777-8777-777777777777"))
	_, err = client.StartPhoneVerification(pending, in)
	assertReason(t, err, "REQUEST_IN_PROGRESS")
	if status.Code(err) != codes.Aborted {
		t.Fatal(err)
	}
	assertCount(t, db, 2)
}

func TestIntegrationUsageBusyPhoneRateLimitedRebuildsRetryInfo(t *testing.T) {
	e, db, clock := setup(t)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(e.UnaryInterceptor(func(context.Context) (string, error) { return "", nil })))
	userv1.RegisterAuthServiceServer(srv, &usageServer{e: e, db: db, clock: clock})
	client := userv1.NewAuthServiceClient(dialUsage(t, srv))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "idempotency-key", testKey)
	retry := clock.Now().Add(time.Minute)
	var firstDelay time.Duration
	for i := range 2 {
		before := time.Now()
		_, err := client.StartPhoneVerification(ctx, &userv1.StartPhoneVerificationRequest{Phone: "busy"})
		rest, ok := finerr.ToREST(err)
		if !ok || rest.Code != "RATE_LIMITED" || status.Code(err) != codes.ResourceExhausted || rest.Metadata["retry_at"] != retry.Format(time.RFC3339) || rest.RetryAfter <= 0 || rest.RetryAfter > retry.Sub(before)+time.Second {
			t.Fatal(rest, err)
		}
		if i == 0 {
			firstDelay = rest.RetryAfter
			// Replay ages against the service's injected clock, independently of wall time.
			clock.Add(20 * time.Second)
		} else if rest.RetryAfter >= firstDelay {
			t.Fatal("RetryInfo was not recomputed", rest.RetryAfter, firstDelay)
		}
	}
	assertCount(t, db, 1)
}

func TestIntegrationUsageWrappedContractErrorSanitized(t *testing.T) {
	e, db, clock := setup(t)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(e.UnaryInterceptor(func(context.Context) (string, error) { return "", nil })))
	userv1.RegisterAuthServiceServer(srv, &usageServer{e: e, db: db, clock: clock})
	client := userv1.NewAuthServiceClient(dialUsage(t, srv))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "idempotency-key", testKey)
	for range 2 {
		_, err := client.StartPhoneVerification(ctx, &userv1.StartPhoneVerificationRequest{Phone: "wrapped"})
		assertReason(t, err, "WRONG_FORMAT")
		if status.Convert(err).Message() != "WRONG_FORMAT" {
			t.Fatal("wrapper leaked", err)
		}
	}
	assertCount(t, db, 1)
}

func TestUnitServiceTransactionErrorMappingDropsRollbackText(t *testing.T) {
	original := finerr.RequestInProgress(time.Second)
	for _, err := range []error{
		fmt.Errorf("private transaction details: %w", original),
		errors.Join(original, errors.New("private rollback SQL users_phone_key")),
	} {
		mapped := contractError(err)
		assertReason(t, mapped, "REQUEST_IN_PROGRESS")
		if status.Code(mapped) != codes.Aborted || status.Convert(mapped).Message() != "REQUEST_IN_PROGRESS" {
			t.Fatal(mapped)
		}
	}
	if status.Code(contractError(errors.New("private SQL"))) != codes.Internal {
		t.Fatal("unknown error exposed")
	}
}
