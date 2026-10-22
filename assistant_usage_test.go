package idempotency

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	assistantv1 "github.com/fin-assist/contracts/gen/go/finassist/assistant/v1"
	userv1 "github.com/fin-assist/contracts/gen/go/finassist/user/v1"
	"github.com/fin-assist/contracts/gen/go/finerr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

const askMethod = "/finassist.assistant.v1.AssistantService/AskQuestion"
const userA = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
const userB = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

type verifiedScopeKey struct{}

type consentPeer struct {
	userv1.UnimplementedUserInternalServiceServer
	db      *serviceDB
	mode    atomic.Int32
	calls   atomic.Int32
	waiting chan struct{}
}

func (s *consentPeer) GetAssistantConsentStatus(ctx context.Context, in *userv1.GetAssistantConsentStatusRequest) (*userv1.GetAssistantConsentStatusResponse, error) {
	s.calls.Add(1)
	// The reservation is visible on another connection, and there is no open
	// transaction while checking the remote consent service.
	var committed bool
	var open int
	if err := s.db.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM idempotency_keys WHERE scope=$1 AND NOT done)`, in.GetUserId()).Scan(&committed); err != nil || !committed {
		return nil, finerr.InternalError()
	}
	if err := s.db.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state='idle in transaction'`).Scan(&open); err != nil || open != 0 {
		return nil, finerr.InternalError()
	}
	if s.mode.Load() == 2 {
		close(s.waiting)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &userv1.GetAssistantConsentStatusResponse{Consent: &userv1.AssistantConsent{Granted: s.mode.Load() == 0}}, nil
}

type assistantUsageServer struct {
	assistantv1.UnimplementedAssistantServiceServer
	e         *Engine
	db        *serviceDB
	consent   userv1.UserInternalServiceClient
	completed chan struct{}
}

func (s *assistantUsageServer) AskQuestion(ctx context.Context, in *assistantv1.AskQuestionRequest) (out *assistantv1.AskQuestionResponse, rpcErr error) {
	var token Reservation
	var result Result
	var err error
	committed := false
	defer func() {
		if !committed && token != (Reservation{}) {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			defer cancel()
			if releaseErr := s.db.Run(cleanup, func(txCtx context.Context) error { return s.e.Release(txCtx, token) }); releaseErr != nil {
				rpcErr = finerr.InternalError()
			}
		}
		s.completed <- struct{}{}
	}()
	r, ok := FromContext(ctx)
	if !ok {
		return nil, finerr.InternalError()
	}
	r.Hash, err = HashProto(in)
	if err != nil {
		return nil, finerr.InternalError()
	}
	err = s.db.Run(ctx, func(txCtx context.Context) error {
		var err error
		token, result, err = s.e.Reserve(txCtx, r)
		return err
	})
	if err != nil {
		return nil, contractError(err)
	}
	if !result.Replayed {
		consent, err := s.consent.GetAssistantConsentStatus(ctx, &userv1.GetAssistantConsentStatusRequest{UserId: &r.Scope})
		if err != nil {
			return nil, contractError(err)
		}
		if !consent.GetConsent().GetGranted() {
			return nil, finerr.ConsentRequired()
		}
		completion, cancel := s.e.WithCompletion(ctx)
		defer cancel()
		err = s.db.Run(completion, func(txCtx context.Context) error {
			var err error
			result, err = s.e.DoReserved(txCtx, token, func(effectCtx context.Context) (Response, error) {
				if _, err := effect(s.db)(effectCtx); err != nil {
					return Response{}, err
				}
				if in.GetText() == "limit" {
					return Response{}, finerr.LimitExceeded(time.Now().Add(time.Hour))
				}
				if in.GetText() == "sql" {
					return Response{}, errors.New("private SQL users_phone_key")
				}
				body, err := proto.Marshal(&assistantv1.AskQuestionResponse{AnswerId: "answer-" + r.Scope, RemainingLimit: 9})
				return Response{Body: body}, err
			})
			return err
		})
		if err != nil {
			if errors.Is(err, ErrNotStored) {
				return nil, result.Err
			}
			return nil, contractError(err)
		}
		committed = true
	}
	if result.Err != nil {
		return nil, result.Err
	}
	out = new(assistantv1.AskQuestionResponse)
	if err = proto.Unmarshal(result.Response.Body, out); err != nil {
		return nil, finerr.InternalError()
	}
	return out, nil
}
func contractError(err error) error {
	if _, ok := finerr.ToREST(err); ok {
		var source interface{ GRPCStatus() *status.Status }
		if errors.As(err, &source) {
			return source.GRPCStatus().Err()
		}
	}
	return finerr.InternalError()
}
func dialUsage(t *testing.T, srv *grpc.Server) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///usage", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
func TestUsageAskQuestionReservationConsentReleaseAndScopes(t *testing.T) {
	e, db, _ := setup(t)
	peer := &consentPeer{db: db, waiting: make(chan struct{})}
	remote := grpc.NewServer()
	userv1.RegisterUserInternalServiceServer(remote, peer)
	handler := &assistantUsageServer{e: e, db: db, consent: userv1.NewUserInternalServiceClient(dialUsage(t, remote)), completed: make(chan struct{}, 20)}
	// A test identity verifier maps fixed bearer fixtures to verified identities;
	// the idempotency scope adapter only reads its context value.
	identity := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		tokens := md.Get("authorization")
		if len(tokens) != 1 {
			return nil, finerr.InvalidToken()
		}
		var user string
		switch tokens[0] {
		case "Bearer a":
			user = userA
		case "Bearer b":
			user = userB
		default:
			return nil, finerr.InvalidToken()
		}
		return next(context.WithValue(ctx, verifiedScopeKey{}, user), req)
	}
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(identity, e.UnaryInterceptor(func(ctx context.Context) (string, error) {
		user, ok := ctx.Value(verifiedScopeKey{}).(string)
		if !ok {
			return "", finerr.InvalidToken()
		}
		return user, nil
	})))
	assistantv1.RegisterAssistantServiceServer(server, handler)
	client := assistantv1.NewAssistantServiceClient(dialUsage(t, server))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	outgoing := func(key, user string) context.Context {
		return metadata.NewOutgoingContext(ctx, metadata.Pairs("idempotency-key", key, "authorization", "Bearer "+user))
	}
	done := func() {
		t.Helper()
		select {
		case <-handler.completed:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	in := &assistantv1.AskQuestionRequest{Text: "question"}
	first, err := client.AskQuestion(outgoing(testKey, "a"), in)
	done()
	if err != nil || first.GetRemainingLimit() != 9 {
		t.Fatal(first, err)
	}
	second, err := client.AskQuestion(outgoing(testKey, "a"), in)
	done()
	if err != nil || !proto.Equal(first, second) || peer.calls.Load() != 1 {
		t.Fatal(second, err, peer.calls.Load())
	}
	assertCount(t, db, 1)
	foreign, err := client.AskQuestion(outgoing(testKey, "b"), in)
	done()
	if err != nil || foreign.GetAnswerId() == first.GetAnswerId() || peer.calls.Load() != 2 {
		t.Fatal(foreign, err)
	}
	assertCount(t, db, 2)
	key := "88888888-8888-4888-8888-888888888888"
	peer.mode.Store(1)
	_, err = client.AskQuestion(outgoing(key, "a"), in)
	done()
	assertReason(t, err, "CONSENT_REQUIRED")
	assertCount(t, db, 2)
	peer.mode.Store(0)
	if _, err = client.AskQuestion(outgoing(key, "a"), in); err != nil {
		t.Fatal("reservation not released", err)
	}
	done()
	assertCount(t, db, 3)
	limitKey := "99999999-9999-4999-8999-999999999999"
	for range 2 {
		_, err = client.AskQuestion(outgoing(limitKey, "a"), &assistantv1.AskQuestionRequest{Text: "limit"})
		done()
		assertReason(t, err, "LIMIT_EXCEEDED")
	}
	assertCount(t, db, 3)
	_, err = client.AskQuestion(outgoing("cccccccc-cccc-4ccc-8ccc-cccccccccccc", "a"), &assistantv1.AskQuestionRequest{Text: "sql"})
	done()
	assertReason(t, err, "INTERNAL_ERROR")
	if status.Code(err) != codes.Internal || strings.Contains(err.Error(), "private SQL") {
		t.Fatal("raw effect error leaked", err)
	}
	assertCount(t, db, 3)
	peer.mode.Store(2)
	cancelKey := "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	callCtx, stop := context.WithCancel(outgoing(cancelKey, "a"))
	callDone := make(chan error, 1)
	go func() { _, err := client.AskQuestion(callCtx, in); callDone <- err }()
	select {
	case <-peer.waiting:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Public API only: zero-token Release must preserve both duplicate errors.
	_, err = client.AskQuestion(outgoing(cancelKey, "a"), in)
	done()
	assertReason(t, err, "REQUEST_IN_PROGRESS")
	if status.Code(err) != codes.Aborted {
		t.Fatal(err)
	}
	_, err = client.AskQuestion(outgoing(cancelKey, "a"), &assistantv1.AskQuestionRequest{Text: "changed"})
	done()
	assertReason(t, err, "IDEMPOTENCY_CONFLICT")
	stop()
	if err = <-callDone; status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
	done()
	assertCount(t, db, 3)
	peer.mode.Store(0)
	if _, err = client.AskQuestion(outgoing(cancelKey, "a"), in); err != nil {
		t.Fatal("cancelled reservation not released", err)
	}
	done()
	assertCount(t, db, 4)
}
