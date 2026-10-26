package idempotency_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonv1 "github.com/fin-assist/contracts/gen/go/finassist/common/v1"
	userv1 "github.com/fin-assist/contracts/gen/go/finassist/user/v1"
	"github.com/fin-assist/contracts/gen/go/finerr"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	idem "github.com/fin-assist/go-idempotency"
)

type replayClock struct {
	mu    sync.Mutex
	value time.Time
}

func (c *replayClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.value }
func (c *replayClock) Set(value time.Time) { c.mu.Lock(); c.value = value; c.mu.Unlock() }

type replayProducer struct {
	userv1.UnimplementedAuthServiceServer
	engine  *idem.Engine
	db      consumerDB
	mu      sync.Mutex
	reply   idem.Response
	outcome error
	effects atomic.Int32
}

func (s *replayProducer) set(reply idem.Response, outcome error) {
	s.mu.Lock()
	s.reply = reply
	s.outcome = outcome
	s.mu.Unlock()
}
func (s *replayProducer) StartPhoneVerification(ctx context.Context, input *userv1.StartPhoneVerificationRequest) (*userv1.StartPhoneVerificationResponse, error) {
	r, ok := idem.FromContext(ctx)
	if !ok {
		return nil, finerr.InternalError()
	}
	var err error
	r.Hash, err = idem.HashProto(input)
	if err != nil {
		return nil, finerr.InternalError()
	}
	result, err := consumerDo(ctx, s.db, s.engine, r, func(ctx context.Context) (idem.Response, error) {
		s.effects.Add(1)
		tx, _ := s.db.Tx(ctx)
		if _, err := tx.Exec(ctx, "INSERT INTO consumer_effects(owner_id) VALUES($1)", publicOwner); err != nil {
			return idem.Response{}, err
		}
		s.mu.Lock()
		reply, outcome := s.reply, s.outcome
		s.mu.Unlock()
		return reply, outcome
	})
	if err != nil {
		return nil, finerr.InternalError()
	}
	if result.Err != nil {
		return nil, result.Err
	}
	out := new(userv1.StartPhoneVerificationResponse)
	if proto.Unmarshal(result.Response.Body, out) != nil {
		return nil, finerr.InternalError()
	}
	return out, nil
}
func replayOutcome(t *testing.T, busy bool, until time.Time, permission *bool) error {
	t.Helper()
	base := finerr.ValidationError(&errdetails.BadRequest_FieldViolation{Field: "phone", Reason: "INVALID_FORMAT", Description: "private input"})
	if busy {
		base = finerr.RateLimited(until)
	}
	wire := status.Convert(base).Proto()
	wire.Message = "private diagnostic"
	wait, err := anypb.New(&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if busy {
		wire.Details[1] = wait
	} else {
		wire.Details = append(wire.Details, wait)
	}
	wire.Details = append(wire.Details, &anypb.Any{TypeUrl: "example.test/future.Extension", Value: []byte{0xff, 0, 1}})
	value := status.FromProto(wire).Err()
	if permission != nil {
		value = finerr.WithRequestRetryInfo(value, *permission)
	}
	return value
}
func verifyReplayedStatus(t *testing.T, original, replayed error, delay time.Duration, permission *bool) {
	t.Helper()
	rest, ok := finerr.ToREST(replayed)
	if !ok {
		t.Fatal("unusable contract error", replayed)
	}
	if (rest.RequestRetryable == nil) != (permission == nil) || permission != nil && *rest.RequestRetryable != *permission {
		t.Fatal("permission changed", rest.RequestRetryable, permission)
	}
	var actual *errdetails.RetryInfo
	after := status.Convert(replayed).Proto()
	for _, detail := range after.Details {
		if detail.MessageIs(&errdetails.RetryInfo{}) {
			actual = new(errdetails.RetryInfo)
			if detail.UnmarshalTo(actual) != nil {
				t.Fatal("bad RetryInfo")
			}
		}
	}
	if delay == 0 {
		if actual != nil {
			t.Fatal("old optional wait restarted")
		}
	} else if actual == nil || actual.RetryDelay.AsDuration() != delay {
		t.Fatal("wrong fresh relative wait", actual, delay)
	}
	for _, detail := range status.Convert(original).Proto().Details {
		if detail.MessageIs(&errdetails.RetryInfo{}) || detail.MessageIs(&commonv1.RequestRetryInfo{}) {
			continue
		}
		found := false
		for _, value := range after.Details {
			if proto.Equal(value, detail) {
				found = true
			}
		}
		if !found {
			t.Fatal("typed or unknown detail lost", detail.TypeUrl)
		}
	}
	if after.Message != rest.Code {
		t.Fatal("wrapper diagnostics persisted")
	}
}
func rewriteReplayPayload(t *testing.T, ctx context.Context, db consumerDB, key string, edit func(map[string]json.RawMessage)) {
	t.Helper()
	var sealed []byte
	if err := db.pool.QueryRow(ctx, "SELECT payload FROM idempotency_keys WHERE key=$1", key).Scan(&sealed); err != nil {
		t.Fatal(err)
	}
	cipher, err := idem.NewCipher(bytes.Repeat([]byte{42}, 32))
	if err != nil {
		t.Fatal(err)
	}
	aad := []byte("00000000-0000-0000-0000-000000000000\x00" + userv1.AuthService_StartPhoneVerification_FullMethodName + "\x00" + key)
	n := cipher.NonceSize()
	plain, err := cipher.Open(nil, sealed[:n], sealed[n:], aad)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err = json.Unmarshal(plain, &object); err != nil {
		t.Fatal(err)
	}
	edit(object)
	plain, err = json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, n)
	if _, err = rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	updated := cipher.Seal(nonce, nonce, plain, aad)
	if _, err = db.pool.Exec(ctx, "UPDATE idempotency_keys SET payload=$1 WHERE key=$2", updated, key); err != nil {
		t.Fatal(err)
	}
}
func TestIntegrationCachedStructuredStatusPublicTransport(t *testing.T) { cachedStructuredTransport(t) }
func TestAPICachedStructuredStatusPublicTransport(t *testing.T)         { cachedStructuredTransport(t) }
func cachedStructuredTransport(t *testing.T) {
	ctx, db, _, _ := publicEngine(t)
	origin := time.Date(2040, 1, 2, 3, 4, 5, 0, time.UTC)
	clock := &replayClock{value: origin}
	cipher, err := idem.NewCipher(bytes.Repeat([]byte{42}, 32))
	if err != nil {
		t.Fatal(err)
	}
	engine, err := idem.New(db, idem.Options{Methods: map[string]idem.Policy{userv1.AuthService_StartPhoneVerification_FullMethodName: {TTL: 90 * time.Second, Encrypt: true}}, Clock: clock.Now, Cipher: cipher, Registerer: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	producer := &replayProducer{engine: engine, db: db}
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(engine.UnaryInterceptor(func(context.Context) (string, error) { return "", nil })))
	userv1.RegisterAuthServiceServer(server, producer)
	listener := bufconn.Listen(1 << 20)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///cached-result", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := userv1.NewAuthServiceClient(conn)
	yes, no := true, false
	serial := 0
	for _, busy := range []bool{false, true} {
		for _, permission := range []*bool{nil, &no, &yes} {
			for _, legacy := range []bool{false, true} {
				serial++
				name := fmt.Sprintf("busy=%t/advice=%v/legacy=%t", busy, permission, legacy)
				t.Run(name, func(t *testing.T) {
					clock.Set(origin)
					key := fmt.Sprintf("11111111-1111-4111-8111-%012d", serial)
					keyed := metadata.AppendToOutgoingContext(ctx, "idempotency-key", key)
					until := origin.Add(time.Minute)
					original := replayOutcome(t, busy, until, permission)
					response := idem.Response{}
					if busy {
						response.DuplicateRetryAt = until
					}
					producer.set(response, original)
					before := producer.effects.Load()
					_, firstErr := client.StartPhoneVerification(keyed, &userv1.StartPhoneVerificationRequest{Phone: "fixture"})
					verifyReplayedStatus(t, original, firstErr, time.Minute, permission)
					if legacy && !busy {
						rewriteReplayPayload(t, ctx, db, key, func(object map[string]json.RawMessage) { delete(object, "RetryInfoUntil") })
					}
					var saved []byte
					var expires time.Time
					if err := db.pool.QueryRow(ctx, "SELECT payload,expires_at FROM idempotency_keys WHERE key=$1", key).Scan(&saved, &expires); err != nil {
						t.Fatal(err)
					}
					clock.Set(origin.Add(15 * time.Second))
					_, replayed := client.StartPhoneVerification(keyed, &userv1.StartPhoneVerificationRequest{Phone: "fixture"})
					expected := permission
					delay := 45 * time.Second
					if (busy || legacy) && permission != nil && *permission {
						expected = &no
					}
					if legacy && !busy {
						delay = 0
					}
					verifyReplayedStatus(t, original, replayed, delay, expected)
					if producer.effects.Load() != before+1 {
						t.Fatal("replay reran transactional effect")
					}
					var unchanged []byte
					var sameExpiry time.Time
					if err := db.pool.QueryRow(ctx, "SELECT payload,expires_at FROM idempotency_keys WHERE key=$1", key).Scan(&unchanged, &sameExpiry); err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(saved, unchanged) || !expires.Equal(sameExpiry) {
						t.Fatal("read renewed outcome or expiry")
					}
					if !busy {
						clock.Set(origin.Add(time.Minute))
						_, replayed = client.StartPhoneVerification(keyed, &userv1.StartPhoneVerificationRequest{Phone: "fixture"})
						verifyReplayedStatus(t, original, replayed, 0, expected)
					}
				})
			}
		}
	}

	// The rounded public wait must fit the actual row expiry, not merely the raw anchor.
	clock.Set(origin)
	boundaryKey := "33333333-3333-4333-8333-333333333333"
	boundaryCtx := metadata.AppendToOutgoingContext(ctx, "idempotency-key", boundaryKey)
	original := replayOutcome(t, false, time.Time{}, &yes)
	producer.set(idem.Response{}, original)
	_, _ = client.StartPhoneVerification(boundaryCtx, &userv1.StartPhoneVerificationRequest{Phone: "boundary"})
	if _, err = db.pool.Exec(ctx, "UPDATE idempotency_keys SET expires_at=$1 WHERE key=$2", origin.Add(time.Minute+250*time.Millisecond), boundaryKey); err != nil {
		t.Fatal(err)
	}
	clock.Set(origin.Add(500 * time.Millisecond))
	_, replayed := client.StartPhoneVerification(boundaryCtx, &userv1.StartPhoneVerificationRequest{Phone: "boundary"})
	verifyReplayedStatus(t, original, replayed, 59500*time.Millisecond, &no)
	// Corrupt recognized details and contradictory absolute bindings fail before the effect.
	for index, change := range []func(map[string]json.RawMessage){
		func(object map[string]json.RawMessage) {
			editReplayWire(t, object, func(wire *statuspb.Status) {
				wire.Details = append(wire.Details, proto.Clone(wire.Details[0]).(*anypb.Any))
			})
		},
		func(object map[string]json.RawMessage) {
			editReplayWire(t, object, func(wire *statuspb.Status) { wire.Details[0].Value = []byte{0xff} })
		},
		func(object map[string]json.RawMessage) {
			editReplayWire(t, object, func(wire *statuspb.Status) {
				for _, d := range wire.Details {
					if d.MessageIs(&commonv1.RequestRetryInfo{}) {
						wire.Details = append(wire.Details, proto.Clone(d).(*anypb.Any))
						return
					}
				}
			})
		},
		func(object map[string]json.RawMessage) {
			editReplayWire(t, object, func(wire *statuspb.Status) {
				for _, d := range wire.Details {
					if d.MessageIs(&errdetails.RetryInfo{}) {
						wire.Details = append(wire.Details, proto.Clone(d).(*anypb.Any))
						return
					}
				}
			})
		},
		func(object map[string]json.RawMessage) {
			object["RetryInfoUntil"] = json.RawMessage(`"3000-01-01T00:00:00Z"`)
		},
	} {
		clock.Set(origin)
		key := fmt.Sprintf("44444444-4444-4444-8444-%012d", index)
		keyed := metadata.AppendToOutgoingContext(ctx, "idempotency-key", key)
		producer.set(idem.Response{}, replayOutcome(t, false, time.Time{}, &yes))
		_, _ = client.StartPhoneVerification(keyed, &userv1.StartPhoneVerificationRequest{Phone: "malformed"})
		rewriteReplayPayload(t, ctx, db, key, change)
		before := producer.effects.Load()
		_, failure := client.StartPhoneVerification(keyed, &userv1.StartPhoneVerificationRequest{Phone: "malformed"})
		consumerCode(t, failure, "INTERNAL_ERROR")
		if producer.effects.Load() != before {
			t.Fatal("unusable cached status reran effect")
		}
	}
	clock.Set(origin)
	brokenBusyKey := "55555555-5555-4555-8555-555555555555"
	brokenBusyCtx := metadata.AppendToOutgoingContext(ctx, "idempotency-key", brokenBusyKey)
	producer.set(idem.Response{DuplicateRetryAt: origin.Add(time.Minute)}, replayOutcome(t, true, origin.Add(time.Minute), &no))
	_, _ = client.StartPhoneVerification(brokenBusyCtx, &userv1.StartPhoneVerificationRequest{Phone: "binding"})
	rewriteReplayPayload(t, ctx, db, brokenBusyKey, func(object map[string]json.RawMessage) { object["RetryAt"] = json.RawMessage(`"2040-01-02T03:04:36Z"`) })
	beforeBroken := producer.effects.Load()
	_, failure := client.StartPhoneVerification(brokenBusyCtx, &userv1.StartPhoneVerificationRequest{Phone: "binding"})
	consumerCode(t, failure, "INTERNAL_ERROR")
	if producer.effects.Load() != beforeBroken {
		t.Fatal("invalid required wait binding reran effect")
	}
	clock.Set(origin)
	distantKey := "66666666-6666-4666-8666-666666666666"
	distantCtx := metadata.AppendToOutgoingContext(ctx, "idempotency-key", distantKey)
	distant := time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
	producer.set(idem.Response{DuplicateRetryAt: distant}, replayOutcome(t, true, distant, &no))
	_, _ = client.StartPhoneVerification(distantCtx, &userv1.StartPhoneVerificationRequest{Phone: "distant"})
	_, failure = client.StartPhoneVerification(distantCtx, &userv1.StartPhoneVerificationRequest{Phone: "distant"})
	consumerCode(t, failure, "INTERNAL_ERROR")
	// Existing expiry is inclusive: the next transaction reexecutes rather than replaying a required expired wait.
	clock.Set(origin)
	key := "22222222-2222-4222-8222-222222222222"
	keyed := metadata.AppendToOutgoingContext(ctx, "idempotency-key", key)
	until := origin.Add(time.Minute)
	producer.set(idem.Response{DuplicateRetryAt: until}, replayOutcome(t, true, until, &yes))
	_, _ = client.StartPhoneVerification(keyed, &userv1.StartPhoneVerificationRequest{Phone: "expiry"})
	before := producer.effects.Load()
	success, _ := proto.Marshal(&userv1.StartPhoneVerificationResponse{VerificationToken: "fresh"})
	producer.set(idem.Response{Body: success}, nil)
	clock.Set(until)
	out, err := client.StartPhoneVerification(keyed, &userv1.StartPhoneVerificationRequest{Phone: "expiry"})
	if err != nil || out.GetVerificationToken() != "fresh" || producer.effects.Load() != before+1 {
		t.Fatal("inclusive expiry did not reexecute", out, err)
	}
}

// This edits independently encoded persisted fixtures, not the library decoder.
func editReplayWire(t *testing.T, object map[string]json.RawMessage, change func(*statuspb.Status)) {
	t.Helper()
	var encoded []byte
	if err := json.Unmarshal(object["Status"], &encoded); err != nil {
		t.Fatal(err)
	}
	wire := new(statuspb.Status)
	if err := proto.Unmarshal(encoded, wire); err != nil {
		t.Fatal(err)
	}
	change(wire)
	encoded, err := proto.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	object["Status"], err = json.Marshal(encoded)
	if err != nil {
		t.Fatal(err)
	}
}
