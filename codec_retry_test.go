package idempotency

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"
	"time"

	commonv1 "github.com/fin-assist/contracts/gen/go/finassist/common/v1"
	"github.com/fin-assist/contracts/gen/go/finerr"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

func codecAdviceError(t *testing.T, busy bool, until time.Time, wait time.Duration, advice *bool) error {
	t.Helper()
	original := finerr.WrongFormat()
	if busy {
		original = finerr.RateLimited(until)
	}
	wire := status.Convert(original).Proto()
	info := new(errdetails.ErrorInfo)
	if err := wire.Details[0].UnmarshalTo(info); err != nil {
		t.Fatal(err)
	}
	info.Metadata = map[string]string{"private_context": "preserved"}
	if busy {
		info.Metadata["retry_at"] = until.Format(time.RFC3339)
	}
	detail, err := anypb.New(info)
	if err != nil {
		t.Fatal(err)
	}
	wire.Details[0] = detail
	retry, err := anypb.New(&errdetails.RetryInfo{RetryDelay: durationpb.New(wait)})
	if err != nil {
		t.Fatal(err)
	}
	if busy {
		wire.Details[1] = retry
	} else {
		wire.Details = append(wire.Details, retry)
	}
	bad, err := anypb.New(&errdetails.BadRequest{FieldViolations: []*errdetails.BadRequest_FieldViolation{{Field: "field", Reason: "INVALID_FORMAT", Description: "private validation"}}})
	if err != nil {
		t.Fatal(err)
	}
	wire.Details = append(wire.Details, bad, &anypb.Any{TypeUrl: "custom.example/future.OpaqueDetail", Value: []byte{0xff, 0, 0x80}})
	// A nonstandard type-URL prefix is still recognized; preserve it when updating its value.
	for _, d := range wire.Details {
		if d.MessageIs(&errdetails.RetryInfo{}) {
			d.TypeUrl = "custom.example/google.rpc.RetryInfo"
		}
	}
	original = status.FromProto(wire).Err()
	if advice != nil {
		original = finerr.WithRequestRetryInfo(original, *advice)
	}
	return original
}
func retryDetail(wire *statuspb.Status) *errdetails.RetryInfo {
	for _, d := range wire.Details {
		if d.MessageIs(&errdetails.RetryInfo{}) {
			out := new(errdetails.RetryInfo)
			if d.UnmarshalTo(out) != nil {
				return nil
			}
			return out
		}
	}
	return nil
}
func assertStoredDetails(t *testing.T, original, result error, wantDelay time.Duration, wantAdvice *bool) {
	t.Helper()
	before, after := status.Convert(original).Proto(), status.Convert(result).Proto()
	got, valid := finerr.ToREST(result)
	if !valid {
		t.Fatal("invalid replay status", result)
	}
	if (got.RequestRetryable == nil) != (wantAdvice == nil) || wantAdvice != nil && *got.RequestRetryable != *wantAdvice {
		t.Fatal("permission presence/value changed", got.RequestRetryable, wantAdvice)
	}
	wait := retryDetail(after)
	if wantDelay == 0 {
		if wait != nil {
			t.Fatal("expired/unanchored optional wait restarted")
		}
	} else if wait == nil || wait.RetryDelay.AsDuration() != wantDelay {
		t.Fatal("wrong fresh delay", wait, wantDelay)
	}
	for _, old := range before.Details {
		if old.MessageIs(&errdetails.RetryInfo{}) || old.MessageIs(&commonv1.RequestRetryInfo{}) {
			continue
		}
		found := false
		for _, current := range after.Details {
			if proto.Equal(old, current) {
				found = true
			}
		}
		if !found {
			t.Fatal("structured detail bytes lost", old.TypeUrl)
		}
	}
	for _, d := range after.Details {
		if d.MessageIs(&errdetails.RetryInfo{}) && d.TypeUrl != "custom.example/google.rpc.RetryInfo" {
			t.Fatal("type URL changed")
		}
	}
	if status.Convert(result).Message() != got.Code {
		t.Fatal("diagnostics not canonicalized")
	}
}
func editStoredOutcome(t *testing.T, e *Engine, r Request, row record, edit func(*stored)) record {
	t.Helper()
	plain := row.payload
	if row.encrypted {
		n := e.cipher.NonceSize()
		var err error
		plain, err = e.cipher.Open(nil, plain[:n], plain[n:], aad(r))
		if err != nil {
			t.Fatal(err)
		}
	}
	var value stored
	if err := json.Unmarshal(plain, &value); err != nil {
		t.Fatal(err)
	}
	edit(&value)
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if row.encrypted {
		nonce := make([]byte, e.cipher.NonceSize())
		if _, err := rand.Read(nonce); err != nil {
			t.Fatal(err)
		}
		encoded = e.cipher.Seal(nonce, nonce, encoded, aad(r))
	}
	row.payload = encoded
	return row
}
func TestUnitCachedRetryDetailsAndAbsoluteClock(t *testing.T) {
	origin := time.Date(2040, 1, 2, 3, 4, 5, 0, time.FixedZone("test", 3*3600))
	yes, no := true, false
	for _, encrypted := range []bool{false, true} {
		for _, busy := range []bool{false, true} {
			for _, advice := range []*bool{nil, &no, &yes} {
				e := unitEngine(t)
				e.cipher, _ = NewCipher(make([]byte, 32))
				now := origin
				e.now = func() time.Time { return now }
				r := request()
				if busy {
					r.Endpoint = "/finassist.user.v1.AuthService/StartPhoneVerification"
				}
				p := Policy{TTL: 90 * time.Second, Encrypt: encrypted}
				e.methods[r.Endpoint] = p
				input := codecAdviceError(t, busy, origin.Add(time.Minute), time.Minute, advice)
				response := Response{}
				if busy {
					response.DuplicateRetryAt = origin.Add(time.Minute)
				}
				payload, expires, err := e.encode(r, p, Result{Response: response, Err: input})
				if err != nil {
					t.Fatal(err)
				}
				saved := append([]byte(nil), payload...)
				row := record{payload: payload, encrypted: encrypted, expires: expires}
				now = origin.Add(20 * time.Second)
				result, err := e.decode(r, row)
				if err != nil {
					t.Fatal(err)
				}
				want := advice
				if busy && advice != nil && *advice {
					want = &no
				}
				assertStoredDetails(t, input, result.Err, 40*time.Second, want)
				if !bytes.Equal(payload, saved) {
					t.Fatal("replay rewrote persisted outcome")
				}
				if !busy {
					now = origin.Add(time.Minute)
					result, err = e.decode(r, row)
					if err != nil {
						t.Fatal(err)
					}
					assertStoredDetails(t, input, result.Err, 0, advice)
					legacy := editStoredOutcome(t, e, r, row, func(s *stored) { s.RetryInfoUntil = time.Time{} })
					result, err = e.decode(r, legacy)
					if err != nil {
						t.Fatal(err)
					}
					want = advice
					if advice != nil && *advice {
						want = &no
					}
					assertStoredDetails(t, input, result.Err, 0, want)
				} else {
					now = origin.Add(time.Minute)
					if _, err = e.decode(r, row); !errors.Is(err, ErrStorage) {
						t.Fatal("expired mandatory wait replayed", err)
					}
				}
			}
		}
	}
}
func TestUnitCachedAdviceExpiryAndLegacyBinding(t *testing.T) {
	yes, no := true, false
	origin := time.Date(2040, 1, 2, 3, 4, 5, 0, time.UTC)
	e := unitEngine(t)
	now := origin
	e.now = func() time.Time { return now }
	r := request()
	p := Policy{TTL: time.Minute + 250*time.Millisecond}
	e.methods[r.Endpoint] = p
	input := codecAdviceError(t, false, time.Time{}, time.Minute, &yes)
	payload, expires, err := e.encode(r, p, Result{Err: input})
	if err != nil {
		t.Fatal(err)
	}
	row := record{payload: payload, expires: expires}
	now = origin.Add(500 * time.Millisecond)
	result, err := e.decode(r, row)
	if err != nil {
		t.Fatal(err)
	}
	assertStoredDetails(t, input, result.Err, 59500*time.Millisecond, &no) // ceil wait ends after real expiry.
	now = origin
	row.expires = origin.Add(time.Minute)
	result, err = e.decode(r, row)
	if err != nil {
		t.Fatal(err)
	}
	assertStoredDetails(t, input, result.Err, time.Minute, &no)
	row.expires = time.Time{}
	result, err = e.decode(r, row)
	if err != nil {
		t.Fatal(err)
	}
	assertStoredDetails(t, input, result.Err, time.Minute, &no)
	row.expires = origin
	if _, err = e.decode(r, row); !errors.Is(err, ErrStorage) {
		t.Fatal("expired key accepted", err)
	}
	// Recheck every stored busy binding rather than replacing any error carrying an unrelated RetryAt.
	r.Endpoint = "/finassist.user.v1.AuthService/StartPhoneVerification"
	e.methods[r.Endpoint] = Policy{TTL: time.Hour}
	until := origin.Add(time.Minute)
	input = codecAdviceError(t, true, until, time.Minute, &no)
	payload, expires, err = e.encode(r, e.methods[r.Endpoint], Result{Response: Response{DuplicateRetryAt: until}, Err: input})
	if err != nil {
		t.Fatal(err)
	}
	row = record{payload: payload, expires: expires}
	for _, edit := range []func(*stored){func(s *stored) { s.RetryAt = time.Time{} }, func(s *stored) { s.Response.DuplicateRetryAt = until.Add(time.Second) }, func(s *stored) { s.RetryInfoUntil = until }, func(s *stored) { s.Status, _ = proto.Marshal(status.Convert(finerr.WrongFormat()).Proto()) }, func(s *stored) { s.Status = nil }} {
		broken := editStoredOutcome(t, e, r, row, edit)
		if _, err = e.decode(r, broken); !errors.Is(err, ErrStorage) {
			t.Fatal("malformed anchor binding accepted", err)
		}
	}
	other := r
	other.Endpoint = testMethod
	if _, err = e.decode(other, row); !errors.Is(err, ErrStorage) {
		t.Fatal("anchor on different endpoint accepted", err)
	}
	// The shared parser rejects duplicate and malformed recognized details, preserving unknown extensions only.
	for _, change := range []func(*statuspb.Status){func(s *statuspb.Status) { s.Details = append(s.Details, proto.Clone(s.Details[1]).(*anypb.Any)) }, func(s *statuspb.Status) { s.Details[1].Value = []byte{0xff} }, func(s *statuspb.Status) { s.Details = append(s.Details, proto.Clone(s.Details[0]).(*anypb.Any)) }} {
		broken := editStoredOutcome(t, e, r, row, func(s *stored) {
			wire := new(statuspb.Status)
			if err := proto.Unmarshal(s.Status, wire); err != nil {
				t.Fatal(err)
			}
			change(wire)
			s.Status, _ = proto.Marshal(wire)
		})
		if _, err = e.decode(r, broken); !errors.Is(err, ErrStorage) {
			t.Fatal("bad recognized detail accepted", err)
		}
	}
}

func TestUnitCachedAbsoluteWaitMustFitDuration(t *testing.T) {
	origin := time.Date(2040, 1, 2, 3, 4, 5, 0, time.UTC)
	e := unitEngine(t)
	e.now = func() time.Time { return origin }
	r := request()
	p := Policy{TTL: time.Hour}
	e.methods[r.Endpoint] = p
	no := false
	input := codecAdviceError(t, false, time.Time{}, time.Minute, &no)
	payload, expiry, err := e.encode(r, p, Result{Err: input})
	if err != nil {
		t.Fatal(err)
	}
	distant := time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
	row := editStoredOutcome(t, e, r, record{payload: payload, expires: expiry}, func(s *stored) { s.RetryInfoUntil = distant })
	if _, err = e.decode(r, row); !errors.Is(err, ErrStorage) {
		t.Fatal("unrepresentable optional wait was saturated", err)
	}
	r.Endpoint = "/finassist.user.v1.AuthService/StartPhoneVerification"
	e.methods[r.Endpoint] = p
	input = codecAdviceError(t, true, distant, time.Minute, &no)
	payload, expiry, err = e.encode(r, p, Result{Response: Response{DuplicateRetryAt: distant}, Err: input})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.decode(r, record{payload: payload, expires: expiry}); !errors.Is(err, ErrStorage) {
		t.Fatal("unrepresentable required wait was saturated", err)
	}
}
