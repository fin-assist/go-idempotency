package idempotency

import (
	"crypto/rand"
	"encoding/json"
	"strings"
	"time"

	commonv1 "github.com/fin-assist/contracts/gen/go/finassist/common/v1"
	"github.com/fin-assist/contracts/gen/go/finerr"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const maxBody = 4 << 20
const maxPayload = 6000000

type stored struct {
	Response Response
	Status   []byte
	RetryAt  time.Time
	// RetryInfoUntil anchors an optional relative wait once, without changing the key expiry.
	RetryInfoUntil time.Time `json:",omitzero"`
}

func (e *Engine) encode(r Request, p Policy, result Result) ([]byte, time.Time, error) {
	now := e.now()
	expires := now.Add(p.TTL)
	s := stored{Response: result.Response}
	if !validResponse(result.Response) || !validOwner(r, result.Response.OwnerID) || !validResource(result.Response.Resource) {
		return nil, time.Time{}, ErrInvalidRequest
	}
	if result.Err != nil {
		wire, rest, statusErr := checkedStatus(result.Err)
		if statusErr != nil {
			return nil, time.Time{}, ErrStorage
		}
		code := rest.Spec.HTTPStatus
		if result.Response.DuplicateRetryAt.IsZero() {
			if code != 400 && code != 413 && code != 422 {
				return nil, time.Time{}, ErrNotStored
			}
		} else {
			retry := result.Response.DuplicateRetryAt
			if r.Endpoint != "/finassist.user.v1.AuthService/StartPhoneVerification" || rest.Code != "RATE_LIMITED" || !retry.After(now) || rest.Metadata["retry_at"] != retry.Format(time.RFC3339) {
				return nil, time.Time{}, ErrInvalidRequest
			}
			expires = retry
			s.RetryAt = retry
		}
		if s.RetryAt.IsZero() {
			_, wait, err := storedRetryInfo(wire)
			if err != nil {
				return nil, time.Time{}, err
			}
			if wait != nil {
				s.RetryInfoUntil = now.Add(wait.RetryDelay.AsDuration())
			}
		}
		var err error
		s.Status, err = proto.Marshal(wire)
		if err != nil {
			return nil, time.Time{}, ErrStorage
		}
	} else if !result.Response.DuplicateRetryAt.IsZero() {
		return nil, time.Time{}, ErrInvalidRequest
	}
	b, err := json.Marshal(s)
	if err != nil || len(b) > maxPayload-64 {
		return nil, time.Time{}, ErrInvalidRequest
	}
	if p.Encrypt {
		nonce := make([]byte, e.cipher.NonceSize())
		if _, err = rand.Read(nonce); err != nil {
			return nil, time.Time{}, ErrStorage
		}
		b = e.cipher.Seal(nonce, nonce, b, aad(r))
	}
	return b, expires, nil
}

func (e *Engine) decode(r Request, row record) (Result, error) {
	now := row.observedAt
	if now.IsZero() {
		now = e.now()
	}
	b := row.payload
	p := e.methods[r.Endpoint]
	if row.encrypted != p.Encrypt || len(b) > maxPayload {
		return Result{}, ErrStorage
	}
	if row.encrypted {
		if e.cipher == nil || len(b) < e.cipher.NonceSize() {
			return Result{}, ErrStorage
		}
		n := e.cipher.NonceSize()
		var err error
		b, err = e.cipher.Open(nil, b[:n], b[n:], aad(r))
		if err != nil {
			return Result{}, ErrStorage
		}
	}
	var s stored
	if err := json.Unmarshal(b, &s); err != nil || !validResponse(s.Response) || !validOwner(r, s.Response.OwnerID) || s.Response.OwnerID != row.owner || !validResource(s.Response.Resource) || s.Response.Resource != row.resource {
		return Result{}, ErrStorage
	}
	result := Result{Response: s.Response}
	if len(s.Status) > 0 {
		st := new(statuspb.Status)
		if err := proto.Unmarshal(s.Status, st); err != nil {
			return Result{}, ErrStorage
		}
		var err error
		result.Err, err = e.replayStatus(r, row, s, st, now)
		if err != nil {
			return Result{}, err
		}
	} else if !s.RetryAt.IsZero() || !s.RetryInfoUntil.IsZero() || !s.Response.DuplicateRetryAt.IsZero() {
		return Result{}, ErrStorage
	}
	return result, nil
}

func restoreStatus(st *statuspb.Status) error {
	// Only validated contract outcomes are converted here. Callers validate with
	// ToREST and canonicalize the message, preserving the typed details.
	return status.FromProto(st).Err() //nolint:forbidigo // persisted contract status, not a new error constructor
}

func aad(r Request) []byte { return []byte(r.Scope + "\x00" + r.Endpoint + "\x00" + r.Key) }

func validResponse(r Response) bool {
	if len(r.Body) > maxBody || len(r.Headers) > 64 {
		return false
	}
	total := 0
	for k, values := range r.Headers {
		// Binary "-bin" values do not survive the JSON outcome format byte for byte.
		if k == "" || len(values) > 64 || strings.HasPrefix(k, "grpc-") || strings.HasSuffix(k, "-bin") {
			return false
		}
		for _, c := range k {
			switch {
			case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_', c == '.':
			default:
				return false
			}
		}
		total += len(k)
		for _, v := range values {
			total += len(v)
			if !printableASCII(v) {
				return false
			}
		}
		if total > 16384 {
			return false
		}
	}
	return true
}

// printableASCII mirrors gRPC metadata value rules for non-binary keys, so a
// committed outcome is always sendable by grpc.SetHeader, including on replay.
func printableASCII(v string) bool {
	for i := 0; i < len(v); i++ {
		if v[i] < 0x20 || v[i] > 0x7e {
			return false
		}
	}
	return true
}

// contractStatus keeps typed contract details but never a wrapper's internal text.
func contractStatus(err error) (error, error) {
	wire, _, validationErr := checkedStatus(err)
	if validationErr != nil {
		return nil, validationErr
	}
	return restoreStatus(wire), nil
}

// validOwner keeps ownership distinct from identity, without allowing an
// authenticated scope to misattribute its result to another user.
func validOwner(r Request, owner string) bool {
	return owner == "" || (validUUID(owner) && owner != nilScope && owner == strings.ToLower(owner) && (r.Scope == "" || r.Scope == nilScope || r.Scope == owner))
}

// checkedStatus delegates the structured profile to finerr and removes wrapper diagnostics.
func checkedStatus(err error) (*statuspb.Status, finerr.REST, error) {
	parsed, ok := finerr.ToREST(err)
	if !ok || status.Code(err) != parsed.Spec.GRPCCode {
		return nil, finerr.REST{}, ErrStorage
	}
	// Proto returns an independent copy, including unknown Any values and unknown fields.
	wire := status.Convert(err).Proto()
	wire.Message = parsed.Code
	return wire, parsed, nil
}

// storedRetryInfo locates a detail only after finerr has validated its uniqueness and payload.
func storedRetryInfo(wire *statuspb.Status) (int, *errdetails.RetryInfo, error) {
	for i, detail := range wire.Details {
		if detail.MessageIs(&errdetails.RetryInfo{}) {
			value := new(errdetails.RetryInfo)
			if detail.UnmarshalTo(value) != nil {
				return -1, nil, ErrStorage
			}
			return i, value, nil
		}
	}
	return -1, nil, nil
}

func (e *Engine) replayStatus(r Request, row record, s stored, original *statuspb.Status, now time.Time) (error, error) {
	wire, parsed, err := checkedStatus(restoreStatus(original))
	if err != nil {
		return nil, err
	}
	index, wait, err := storedRetryInfo(wire)
	if err != nil {
		return nil, err
	}
	if !row.expires.IsZero() && !row.expires.After(now) {
		return nil, ErrStorage
	}
	allowedAt := now
	permissionKnown := true
	if !s.RetryAt.IsZero() || !s.Response.DuplicateRetryAt.IsZero() {
		// Reapply the existing opt-in binding before using its absolute wait as evidence.
		if r.Endpoint != "/finassist.user.v1.AuthService/StartPhoneVerification" || parsed.Code != "RATE_LIMITED" || s.RetryAt.IsZero() ||
			!s.Response.DuplicateRetryAt.Equal(s.RetryAt) || parsed.Metadata["retry_at"] != s.RetryAt.Format(time.RFC3339) ||
			!s.RetryInfoUntil.IsZero() || !s.RetryAt.After(now) || wait == nil {
			return nil, ErrStorage
		}
		allowedAt = s.RetryAt
	} else if wait != nil {
		if parsed.Spec.RetryInfo {
			return nil, ErrStorage
		} // A mandatory saved wait must carry its existing absolute opt-in.
		if s.RetryInfoUntil.IsZero() {
			permissionKnown = false
		}
		if s.RetryInfoUntil.IsZero() || !s.RetryInfoUntil.After(now) {
			wire.Details = append(wire.Details[:index], wire.Details[index+1:]...)
			wait = nil
		} else {
			allowedAt = s.RetryInfoUntil
		}
	} else if !s.RetryInfoUntil.IsZero() {
		return nil, ErrStorage
	}
	if wait != nil {
		delay := allowedAt.Sub(now)
		if delay <= 0 || !now.Add(delay).Equal(allowedAt) {
			return nil, ErrStorage
		}
		wait.RetryDelay = durationpb.New(delay)
		payload, err := proto.Marshal(wait)
		if err != nil {
			return nil, ErrStorage
		}
		wire.Details[index].Value = payload // Preserve the original TypeUrl and all other Any bytes.
		// REST rounds Retry-After up to a whole second; that later instant must fit too.
		if fraction := delay % time.Second; fraction != 0 {
			allowedAt = allowedAt.Add(time.Second - fraction)
		}
	}
	if parsed.RequestRetryable != nil && *parsed.RequestRetryable && (!permissionKnown || row.expires.IsZero() || !allowedAt.Before(row.expires)) {
		// Mechanical replay evidence may only restrict service permission, never create it.
		for _, detail := range wire.Details {
			if detail.MessageIs(&commonv1.RequestRetryInfo{}) {
				advice := new(commonv1.RequestRetryInfo)
				if detail.UnmarshalTo(advice) != nil {
					return nil, ErrStorage
				}
				advice.Retryable = false
				payload, err := proto.Marshal(advice)
				if err != nil {
					return nil, ErrStorage
				}
				detail.Value = payload
			}
		}
	}
	return restoreStatus(wire), nil
}
