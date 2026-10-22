package idempotency

import (
	"crypto/rand"
	"encoding/json"
	"strings"
	"time"

	"github.com/fin-assist/contracts/gen/go/finerr"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const maxBody = 4 << 20
const maxPayload = 6000000

type stored struct {
	Response Response
	Status   []byte
	RetryAt  time.Time
}

func (e *Engine) encode(r Request, p Policy, result Result) ([]byte, time.Time, error) {
	expires := e.now().Add(p.TTL)
	s := stored{Response: result.Response}
	if !validResponse(result.Response) {
		return nil, time.Time{}, ErrInvalidRequest
	}
	if result.Err != nil {
		rest, ok := finerr.ToREST(result.Err)
		if !ok {
			return nil, time.Time{}, ErrNotStored
		}
		code := rest.Spec.HTTPStatus
		if result.Response.DuplicateRetryAt.IsZero() {
			if code != 400 && code != 413 && code != 422 {
				return nil, time.Time{}, ErrNotStored
			}
		} else {
			retry := result.Response.DuplicateRetryAt
			if r.Endpoint != "/finassist.user.v1.AuthService/StartPhoneVerification" || rest.Code != "RATE_LIMITED" || !retry.After(e.now()) || rest.Metadata["retry_at"] != retry.Format(time.RFC3339) {
				return nil, time.Time{}, ErrInvalidRequest
			}
			expires = retry
			s.RetryAt = retry
		}
		var err error
		s.Status, err = proto.Marshal(status.Convert(result.Err).Proto())
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
	if err := json.Unmarshal(b, &s); err != nil || !validResponse(s.Response) {
		return Result{}, ErrStorage
	}
	result := Result{Response: s.Response}
	if len(s.Status) > 0 {
		st := new(statuspb.Status)
		if err := proto.Unmarshal(s.Status, st); err != nil {
			return Result{}, ErrStorage
		}
		result.Err = restoreStatus(st)
		if _, ok := finerr.ToREST(result.Err); !ok {
			return Result{}, ErrStorage
		}
		if !s.RetryAt.IsZero() {
			result.Err = finerr.RateLimited(s.RetryAt)
		}
	}
	return result, nil
}

func restoreStatus(st *statuspb.Status) error {
	// Only a previously validated finerr status is persisted; ToREST validates it
	// again after decoding. This preserves its full typed details on replay.
	return status.FromProto(st).Err() //nolint:forbidigo // persisted contract status, not a new error constructor
}

func aad(r Request) []byte { return []byte(r.Scope + "\x00" + r.Endpoint + "\x00" + r.Key) }

func validResponse(r Response) bool {
	if len(r.Body) > maxBody || len(r.Headers) > 64 {
		return false
	}
	total := 0
	for k, values := range r.Headers {
		if k == "" || len(values) > 64 || strings.HasPrefix(k, "grpc-") {
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
			if strings.ContainsAny(v, "\r\n") {
				return false
			}
		}
		if total > 16384 {
			return false
		}
	}
	return true
}
