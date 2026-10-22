package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"

	"github.com/fin-assist/contracts/gen/go/finerr"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type completionKey struct{}
type requestKey struct{}

// WithCompletion bounds a detached context for the service's transaction AND its
// commit. Do/DoReserved still use the original request cancellation until the
// effect starts. Call cancel only after Run returns. Do not use this context for
// preflight network calls. No goroutine is started.
func (e *Engine) WithCompletion(ctx context.Context) (context.Context, context.CancelFunc) {
	work, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.effectTimeout)
	return context.WithValue(work, completionKey{}, ctx), cancel
}

// preflightContext observes both cancellation sources until the effect starts.
// Cancelling it never cancels the completion context used by the effect/commit.
func preflightContext(completion, original context.Context) (context.Context, context.CancelFunc) {
	var preflight context.Context
	var cancel context.CancelFunc
	if deadline, ok := original.Deadline(); ok {
		preflight, cancel = context.WithDeadline(completion, deadline)
	} else {
		preflight, cancel = context.WithCancel(completion)
	}
	stop := context.AfterFunc(original, cancel)
	// AfterFunc is asynchronous, including when original is already cancelled.
	if original.Err() != nil {
		cancel()
	}
	return preflight, func() {
		stop()
		cancel()
	}
}

// UnaryInterceptor extracts metadata for configured methods only. Place it after
// authentication/tombstones. scope must return only a VERIFIED user UUID (or
// empty for a no-user method); it must fail closed when identity is missing.
// Validation, effects and transactions remain the handler's responsibility.
func (e *Engine) UnaryInterceptor(scope func(context.Context) (string, error)) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if _, ok := e.methods[info.FullMethod]; !ok {
			return next(ctx, req)
		}
		if scope == nil {
			return nil, ErrInvalidRequest
		}
		user, err := scope(ctx)
		if err != nil {
			return nil, err
		}
		md, _ := metadata.FromIncomingContext(ctx)
		keys := md.Get("idempotency-key")
		if len(keys) != 1 || !validUUID(keys[0]) || (user != "" && !validUUID(user)) {
			return nil, invalidKey()
		}
		r := Request{Scope: strings.ToLower(user), Endpoint: info.FullMethod, Key: strings.ToLower(keys[0])}
		return next(context.WithValue(ctx, requestKey{}, r), req)
	}
}

// FromContext returns the identity and key extracted by UnaryInterceptor. The
// handler fills Hash before Do or Reserve. False means no configured method.
func FromContext(ctx context.Context) (Request, bool) {
	r, ok := ctx.Value(requestKey{}).(Request)
	return r, ok
}

// HashProto deterministically marshals a request and returns its SHA-256. This
// follows grpc-api.md; schema changes across deploys may cause a false conflict.
func HashProto(m proto.Message) ([32]byte, error) {
	if m == nil || !m.ProtoReflect().IsValid() || proto.Size(m) > maxBody {
		return [32]byte{}, ErrInvalidRequest
	}
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(m)
	if err != nil {
		return [32]byte{}, ErrInvalidRequest
	}
	return sha256.Sum256(b), nil
}

// HashFile hashes a file as a stream and rejects inputs beyond maxBytes. The
// service supplies an upload-limited reader and hashes before starting a transaction.
func HashFile(r io.Reader, maxBytes int64) ([32]byte, error) {
	if r == nil || maxBytes <= 0 || maxBytes >= 1<<63-1 {
		return [32]byte{}, ErrInvalidRequest
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(r, maxBytes+1))
	if err != nil {
		return [32]byte{}, err
	}
	if n > maxBytes {
		return [32]byte{}, finerr.FileTooLarge()
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}

func validUUID(s string) bool {
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	b, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	return err == nil && len(b) == 16
}

func invalidKey() error {
	return finerr.ValidationError(&errdetails.BadRequest_FieldViolation{Field: "idempotency-key", Reason: "INVALID_FORMAT", Description: "A single UUID is required"})
}
