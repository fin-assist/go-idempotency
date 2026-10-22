package idempotency

import (
	"context"
	"crypto/cipher"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// ErrNoTransaction means the service did not supply a primary transaction.
	ErrNoTransaction = errors.New("idempotency: primary transaction required")
	// ErrCompletionContext means Do was called without WithCompletion around the service transaction.
	ErrCompletionContext = errors.New("idempotency: completion context required")
	// ErrInvalidRequest means a method, scope, key, hash or effect is invalid.
	ErrInvalidRequest = errors.New("idempotency: invalid request")
	// ErrStorage hides database and encrypted payload details. Roll back on this error.
	ErrStorage = errors.New("idempotency: storage failure")
	// ErrNotStored means a validated contract error requires rollback with the effect.
	// Inspect Result.Err after rollback. Unknown effect errors instead return ErrStorage with an empty Result.
	ErrNotStored = errors.New("idempotency: outcome requires rollback")
)

// Transactions exposes the service transaction. It must use READ COMMITTED on the
// primary; the library never commits or rolls it back.
type Transactions interface {
	Tx(context.Context) (pgx.Tx, bool)
}

// Policy configures one stable operation ID. A zero TTL means 24 hours.
type Policy struct {
	TTL time.Duration
	// Encrypt requires Options.Cipher and encrypts the entire stored outcome. Use
	// TTL=60*time.Second and Encrypt=true for RefreshTokens.
	Encrypt bool
}

// Options are copied by New. Clock and Cipher must be safe for concurrent use.
type Options struct {
	// Methods maps stable operation IDs to policies. For gRPC use the full method;
	// for HTTP use a fixed operation name or method/route, never a URL with user IDs.
	Methods map[string]Policy
	Clock   func() time.Time
	// Cipher is an AES-GCM AEAD constructed by NewCipher. Keep its key for at least
	// the longest encrypted TTL; changing it makes old responses unreadable.
	Cipher cipher.AEAD
	// LockTimeout defaults to one second; it applies only to library key operations.
	LockTimeout time.Duration
	// EffectTimeout defaults to 30 seconds, including the service's commit.
	EffectTimeout time.Duration
	// Registerer is required; no process-wide registry is used.
	Registerer prometheus.Registerer
}

// Request identifies a logical action. Endpoint is a configured stable operation
// ID, independent of transport. Scope is a verified user UUID, or empty
// for a no-user method. Key is the client's UUID. Hash must cover the whole input.
type Request struct {
	Scope    string
	Endpoint string
	Key      string
	Hash     [32]byte
}

// Response holds the serialized successful reply and bounded application headers.
// The service chooses its format (for example protobuf for gRPC, JSON for HTTP).
// Errors are returned separately by the effect.
type Response struct {
	Body    []byte
	Headers map[string][]string
	// DuplicateRetryAt explicitly opts into the busy-phone-number RATE_LIMITED
	// exception for StartPhoneVerification only. Zero keeps ordinary 429 rollback.
	DuplicateRetryAt time.Time
}

// Result is usable only after the service transaction commits. Err is a validated
// contract error, original or replayed; return it to gRPC AFTER committing. Returning
// it from the transaction callback would discard a cacheable error.
type Result struct {
	Response Response
	Err      error
	Replayed bool
}

// Reservation is an opaque fencing token from Reserve. It is safe to copy.
// A zero value is invalid for DoReserved and a no-op for Release.
// Never send it to a client or log it.
type Reservation struct {
	request Request
	id      string
}

// Engine is safe for concurrent use with concurrent-safe injected dependencies.
// Methods, keys and user IDs are never logged.
type Engine struct {
	txs           Transactions
	methods       map[string]Policy
	now           func() time.Time
	cipher        cipher.AEAD
	lockTimeout   time.Duration
	effectTimeout time.Duration
	outcomes      *prometheus.CounterVec
}
