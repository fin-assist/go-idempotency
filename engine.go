package idempotency

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/fin-assist/contracts/gen/go/finerr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
)

const nilScope = "00000000-0000-0000-0000-000000000000"
const reservationTTL = 15 * time.Second

// New validates explicit method policies, clock, transaction provider and registry.
// Register metrics once per service. A registration error is returned to the caller.
func New(txs Transactions, o Options) (*Engine, error) {
	if txs == nil || o.Clock == nil || o.Registerer == nil || len(o.Methods) == 0 {
		return nil, ErrInvalidRequest
	}
	if o.LockTimeout == 0 {
		o.LockTimeout = time.Second
	}
	if o.EffectTimeout == 0 {
		o.EffectTimeout = 30 * time.Second
	}
	if o.LockTimeout < time.Millisecond || o.EffectTimeout <= 0 {
		return nil, ErrInvalidRequest
	}
	methods := make(map[string]Policy, len(o.Methods))
	for method, p := range o.Methods {
		if !strings.HasPrefix(method, "/") || strings.Count(method, "/") != 2 || len(method) > 256 || strings.HasSuffix(method, "/") {
			return nil, ErrInvalidRequest
		}
		if p.TTL == 0 {
			p.TTL = 24 * time.Hour
		}
		if p.TTL <= 0 || (p.Encrypt && o.Cipher == nil) {
			return nil, ErrInvalidRequest
		}
		if strings.HasSuffix(method, "/RefreshTokens") && (p.TTL != 60*time.Second || !p.Encrypt) {
			return nil, ErrInvalidRequest
		}
		if (method == "/finassist.user.v1.AuthService/Register" || method == "/finassist.user.v1.AuthService/StartPhoneVerification") && !p.Encrypt {
			return nil, ErrInvalidRequest
		}
		methods[method] = p
	}
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "finassist_idempotency_requests_total", Help: "Idempotency decisions by configured endpoint and outcome."}, []string{"endpoint", "outcome"})
	if err := o.Registerer.Register(counter); err != nil {
		return nil, err
	}
	return &Engine{txs: txs, methods: methods, now: o.Clock, cipher: o.Cipher, lockTimeout: o.LockTimeout, effectTimeout: o.EffectTimeout, outcomes: counter}, nil
}

// NewCipher constructs AES-GCM from a 16-, 24- or 32-byte service secret. It uses
// independent random nonces and binds ciphertext to scope, endpoint and key.
func NewCipher(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrInvalidRequest
	}
	return cipher.NewGCM(block)
}

func (e *Engine) validate(r Request) (Request, Policy, error) {
	p, ok := e.methods[r.Endpoint]
	if !ok || r.Hash == [32]byte{} || !validUUID(r.Key) || (r.Scope != "" && !validUUID(r.Scope)) {
		return Request{}, Policy{}, ErrInvalidRequest
	}
	if r.Scope == "" {
		r.Scope = nilScope
	}
	r.Scope = strings.ToLower(r.Scope)
	r.Key = strings.ToLower(r.Key)
	return r, p, nil
}

func (e *Engine) transaction(ctx context.Context) (pgx.Tx, error) {
	tx, ok := e.txs.Tx(ctx)
	if !ok || tx == nil {
		return nil, ErrNoTransaction
	}
	return tx, nil
}

// Do claims a key and runs effect in the supplied service transaction on the
// primary. Use WithCompletion BEFORE starting that transaction. The callback
// receives the detached, bounded transaction context. It must do no network I/O.
// A non-nil error requires rollback; Result.Err is returned to gRPC after commit.
func (e *Engine) Do(ctx context.Context, r Request, effect func(context.Context) (Response, error)) (Result, error) {
	original, ok := ctx.Value(completionKey{}).(context.Context)
	if !ok {
		return Result{}, ErrCompletionContext
	}
	r, p, err := e.validate(r)
	if err != nil || effect == nil {
		return Result{}, ErrInvalidRequest
	}
	tx, err := e.transaction(ctx)
	if err != nil {
		return Result{}, err
	}
	if err = original.Err(); err != nil {
		return Result{}, err
	}
	row, fresh, err := e.claim(original, tx, r, p.TTL, "")
	if err != nil {
		e.observeError(r, err)
		return Result{}, err
	}
	if !fresh {
		return e.existing(r, row)
	}
	if err = original.Err(); err != nil {
		return Result{}, err
	}
	return e.perform(ctx, tx, r, p, "", effect)
}

// Reserve creates a 15-second fencing reservation in a short, service-owned
// transaction on primary. Commit it BEFORE calling another service. A replay
// returns Result.Replayed=true and a zero token. Errors require rollback.
func (e *Engine) Reserve(ctx context.Context, r Request) (Reservation, Result, error) {
	r, _, err := e.validate(r)
	if err != nil {
		return Reservation{}, Result{}, err
	}
	tx, err := e.transaction(ctx)
	if err != nil {
		return Reservation{}, Result{}, err
	}
	b := make([]byte, 16)
	if _, err = rand.Read(b); err != nil {
		return Reservation{}, Result{}, ErrStorage
	}
	id := hex.EncodeToString(b)
	row, fresh, err := e.claim(ctx, tx, r, reservationTTL, id)
	if err != nil {
		e.observeError(r, err)
		return Reservation{}, Result{}, err
	}
	if !fresh {
		result, err := e.existing(r, row)
		return Reservation{}, result, err
	}
	return Reservation{request: r, id: id}, Result{}, nil
}

// DoReserved fences a committed Reserve token before running the effect in the
// service transaction. A lost/expired token cannot run the effect; a completed
// key returns the saved result. Use WithCompletion before starting the transaction.
// On any preflight/transaction error the service rolls back and calls Release.
func (e *Engine) DoReserved(ctx context.Context, token Reservation, effect func(context.Context) (Response, error)) (Result, error) {
	original, ok := ctx.Value(completionKey{}).(context.Context)
	if !ok {
		return Result{}, ErrCompletionContext
	}
	r, p, err := e.validate(token.request)
	if err != nil || token.id == "" || effect == nil {
		return Result{}, ErrInvalidRequest
	}
	tx, err := e.transaction(ctx)
	if err != nil {
		return Result{}, err
	}
	if err = original.Err(); err != nil {
		return Result{}, err
	}
	var row record
	err = e.locked(original, tx, func() error {
		var readErr error
		row, readErr = readRecord(original, tx, r)
		return readErr
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Result{}, e.progress(r, time.Second)
	}
	if err != nil {
		e.observeError(r, err)
		return Result{}, err
	}
	if row.done {
		if !row.expires.After(e.now()) {
			return Result{}, e.progress(r, time.Second)
		}
		return e.existing(r, row)
	}
	if row.reservation != token.id || !row.expires.After(e.now()) {
		return Result{}, e.progress(r, time.Second)
	}
	if err = original.Err(); err != nil {
		return Result{}, err
	}
	return e.perform(ctx, tx, r, p, token.id, effect)
}

// Release removes only the caller's unfinished reservation. Call it in a NEW,
// bounded service transaction after rollback or a preflight failure. A delayed
// release never deletes a new owner's reservation or a completed response.
func (e *Engine) Release(ctx context.Context, token Reservation) error {
	r, _, err := e.validate(token.request)
	if err != nil || token.id == "" {
		return ErrInvalidRequest
	}
	tx, err := e.transaction(ctx)
	if err != nil {
		return err
	}
	return e.locked(ctx, tx, func() error {
		_, err := tx.Exec(ctx, `DELETE FROM idempotency_keys WHERE scope=$1 AND endpoint=$2 AND key=$3 AND NOT done AND reservation_id=$4`, r.Scope, r.Endpoint, r.Key, token.id)
		return err
	})
}

func (e *Engine) perform(ctx context.Context, tx pgx.Tx, r Request, p Policy, reservation string, effect func(context.Context) (Response, error)) (Result, error) {
	response, domainErr := effect(ctx)
	if domainErr != nil {
		if _, ok := finerr.ToREST(domainErr); !ok {
			return Result{}, ErrStorage
		}
	}
	result := Result{Response: response, Err: domainErr}
	encoded, expires, err := e.encode(r, p, result)
	if err != nil {
		return result, err
	}
	tag, err := tx.Exec(ctx, `UPDATE idempotency_keys SET done=true, payload=$4, encrypted=$5, expires_at=$6, reservation_id='' WHERE scope=$1 AND endpoint=$2 AND key=$3 AND ($7='' OR (reservation_id=$7 AND expires_at>$8))`, r.Scope, r.Endpoint, r.Key, encoded, p.Encrypt, expires, reservation, e.now())
	if err != nil {
		return Result{}, storageError(err)
	}
	if tag.RowsAffected() != 1 {
		return Result{}, e.progress(r, time.Second)
	}
	e.outcomes.WithLabelValues(r.Endpoint, "new").Inc()
	return result, nil
}

func (e *Engine) existing(r Request, row record) (Result, error) {
	if row.hash != r.Hash {
		e.outcomes.WithLabelValues(r.Endpoint, "conflict").Inc()
		return Result{}, finerr.IdempotencyConflict()
	}
	if !row.done {
		return Result{}, e.progress(r, row.expires.Sub(e.now()))
	}
	result, err := e.decode(r, row)
	if err != nil {
		return Result{}, err
	}
	e.outcomes.WithLabelValues(r.Endpoint, "replay").Inc()
	result.Replayed = true
	return result, nil
}

func (e *Engine) progress(r Request, delay time.Duration) error {
	if delay <= 0 {
		delay = time.Millisecond
	}
	e.outcomes.WithLabelValues(r.Endpoint, "in_progress").Inc()
	return finerr.RequestInProgress(delay)
}
func (e *Engine) observeError(r Request, err error) {
	if rest, ok := finerr.ToREST(err); ok && rest.Code == "REQUEST_IN_PROGRESS" {
		e.outcomes.WithLabelValues(r.Endpoint, "in_progress").Inc()
	}
}

// locked restores lock_timeout before the effect. A PostgreSQL error aborts the
// transaction, so all failures must be returned to the service for rollback.
func (e *Engine) locked(ctx context.Context, tx pgx.Tx, fn func() error) error {
	var previous string
	if err := tx.QueryRow(ctx, `SELECT current_setting('lock_timeout')`).Scan(&previous); err != nil {
		return storageError(err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('lock_timeout',$1,true)`, strconv.FormatInt(e.lockTimeout.Milliseconds(), 10)+"ms"); err != nil {
		return storageError(err)
	}
	err := fn()
	if err != nil {
		return storageError(err)
	}
	if _, err = tx.Exec(ctx, `SELECT set_config('lock_timeout',$1,true)`, previous); err != nil {
		return storageError(err)
	}
	return nil
}
func storageError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return pgx.ErrNoRows
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) && pgerr.Code == "55P03" {
		return finerr.RequestInProgress(time.Second)
	}
	return ErrStorage
}
