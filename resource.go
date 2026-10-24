package idempotency

import (
	"context"
	"errors"
	"time"

	"github.com/fin-assist/contracts/gen/go/finerr"
	"github.com/jackc/pgx/v5"
)

func storedResource(kind, id *string) (Resource, error) {
	if kind == nil && id == nil {
		return Resource{}, nil
	}
	if kind == nil || id == nil {
		return Resource{}, ErrStorage
	}
	r := Resource{Type: *kind, ID: *id}
	if r == (Resource{}) || !validResource(r) {
		return Resource{}, ErrStorage
	}
	return r, nil
}

// The row was fetched by the complete PK under lock; evaluate the remaining
// fence with a timestamp taken after that lock, never before its wait.
func checkResourceBinding(row record, token Reservation, resource Resource, now time.Time) error {
	if row.done || row.hash != token.request.Hash || row.reservation != token.id || !row.expires.After(now) {
		return finerr.RequestInProgress(time.Second)
	}
	if row.resource != (Resource{}) && row.resource != resource {
		return ErrInvalidRequest
	}
	return nil
}

func inheritResource(response Response, bound Resource) (Response, error) {
	if bound == (Resource{}) {
		return response, nil
	}
	if response.Resource != (Resource{}) && response.Resource != bound {
		return Response{}, ErrInvalidRequest
	}
	response.Resource = bound
	return response, nil
}

func validResource(r Resource) bool {
	if r == (Resource{}) {
		return true
	}
	if len(r.Type) < 1 || len(r.Type) > 64 || len(r.ID) < 1 || len(r.ID) > 256 || r.Type[0] < 'a' || r.Type[0] > 'z' {
		return false
	}
	for _, c := range r.Type {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
		default:
			return false
		}
	}
	for _, c := range r.ID {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '_', c == ':', c == '-':
		default:
			return false
		}
	}
	return true
}

// BindResource links the caller's unfinished, unexpired reservation to a verified
// resource in the SAME primary transaction as the resource claim. Commit before
// external I/O. Repeating the same binding is harmless; changing it is invalid.
// DoReserved inherits the link, including a cacheable error. This is bookkeeping,
// never authorization: the service validates access and serializes purge/replay.
//
// Invalid/zero tokens or resources return ErrInvalidRequest, no transaction gives
// ErrNoTransaction, and a missing/completed/expired/foreign reservation gives
// REQUEST_IN_PROGRESS. Other storage/lock/cancellation errors require rollback.
func (e *Engine) BindResource(ctx context.Context, token Reservation, resource Resource) error {
	r, _, err := e.validate(token.request)
	if err != nil || token.id == "" || resource == (Resource{}) || !validResource(resource) {
		return ErrInvalidRequest
	}
	tx, err := e.transaction(ctx)
	if err != nil {
		return err
	}
	var row record
	err = e.locked(ctx, tx, func() error {
		var readErr error
		row, readErr = readRecord(ctx, tx, r)
		return readErr
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return e.progress(r, time.Second)
	}
	if err != nil {
		return err
	}
	// Check the clock after acquiring the key, as with conditional compensation.
	if err = checkResourceBinding(row, token, resource, e.now()); err != nil {
		e.observeError(r, err)
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE idempotency_keys SET resource_type=$7,resource_id=$8 WHERE scope=$1 AND endpoint=$2 AND key=$3 AND NOT done AND request_hash=$4 AND reservation_id=$5 AND expires_at>$6`, r.Scope, r.Endpoint, r.Key, r.Hash[:], token.id, e.now(), resource.Type, resource.ID)
	if err != nil {
		return storageError(err)
	}
	if tag.RowsAffected() != 1 {
		return e.progress(r, time.Second)
	}
	return nil
}
