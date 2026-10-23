// Package idempotency stores a serialized response or a contract error atomically
// with a service's effect, scoped by verified user, stable operation ID and client key.
// It supports index-lock deduplication and short fenced reservations for preflight
// network checks. The service owns all primary READ COMMITTED transactions.
//
// Use Engine.WithCompletion before starting the effect transaction so cancellation
// after the effect begins cannot prevent commit. Return Do's error from the
// transaction callback; return Result.Err to the caller only after commit. On any Do
// error roll back; ErrNotStored carries the original error in Result.Err.
//
// Response.OwnerID binds a no-user result to an account discovered by the effect.
// Before returning a replay, the service checks its durable tombstone for that owner.
//
// See README for migration wiring, interceptor order, reservation release, refresh
// encryption, cleanup and special phone-verification replay rules.
package idempotency
