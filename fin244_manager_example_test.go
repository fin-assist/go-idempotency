package idempotency_test

import (
	"context"
	"time"

	"github.com/fin-assist/contracts/gen/go/finerr"
	outbox "github.com/fin-assist/go-outbox"
	postgres "github.com/fin-assist/go-postgres"
	tombstone "github.com/fin-assist/go-tombstone"
	"github.com/jackc/pgx/v5/pgxpool"

	idem "github.com/fin-assist/go-idempotency"
)

func ExampleEngine_Do_manager() {
	// The service applies all migration sources and wires these long-lived
	// dependencies once. The same manager owns every collaborator's transaction.
	type createRecord func(context.Context, idem.Request) (idem.Result, error)
	wire := func(primary *pgxpool.Pool, options idem.Options) (createRecord, error) {
		manager := postgres.New(primary)
		engine, err := idem.New(manager, options)
		if err != nil {
			return nil, err
		}
		publisher, err := outbox.NewPublisher(manager, time.Now)
		if err != nil {
			return nil, err
		}
		guard, err := tombstone.New(manager, primary, time.Now)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, request idem.Request) (idem.Result, error) {
			completion, cancel := engine.WithCompletion(ctx)
			defer cancel() // includes the outer commit, not just Engine.Do
			var result idem.Result
			err := manager.Run(completion, func(txCtx context.Context) error {
				var err error
				result, err = engine.Do(txCtx, request, func(effectCtx context.Context) (idem.Response, error) {
					alive, err := guard.Guard(effectCtx, request.Scope) // verified user, never request-body identity
					if err != nil {
						return idem.Response{}, err
					}
					if !alive {
						return idem.Response{}, finerr.TokenRevoked()
					}
					tx, _ := manager.Tx(effectCtx)
					if _, err = tx.Exec(effectCtx, `INSERT INTO service_records(owner_id,catalog_id) VALUES($1,$2)`, request.Scope, 1); err != nil {
						return idem.Response{}, err
					}
					if err = publisher.Publish(effectCtx, "record.created", request.Scope, []byte("event")); err != nil {
						return idem.Response{}, err
					}
					return idem.Response{Body: []byte("created"), OwnerID: request.Scope}, nil
				})
				if err == nil && result.Replayed && result.Err == nil {
					alive, guardErr := guard.Guard(txCtx, request.Scope)
					if guardErr != nil {
						return guardErr
					}
					if !alive {
						return finerr.TokenRevoked()
					}
				}
				return err // Never return result.Err here: a cacheable refusal must commit.
			})
			// The transport handles ErrNotStored after rollback, result.Err after commit,
			// and replay-owner authorization before responding. Unknown errors (including
			// raw COMMIT failures) become bare INTERNAL, never err.Error().
			return result, err
		}, nil
	}
	_ = wire
}
