package idempotency_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/types/known/emptypb"

	idem "github.com/fin-assist/go-idempotency"
)

type serviceTransactions interface {
	idem.Transactions
	Run(context.Context, func(context.Context) error) error
}

func ExampleEngine_Do() {
	execute := func(ctx context.Context, db serviceTransactions, engine *idem.Engine, request idem.Request) (idem.Result, error) {
		completion, cancel := engine.WithCompletion(ctx)
		defer cancel()
		var result idem.Result
		err := db.Run(completion, func(txCtx context.Context) error {
			var err error
			result, err = engine.Do(txCtx, request, func(effectCtx context.Context) (idem.Response, error) {
				// Write the effect using db.Tx(effectCtx), then marshal the protobuf reply.
				return idem.Response{Body: []byte{}}, nil
			})
			return err // result.Err is returned to the client only AFTER commit.
		})
		return result, err
	}
	_ = execute
}
func ExampleNew() {
	wire := func(db serviceTransactions) (*idem.Engine, error) {
		return idem.New(db, idem.Options{Methods: map[string]idem.Policy{"/my.Service/Create": {}}, Clock: time.Now, Registerer: prometheus.NewRegistry()})
	}
	_ = wire
}
func ExampleNewCipher() {
	// The service loads the key from its secret manager before constructing the library.
	cipher, err := idem.NewCipher(make([]byte, 32))
	fmt.Println(cipher.NonceSize(), err)
	// Output: 12 <nil>
}
func ExampleHashProto() {
	hash, err := idem.HashProto(&emptypb.Empty{})
	fmt.Printf("%x %v\n", hash, err)
	// Output: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 <nil>
}
func ExampleHashFile() {
	hash, err := idem.HashFile(strings.NewReader("abc"), 3)
	fmt.Printf("%x %v\n", hash, err)
	// Output: ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad <nil>
}
func ExampleEngine_Reserve() {
	reserve := func(ctx context.Context, db serviceTransactions, e *idem.Engine, r idem.Request) (idem.Reservation, idem.Result, error) {
		var token idem.Reservation
		var result idem.Result
		err := db.Run(ctx, func(txCtx context.Context) error { var err error; token, result, err = e.Reserve(txCtx, r); return err })
		// After commit: return a replay, or check consent remotely before DoReserved.
		return token, result, err
	}
	_ = reserve
}
func ExampleEngine_Cleanup() {
	cleanup := func(ctx context.Context, db serviceTransactions, e *idem.Engine) error {
		return db.Run(ctx, func(txCtx context.Context) error { _, err := e.Cleanup(txCtx, 1000); return err })
	}
	_ = cleanup
}

func ExampleEngine_DoReserved() {
	finish := func(ctx context.Context, db serviceTransactions, e *idem.Engine, token idem.Reservation) (idem.Result, error) {
		completion, cancel := e.WithCompletion(ctx)
		defer cancel()
		var result idem.Result
		err := db.Run(completion, func(txCtx context.Context) error {
			var err error
			result, err = e.DoReserved(txCtx, token, func(effectCtx context.Context) (idem.Response, error) {
				// Check the local limit and persist the question using effectCtx.
				return idem.Response{}, nil
			})
			return err
		})
		return result, err
	}
	_ = finish
}
func ExampleEngine_Release() {
	release := func(ctx context.Context, db serviceTransactions, e *idem.Engine, token idem.Reservation) error {
		if token == (idem.Reservation{}) {
			return nil
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		return db.Run(cleanup, func(txCtx context.Context) error { return e.Release(txCtx, token) })
	}
	_ = release
}
func ExampleEngine_UnaryInterceptor() {
	configure := func(e *idem.Engine) {
		interceptor := e.UnaryInterceptor(func(ctx context.Context) (string, error) {
			// In a no-user AuthService method authentication has verified the caller.
			// User methods instead read a verified user UUID from the identity context.
			return "", nil
		})
		_ = interceptor // Install after identity and tombstone interceptors.
	}
	_ = configure
}

func ExampleEngine_ReleaseIfCurrent() {
	compensate := func(ctx context.Context, db serviceTransactions, e *idem.Engine, token idem.Reservation, refund func(context.Context) error) error {
		return db.Run(ctx, func(txCtx context.Context) error {
			released, err := e.ReleaseIfCurrent(txCtx, token)
			if err != nil || !released {
				return err
			}
			return refund(txCtx) // A failure rolls back BOTH the release and refund.
		})
	}
	_ = compensate
}

func ExampleResponse() {
	// Set the owner only after verifying the secret and finding/creating the account
	// inside the effect. Keep Request.Scope empty for no-user methods.
	response := idem.Response{Body: []byte("serialized reply"), OwnerID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}
	fmt.Println(response.OwnerID)
	// Output: aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa
}

func ExampleEngine_BindResource() {
	claim := func(ctx context.Context, db serviceTransactions, e *idem.Engine, request idem.Request, create func(context.Context) (string, error)) (idem.Reservation, idem.Result, error) {
		var token idem.Reservation
		var result idem.Result
		err := db.Run(ctx, func(ctx context.Context) error {
			var err error
			token, result, err = e.Reserve(ctx, request)
			if err != nil || result.Replayed {
				return err
			}
			id, err := create(ctx) // Local claim only; no network inside this transaction.
			if err != nil {
				return err
			}
			return e.BindResource(ctx, token, idem.Resource{Type: "upload", ID: id})
		})
		return token, result, err // External work only after successful commit.
	}
	_ = claim
}
