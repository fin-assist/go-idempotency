package idempotency

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	userv1 "github.com/fin-assist/contracts/gen/go/finassist/user/v1"
	"github.com/fin-assist/contracts/gen/go/finerr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type resourceUsageServer struct {
	userv1.UnimplementedAuthServiceServer
	e  *Engine
	db *serviceDB
}

func (s *resourceUsageServer) StartPhoneVerification(ctx context.Context, in *userv1.StartPhoneVerificationRequest) (*userv1.StartPhoneVerificationResponse, error) {
	r, ok := FromContext(ctx)
	if !ok {
		return nil, finerr.InternalError()
	}
	var err error
	r.Hash, err = HashProto(in)
	if err != nil {
		return nil, contractError(err)
	}
	var token Reservation
	var result Result
	var resource Resource
	err = s.db.Run(ctx, func(ctx context.Context) error {
		var err error
		token, result, err = s.e.Reserve(ctx, r)
		if err != nil || result.Replayed {
			return err
		}
		tx, _ := s.db.Tx(ctx)
		var id int
		if in.GetPhone() == "busy" {
			err = tx.QueryRow(ctx, `SELECT id FROM effects ORDER BY id LIMIT 1`).Scan(&id)
		} else {
			err = tx.QueryRow(ctx, `INSERT INTO effects DEFAULT VALUES RETURNING id`).Scan(&id)
		}
		if err != nil {
			return err
		}
		resource = Resource{Type: "verification", ID: strconv.Itoa(id)}
		return s.e.BindResource(ctx, token, resource)
	})
	if err != nil {
		return nil, contractError(err)
	}
	if !result.Replayed {
		// The linked reservation is now committed; external work would run here.
		result, err = finalize(s.e, s.db, token, func(context.Context) (Response, error) {
			if in.GetPhone() == "busy" {
				retry := s.e.now().Add(time.Minute)
				return Response{DuplicateRetryAt: retry}, finerr.RateLimited(retry)
			}
			body, err := proto.Marshal(&userv1.StartPhoneVerificationResponse{VerificationToken: "secret:" + resource.ID, CallbackNumber: "+74951234567"})
			return Response{Body: body}, err // The previously bound resource is inherited.
		})
	}
	if err != nil {
		return nil, contractError(err)
	}
	if result.Err != nil {
		return nil, result.Err
	}
	out := new(userv1.StartPhoneVerificationResponse)
	if err = proto.Unmarshal(result.Response.Body, out); err != nil {
		return nil, finerr.InternalError()
	}
	return out, nil
}

func TestIntegrationUsageResourceLinksSuccessBusyReplayAndScopedPurge(t *testing.T) {
	e, db, _ := setup(t)
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.UnaryInterceptor(e.UnaryInterceptor(func(context.Context) (string, error) { return "", nil })))
	userv1.RegisterAuthServiceServer(server, &resourceUsageServer{e: e, db: db})
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///resource", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := userv1.NewAuthServiceClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	call := func(key, phone string) (*userv1.StartPhoneVerificationResponse, error) {
		return c.StartPhoneVerification(metadata.AppendToOutgoingContext(ctx, "idempotency-key", key), &userv1.StartPhoneVerificationRequest{Phone: phone})
	}
	first, err := call(testKey, "phone")
	if err != nil {
		t.Fatal(err)
	}
	again, err := call(testKey, "phone")
	if err != nil || !proto.Equal(first, again) {
		t.Fatal(again, err)
	}
	_, err = call(testKey, "other")
	assertReason(t, err, "IDEMPOTENCY_CONFLICT")
	busyKey := "22222222-2222-4222-8222-222222222222"
	for range 2 {
		_, err = call(busyKey, "busy")
		assertReason(t, err, "RATE_LIMITED")
	}
	var id string
	var keys int
	if err = db.pool.QueryRow(ctx, `SELECT min(resource_id),count(*) FROM idempotency_keys WHERE resource_type='verification' AND owner_id IS NULL`).Scan(&id, &keys); err != nil || keys != 2 {
		t.Fatal(id, keys, err)
	}
	assertCount(t, db, 1)
	// Purge links and the business object in one service-owned transaction. A
	// real service acquires its resource/user guard before this NOWAIT key lock.
	err = db.Run(ctx, func(ctx context.Context) error {
		tx, _ := db.Tx(ctx)
		rows, err := tx.Query(ctx, `SELECT 1 FROM idempotency_keys WHERE scope=$1 AND resource_type=$2 AND resource_id=$3 FOR UPDATE NOWAIT`, nilScope, "verification", id)
		if err != nil {
			return err
		}
		for rows.Next() {
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `DELETE FROM idempotency_keys WHERE scope=$1 AND resource_type=$2 AND resource_id=$3`, nilScope, "verification", id)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 2 {
			return fmt.Errorf("expected success and busy links")
		}
		_, err = tx.Exec(ctx, `DELETE FROM effects WHERE id=$1`, id)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	after, err := call(testKey, "phone")
	if err != nil || after.GetVerificationToken() == first.GetVerificationToken() {
		t.Fatal("purge replayed removed resource", after, err)
	}
	assertCount(t, db, 1)
}
