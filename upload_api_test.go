package idempotency_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	statementv1 "github.com/fin-assist/contracts/gen/go/finassist/statementdata/v1"
	"github.com/fin-assist/contracts/gen/go/finerr"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	idem "github.com/fin-assist/go-idempotency"
)

type verifiedUser struct{}

type identityStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s identityStream) Context() context.Context { return s.ctx }

// consumerIdentity stands in for the service's verified-token interceptor: it
// maps a test bearer value to a user and refuses everything else.
func consumerIdentity(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
	md, _ := metadata.FromIncomingContext(ss.Context())
	users := map[string]string{"token-owner": publicOwner, "token-foreign": publicForeign}
	auth := md.Get("authorization")
	if len(auth) != 1 || users[auth[0]] == "" {
		return finerr.InvalidToken()
	}
	return next(srv, identityStream{ServerStream: ss, ctx: context.WithValue(ss.Context(), verifiedUser{}, users[auth[0]])})
}

type consumerUpload struct {
	statementv1.UnimplementedStatementsServiceServer
	db     consumerDB
	engine *idem.Engine
}

func (s consumerUpload) UploadStatement(stream grpc.ClientStreamingServer[statementv1.UploadStatementRequest, statementv1.UploadStatementResponse]) error {
	request, ok := idem.FromContext(stream.Context())
	if !ok {
		return finerr.InternalError()
	}
	first, err := stream.Recv()
	if err != nil || first.GetHeader() == nil {
		return finerr.InternalError()
	}
	content := sha256.New()
	var size int64
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		size += int64(len(msg.GetChunk()))
		if size > 1<<10 {
			return finerr.FileTooLarge()
		}
		content.Write(msg.GetChunk())
	}
	var contentHash [32]byte
	copy(contentHash[:], content.Sum(nil))
	request.Hash = idem.HashUpload(contentHash, first.GetHeader().GetFileName())
	owner := request.Scope
	result, err := consumerDo(stream.Context(), s.db, s.engine, request, func(ctx context.Context) (idem.Response, error) {
		tx, ok := s.db.Tx(ctx)
		if !ok {
			return idem.Response{}, idem.ErrNoTransaction
		}
		if _, err := tx.Exec(ctx, "INSERT INTO consumer_effects VALUES($1)", owner); err != nil {
			return idem.Response{}, err
		}
		body, err := proto.Marshal(&statementv1.UploadStatementResponse{UploadId: "upload-of-" + owner, FileName: first.GetHeader().GetFileName()})
		return idem.Response{Body: body}, err
	})
	if err != nil {
		// Library contract errors (IDEMPOTENCY_CONFLICT, REQUEST_IN_PROGRESS) keep
		// their status; everything else becomes a bare internal error.
		var contract interface{ GRPCStatus() *status.Status }
		if _, ok := finerr.ToREST(err); ok && errors.As(err, &contract) {
			return contract.GRPCStatus().Err()
		}
		return finerr.InternalError()
	}
	if result.Err != nil {
		return result.Err
	}
	out := new(statementv1.UploadStatementResponse)
	if err = proto.Unmarshal(result.Response.Body, out); err != nil {
		return finerr.InternalError()
	}
	return stream.SendAndClose(out)
}

func consumerSend(ctx context.Context, client statementv1.StatementsServiceClient, name string, chunks ...string) (*statementv1.UploadStatementResponse, error) {
	stream, err := client.UploadStatement(ctx)
	if err != nil {
		return nil, err
	}
	if err = stream.Send(&statementv1.UploadStatementRequest{Part: &statementv1.UploadStatementRequest_Header{Header: &statementv1.UploadHeader{FileName: name, SizeBytes: 9}}}); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	for _, c := range chunks {
		if err = stream.Send(&statementv1.UploadStatementRequest{Part: &statementv1.UploadStatementRequest_Chunk{Chunk: []byte(c)}}); err != nil {
			break
		}
	}
	return stream.CloseAndRecv()
}

func consumerCode(t *testing.T, err error, want string) {
	t.Helper()
	if rest, ok := finerr.ToREST(err); !ok || rest.Code != want {
		t.Fatalf("code=%v want=%s err=%v", rest.Code, want, err)
	}
}

func TestAPIConsumerUploadStatementStreamScopesReplaysAndConflicts(t *testing.T) {
	ctx, db, _, now := publicEngine(t)
	engine, err := idem.New(db, idem.Options{Methods: map[string]idem.Policy{statementv1.StatementsService_UploadStatement_FullMethodName: {}}, Clock: func() time.Time { return *now }, Registerer: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	scope := func(ctx context.Context) (string, error) {
		user, _ := ctx.Value(verifiedUser{}).(string)
		if user == "" {
			return "", finerr.InvalidToken() // fail closed without identity
		}
		return user, nil
	}
	server := grpc.NewServer(grpc.ChainStreamInterceptor(consumerIdentity, engine.StreamInterceptor(scope)))
	statementv1.RegisterStatementsServiceServer(server, consumerUpload{db: db, engine: engine})
	listener := bufconn.Listen(1 << 20)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///consumer", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := statementv1.NewStatementsServiceClient(conn)

	owner := metadata.AppendToOutgoingContext(ctx, "authorization", "token-owner")
	_, err = consumerSend(metadata.AppendToOutgoingContext(owner, "idempotency-key", "not-a-uuid"), client, "a.ofx", "OFX")
	consumerCode(t, err, "VALIDATION_ERROR")
	_, err = consumerSend(metadata.AppendToOutgoingContext(ctx, "idempotency-key", publicKey), client, "a.ofx", "OFX")
	consumerCode(t, err, "INVALID_TOKEN")
	requireEffects(t, ctx, db, 0)

	keyed := metadata.AppendToOutgoingContext(owner, "idempotency-key", publicKey)
	first, err := consumerSend(keyed, client, "Выписка.ofx", "OFX", "DATA")
	if err != nil || first.GetUploadId() != "upload-of-"+publicOwner || first.GetFileName() != "Выписка.ofx" {
		t.Fatal(first, err)
	}
	replay, err := consumerSend(keyed, client, "Выписка.ofx", "OFXDATA")
	if err != nil || !proto.Equal(replay, first) {
		t.Fatal(replay, err)
	}
	_, err = consumerSend(keyed, client, "выписка.ofx", "OFX", "DATA")
	consumerCode(t, err, "IDEMPOTENCY_CONFLICT")
	requireEffects(t, ctx, db, 1)

	// Another user with the same key and file gets its own upload, never the owner's.
	foreign := metadata.AppendToOutgoingContext(ctx, "authorization", "token-foreign", "idempotency-key", publicKey)
	theirs, err := consumerSend(foreign, client, "Выписка.ofx", "OFX", "DATA")
	if err != nil || theirs.GetUploadId() != "upload-of-"+publicForeign {
		t.Fatal(theirs, err)
	}
	requireEffects(t, ctx, db, 2)
}
