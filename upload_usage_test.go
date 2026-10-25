package idempotency

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	statementv1 "github.com/fin-assist/contracts/gen/go/finassist/statementdata/v1"
	"github.com/fin-assist/contracts/gen/go/finerr"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

const uploadMethod = "/finassist.statementdata.v1.StatementsService/UploadStatement"
const uploadMaxBytes = 16

// chunkReader turns the chunks after the header into an io.Reader for HashFile.
type chunkReader struct {
	stream grpc.ClientStreamingServer[statementv1.UploadStatementRequest, statementv1.UploadStatementResponse]
	rest   []byte
}

func (r *chunkReader) Read(p []byte) (int, error) {
	for len(r.rest) == 0 {
		msg, err := r.stream.Recv()
		if err != nil {
			return 0, err // io.EOF ends the file
		}
		if msg.GetHeader() != nil {
			return 0, finerr.InternalError()
		}
		r.rest = msg.GetChunk()
	}
	n := copy(p, r.rest)
	r.rest = r.rest[n:]
	return n, nil
}

// uploadServer follows README: StreamInterceptor -> header -> streamed content
// hash -> HashUpload at the end of the stream -> Do in the service transaction.
type uploadServer struct {
	statementv1.UnimplementedStatementsServiceServer
	e       *Engine
	db      *serviceDB
	handled atomic.Int32
}

func (s *uploadServer) UploadStatement(stream grpc.ClientStreamingServer[statementv1.UploadStatementRequest, statementv1.UploadStatementResponse]) error {
	s.handled.Add(1)
	r, ok := FromContext(stream.Context())
	if !ok {
		return finerr.InternalError()
	}
	first, err := stream.Recv()
	if err != nil || first.GetHeader() == nil {
		return finerr.InternalError()
	}
	header := first.GetHeader()
	if header.GetSizeBytes() > uploadMaxBytes {
		return finerr.FileTooLarge() // before the key lookup: not stored
	}
	content, err := HashFile(&chunkReader{stream: stream}, uploadMaxBytes)
	if err != nil {
		return contractError(err)
	}
	r.Hash = HashUpload(content, header.GetFileName())
	result, err := runDo(s.e, s.db, stream.Context(), r, func(ctx context.Context) (Response, error) {
		tx, _ := s.db.Tx(ctx)
		var id int
		if err := tx.QueryRow(ctx, `INSERT INTO effects DEFAULT VALUES RETURNING id`).Scan(&id); err != nil {
			return Response{}, err
		}
		body, err := proto.Marshal(&statementv1.UploadStatementResponse{UploadId: fmt.Sprintf("upload-%d", id), FileName: header.GetFileName()})
		return Response{Body: body}, err
	})
	if err != nil {
		if errors.Is(err, ErrNotStored) {
			return result.Err
		}
		return contractError(err)
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

func upload(ctx context.Context, client statementv1.StatementsServiceClient, name string, size int64, chunks ...string) (*statementv1.UploadStatementResponse, error) {
	stream, err := client.UploadStatement(ctx)
	if err != nil {
		return nil, err
	}
	if err = stream.Send(&statementv1.UploadStatementRequest{Part: &statementv1.UploadStatementRequest_Header{Header: &statementv1.UploadHeader{FileName: name, ContentType: "application/x-ofx", SizeBytes: size}}}); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	for _, c := range chunks {
		if err = stream.Send(&statementv1.UploadStatementRequest{Part: &statementv1.UploadStatementRequest_Chunk{Chunk: []byte(c)}}); err != nil {
			break // the server already answered; CloseAndRecv returns its status
		}
	}
	return stream.CloseAndRecv()
}

func TestIntegrationUsageClientStreamingUploadReplayAndNameConflict(t *testing.T) {
	_, db, clock := setup(t)
	e, err := New(db, Options{Methods: map[string]Policy{uploadMethod: {}}, Clock: clock.Now, LockTimeout: 40 * time.Millisecond, Registerer: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(grpc.ChainStreamInterceptor(e.StreamInterceptor(func(context.Context) (string, error) {
		return "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", nil // stands in for the verified user
	})))
	service := &uploadServer{e: e, db: db}
	statementv1.RegisterStatementsServiceServer(srv, service)
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///upload", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := statementv1.NewStatementsServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A missing key is refused when the stream opens: the handler never runs.
	_, err = upload(ctx, client, "a.ofx", 6, "OFX", "DATA")
	assertReason(t, err, "VALIDATION_ERROR")
	if service.handled.Load() != 0 {
		t.Fatal("handler ran without a key")
	}
	keyed := metadata.AppendToOutgoingContext(ctx, "idempotency-key", testKey)
	first, err := upload(keyed, client, "a.ofx", 7, "OFX", "DATA")
	if err != nil || first.GetFileName() != "a.ofx" {
		t.Fatal(first, err)
	}
	// Chunk boundaries do not matter: the same bytes and name replay.
	second, err := upload(keyed, client, "a.ofx", 7, "OF", "XDA", "TA")
	if err != nil || second.GetUploadId() != first.GetUploadId() || second.GetFileName() != "a.ofx" {
		t.Fatal(second, err)
	}
	assertCount(t, db, 1)
	// Same key and content under another name: a conflict, not the old name.
	_, err = upload(keyed, client, "b.ofx", 7, "OFX", "DATA")
	assertReason(t, err, "IDEMPOTENCY_CONFLICT")
	if status.Code(err) != codes.AlreadyExists {
		t.Fatal(err)
	}
	_, err = upload(keyed, client, "a.ofx", 7, "OFX", "DATX")
	assertReason(t, err, "IDEMPOTENCY_CONFLICT")
	// Oversize ends before the key lookup and is not stored under the key.
	big := metadata.AppendToOutgoingContext(ctx, "idempotency-key", "22222222-2222-4222-8222-222222222222")
	for range 2 {
		_, err = upload(big, client, "big.ofx", 7, "0123456789", "0123456789")
		assertReason(t, err, "FILE_TOO_LARGE")
	}
	var stored int
	if err = db.pool.QueryRow(ctx, "SELECT count(*) FROM idempotency_keys WHERE key='22222222-2222-4222-8222-222222222222'").Scan(&stored); err != nil || stored != 0 {
		t.Fatal("oversize outcome stored:", stored, err)
	}
	assertCount(t, db, 1)
}
