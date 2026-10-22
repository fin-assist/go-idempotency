package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fin-assist/contracts/gen/go/finerr"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
)

// The HTTP adapter uses only public library APIs; validation and HTTP formatting
// belong to the service. In particular, it cannot call the gRPC invalidKey helper.
func httpKey(values []string) (string, error) {
	if len(values) == 1 {
		key := values[0]
		if len(key) == 36 && key[8] == '-' && key[13] == '-' && key[18] == '-' && key[23] == '-' {
			decoded, err := hex.DecodeString(strings.ReplaceAll(key, "-", ""))
			if err == nil && len(decoded) == 16 {
				return key, nil
			}
		}
	}
	return "", finerr.ValidationError(&errdetails.BadRequest_FieldViolation{Field: "Idempotency-Key", Reason: "INVALID_FORMAT", Description: "A single UUID is required"})
}

func httpError(w http.ResponseWriter, err error) {
	rest, ok := finerr.ToREST(contractError(err))
	if !ok {
		rest, _ = finerr.ToREST(finerr.InternalError())
	}
	if rest.RetryAfter > 0 {
		seconds := int64(rest.RetryAfter / time.Second)
		if rest.RetryAfter%time.Second != 0 {
			seconds++
		}
		w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
	}
	details := make([]map[string]string, 0, len(rest.Violations))
	for _, v := range rest.Violations {
		details = append(details, map[string]string{"field": v.GetField(), "code": v.GetReason(), "message": v.GetDescription()})
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(rest.Spec.HTTPStatus)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": rest.Code, "details": details, "metadata": rest.Metadata}})
}

func TestUsageHTTPHandlerUsesTransportIndependentDo(t *testing.T) {
	_, db, clock := setup(t)
	const operation = "POST /v1/actions"
	e, err := New(db, Options{Methods: map[string]Policy{operation: {}}, Clock: clock.Now, Registerer: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	var executions atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc(operation, func(w http.ResponseWriter, r *http.Request) {
		key, err := httpKey(r.Header.Values("Idempotency-Key"))
		if err != nil {
			httpError(w, err)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024))
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				httpError(w, finerr.FileTooLarge())
			} else {
				httpError(w, finerr.ValidationError(&errdetails.BadRequest_FieldViolation{Field: "body", Reason: "INVALID_FORMAT"}))
			}
			return
		}
		var input struct{ Name string }
		if err := json.Unmarshal(body, &input); err != nil || input.Name == "" {
			httpError(w, finerr.ValidationError(&errdetails.BadRequest_FieldViolation{Field: "name", Reason: "REQUIRED"}))
			return
		}
		canonical, err := json.Marshal(input)
		if err != nil {
			httpError(w, finerr.InternalError())
			return
		}
		// Authentication precedes this handler; its verified test user is fixed.
		request := Request{Scope: userA, Endpoint: operation, Key: key, Hash: sha256.Sum256(canonical)}
		result, err := runDo(e, db, r.Context(), request, func(ctx context.Context) (Response, error) {
			executions.Add(1)
			if _, err := effect(db)(ctx); err != nil {
				return Response{}, err
			}
			switch input.Name {
			case "bad-format":
				return Response{}, finerr.WrongFormat()
			case "missing":
				return Response{}, finerr.NotFound()
			case "no-consent":
				return Response{}, finerr.ConsentRequired()
			}
			return Response{Body: []byte("{\"created\":true}"), Headers: map[string][]string{"content-type": {"application/json"}}}, nil
		})
		if errors.Is(err, ErrNotStored) {
			httpError(w, result.Err)
			return
		}
		if err != nil {
			httpError(w, err)
			return
		}
		if result.Err != nil {
			httpError(w, result.Err)
			return
		}
		for key, values := range result.Response.Headers {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(result.Response.Body)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := server.Client()
	client.Timeout = 5 * time.Second
	call := func(body string, keys ...string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/v1/actions", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			req.Header.Add("Idempotency-Key", key)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.Header.Get("Content-Type") != "application/json" {
			t.Fatal(response.Header)
		}
		return response.StatusCode, data
	}
	for i, body := range []string{"{\"name\":\"a\"}", "{\"name\" : \"a\"}", "{\"name\":\"b\"}"} {
		code, data := call(body, testKey)
		if i < 2 {
			if code != http.StatusCreated || string(data) != "{\"created\":true}" {
				t.Fatal(code, string(data))
			}
		} else if code != http.StatusConflict || !bytes.Contains(data, []byte("IDEMPOTENCY_CONFLICT")) {
			t.Fatal(code, string(data))
		}
	}
	assertCount(t, db, 1)
	if executions.Load() != 1 {
		t.Fatal(executions.Load())
	}

	savedKey := "11111111-1111-4111-8111-222222222222"
	var saved []byte
	for i := range 2 {
		code, data := call("{\"name\":\"bad-format\"}", savedKey)
		if code != http.StatusUnprocessableEntity || !bytes.Contains(data, []byte("WRONG_FORMAT")) {
			t.Fatal(code, string(data))
		}
		if i == 0 {
			saved = data
		} else if !bytes.Equal(data, saved) {
			t.Fatal("saved error changed", string(data))
		}
	}
	assertCount(t, db, 2)
	if executions.Load() != 2 {
		t.Fatal(executions.Load())
	}

	for _, tc := range []struct {
		name, key, reason string
		code              int
	}{
		{"missing", "11111111-1111-4111-8111-333333333333", "NOT_FOUND", http.StatusNotFound},
		{"no-consent", "11111111-1111-4111-8111-444444444444", "CONSENT_REQUIRED", http.StatusForbidden},
	} {
		for range 2 {
			code, data := call("{\"name\":\""+tc.name+"\"}", tc.key)
			if code != tc.code || !bytes.Contains(data, []byte(tc.reason)) {
				t.Fatal(code, string(data))
			}
		}
	}
	assertCount(t, db, 2)
	if executions.Load() != 6 {
		t.Fatal("non-stored outcome was replayed", executions.Load())
	}

	for _, keys := range [][]string{nil, {"abc"}, {testKey, testKey}} {
		code, data := call("{\"name\":\"a\"}", keys...)
		if code != http.StatusBadRequest || !bytes.Contains(data, []byte("VALIDATION_ERROR")) {
			t.Fatal(code, string(data))
		}
	}
	assertCount(t, db, 2)
	if executions.Load() != 6 {
		t.Fatal("invalid key ran effect", executions.Load())
	}
}
