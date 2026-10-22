package idempotency

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/fin-assist/contracts/gen/go/finerr"
	"github.com/prometheus/client_golang/prometheus"
)

func TestUsageHTTPHandlerUsesTransportIndependentDo(t *testing.T) {
	_, db, clock := setup(t)
	const operation = "POST /v1/actions"
	e, err := New(db, Options{Methods: map[string]Policy{operation: {}}, Clock: clock.Now, Registerer: prometheus.NewRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(operation, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024))
		if err != nil {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		var input struct{ Name string }
		if err := json.Unmarshal(body, &input); err != nil || input.Name == "" {
			http.Error(w, "invalid body", http.StatusBadRequest)
			return
		}
		canonical, err := json.Marshal(input)
		if err != nil {
			http.Error(w, "INTERNAL_ERROR", http.StatusInternalServerError)
			return
		}
		keys := r.Header.Values("Idempotency-Key")
		if len(keys) != 1 {
			http.Error(w, "invalid key", http.StatusBadRequest)
			return
		}
		// Authentication precedes this handler; its verified test user is fixed.
		request := Request{Scope: userA, Endpoint: operation, Key: keys[0], Hash: sha256.Sum256(canonical)}
		result, err := runDo(e, db, r.Context(), request, func(ctx context.Context) (Response, error) {
			if _, err := effect(db)(ctx); err != nil {
				return Response{}, err
			}
			return Response{Body: []byte("{\"created\":true}"), Headers: map[string][]string{"content-type": {"application/json"}}}, nil
		})
		if err != nil {
			if rest, ok := finerr.ToREST(err); ok {
				http.Error(w, rest.Code, rest.Spec.HTTPStatus)
				return
			}
			http.Error(w, "INTERNAL_ERROR", http.StatusInternalServerError)
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
	for i, body := range []string{"{\"name\":\"a\"}", "{\"name\" : \"a\"}", "{\"name\":\"b\"}"} {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/v1/actions", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Idempotency-Key", testKey)
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			if response.StatusCode != http.StatusCreated || string(data) != "{\"created\":true}" || response.Header.Get("Content-Type") != "application/json" {
				t.Fatal(response.StatusCode, string(data))
			}
		} else if response.StatusCode != http.StatusConflict || !bytes.Contains(data, []byte("IDEMPOTENCY_CONFLICT")) {
			t.Fatal(response.StatusCode, string(data))
		}
	}
	assertCount(t, db, 1)
}
