package idempotency

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPreflightCancellationSources(t *testing.T) {
	for _, source := range []string{"request", "completion", "cleanup", "already cancelled"} {
		t.Run(source, func(t *testing.T) {
			original, stopRequest := context.WithCancel(context.Background())
			defer stopRequest()
			completion, stopCompletion := context.WithCancel(context.Background())
			defer stopCompletion()
			if source == "already cancelled" {
				stopRequest()
			}
			preflight, cleanup := preflightContext(completion, original)
			defer cleanup()
			switch source {
			case "request":
				stopRequest()
			case "completion":
				stopCompletion()
			case "cleanup":
				cleanup()
			case "already cancelled":
				if !errors.Is(preflight.Err(), context.Canceled) {
					t.Fatal("already cancelled request was not observed synchronously")
				}
			}
			select {
			case <-preflight.Done():
			case <-time.After(time.Second):
				t.Fatal("preflight did not stop")
			}
			if !errors.Is(preflight.Err(), context.Canceled) {
				t.Fatal(preflight.Err())
			}
			if source != "completion" && completion.Err() != nil {
				t.Fatal("preflight cancellation propagated to effect/commit context")
			}
			if source == "cleanup" && original.Err() != nil {
				t.Fatal("cleanup cancelled the request")
			}
		})
	}
}

func TestPreflightUsesEarlierDeadline(t *testing.T) {
	for _, requestFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "completion", true: "request"}[requestFirst], func(t *testing.T) {
			earlier := time.Now().Add(-time.Second)
			later := time.Now().Add(time.Hour)
			requestDeadline, completionDeadline := later, earlier
			if requestFirst {
				requestDeadline, completionDeadline = earlier, later
			}
			original, stopRequest := context.WithDeadline(context.Background(), requestDeadline)
			defer stopRequest()
			completion, stopCompletion := context.WithDeadline(context.Background(), completionDeadline)
			defer stopCompletion()
			preflight, cleanup := preflightContext(completion, original)
			defer cleanup()
			deadline, ok := preflight.Deadline()
			if !ok || !deadline.Equal(earlier) || !errors.Is(preflight.Err(), context.DeadlineExceeded) {
				t.Fatalf("deadline=%v ok=%v err=%v", deadline, ok, preflight.Err())
			}
		})
	}
}
