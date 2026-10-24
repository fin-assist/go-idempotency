package idempotency

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/fin-assist/contracts/gen/go/finerr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type rowScan func(...any) error

func (f rowScan) Scan(dest ...any) error { return f(dest...) }

type adapterTx struct {
	pgx.Tx
	query func(string, []any) pgx.Row
	exec  func(string, []any) (pgconn.CommandTag, error)
}

func (tx *adapterTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	return tx.query(sql, args)
}
func (tx *adapterTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return tx.exec(sql, args)
}

type adapterTransactions struct{ tx pgx.Tx }

func (d adapterTransactions) Tx(context.Context) (pgx.Tx, bool) { return d.tx, true }
func storedRow(r record) pgx.Row {
	return rowScan(func(dest ...any) error {
		*dest[0].(*[]byte) = r.hash[:]
		*dest[1].(*bool) = r.done
		*dest[2].(*[]byte) = r.payload
		*dest[3].(*bool) = r.encrypted
		*dest[4].(*time.Time) = r.expires
		*dest[5].(*string) = r.reservation
		*dest[6].(*string) = r.owner
		if r.resource != (Resource{}) {
			kind, id := r.resource.Type, r.resource.ID
			*dest[7].(**string) = &kind
			*dest[8].(**string) = &id
		}
		return nil
	})
}
func timeoutRow() pgx.Row {
	return rowScan(func(dest ...any) error { *dest[0].(*string) = "7s"; return nil })
}
func TestUnitPerformPreservesOwnerAndFencesLostUpdates(t *testing.T) {
	for _, scenario := range []string{"success", "zero updated", "storage failure", "invalid resource", "invalid outcome", "uncacheable domain"} {
		t.Run(scenario, func(t *testing.T) {
			e := unitEngine(t)
			r, policy, err := e.validate(request())
			if err != nil {
				t.Fatal(err)
			}
			updates := 0
			tx := &adapterTx{exec: func(sql string, args []any) (pgconn.CommandTag, error) {
				updates++
				if !strings.Contains(sql, "SET done=true") || args[0] != r.Scope || args[1] != r.Endpoint || args[2] != r.Key || args[8] != resultOwner {
					t.Fatalf("incorrect persisted identity/owner: %s %+v", sql, args)
				}
				if scenario == "storage failure" {
					return pgconn.CommandTag{}, errors.New("private SQL")
				}
				if scenario == "zero updated" {
					return pgconn.NewCommandTag("UPDATE 0"), nil
				}
				return pgconn.NewCommandTag("UPDATE 1"), nil
			}}
			response := Response{OwnerID: strings.ToUpper(resultOwner), Body: []byte("answer")}
			bound := Resource{}
			if scenario == "invalid resource" {
				bound = Resource{Type: "answer", ID: "original"}
				response.Resource = Resource{Type: "answer", ID: "replacement"}
			}
			var domain error
			if scenario == "invalid outcome" {
				domain = errors.New("private effect")
			}
			if scenario == "uncacheable domain" {
				domain = context.Canceled
			}
			result, err := e.perform(context.Background(), tx, r, policy, "token", bound, func(context.Context) (Response, error) { return response, domain })
			switch scenario {
			case "success":
				if err != nil || result.Response.OwnerID != resultOwner || updates != 1 {
					t.Fatalf("result=%+v updates=%d err=%v", result, updates, err)
				}
			case "zero updated":
				assertReason(t, err, "REQUEST_IN_PROGRESS")
			case "storage failure", "invalid outcome", "uncacheable domain":
				if !errors.Is(err, ErrStorage) {
					t.Fatal(err)
				}
			case "invalid resource":
				if !errors.Is(err, ErrInvalidRequest) {
					t.Fatal(err)
				}
			}
			if scenario == "invalid resource" || scenario == "invalid outcome" || scenario == "uncacheable domain" {
				if updates != 0 {
					t.Fatal("invalid outcome was persisted")
				}
			}
		})
	}
}
func TestUnitClaimUsesScopedArgumentsAndRestoresLockTimeout(t *testing.T) {
	for _, scenario := range []string{"new", "existing", "expired", "missing after conflict", "bad insert", "bad lookup", "bad delete"} {
		t.Run(scenario, func(t *testing.T) {
			e := unitEngine(t)
			r, policy, _ := e.validate(request())
			inserts, reads, sets, deletes := 0, 0, 0, 0
			var tx adapterTx
			tx.query = func(sql string, args []any) pgx.Row {
				if strings.Contains(sql, "current_setting") {
					return timeoutRow()
				}
				if args[0] != r.Scope || args[1] != r.Endpoint || args[2] != r.Key {
					t.Fatal("unscoped key access")
				}
				if strings.HasPrefix(sql, "INSERT") {
					inserts++
					if scenario == "bad insert" {
						return rowScan(func(...any) error { return errors.New("secret SQL") })
					}
					if scenario == "new" || (scenario == "expired" && inserts == 2) || (scenario == "missing after conflict" && inserts == 2) {
						return rowScan(func(dest ...any) error { *dest[0].(*bool) = true; return nil })
					}
					return rowScan(func(...any) error { return pgx.ErrNoRows })
				}
				reads++
				if scenario == "bad lookup" {
					return rowScan(func(...any) error { return errors.New("secret SQL") })
				}
				if scenario == "missing after conflict" {
					return rowScan(func(...any) error { return pgx.ErrNoRows })
				}
				expiry := e.now().Add(time.Hour)
				if scenario == "expired" || scenario == "bad delete" {
					expiry = e.now().Add(-time.Second)
				}
				return storedRow(record{hash: r.Hash, expires: expiry, reservation: "original"})
			}
			tx.exec = func(sql string, args []any) (pgconn.CommandTag, error) {
				if strings.Contains(sql, "set_config") {
					sets++
					want := "1000ms"
					if sets == 2 {
						want = "7s"
					}
					if args[0] != want {
						t.Fatalf("lock timeout=%v want=%v", args[0], want)
					}
					return pgconn.NewCommandTag("SELECT 1"), nil
				}
				deletes++
				if scenario == "bad delete" {
					return pgconn.CommandTag{}, errors.New("private SQL")
				}
				if args[0] != r.Scope || args[1] != r.Endpoint || args[2] != r.Key {
					t.Fatal("unscoped delete")
				}
				return pgconn.NewCommandTag("DELETE 1"), nil
			}
			row, fresh, err := e.claim(context.Background(), &tx, r, policy.TTL, "")
			switch scenario {
			case "bad insert", "bad lookup", "bad delete":
				if !errors.Is(err, ErrStorage) {
					t.Fatal(err)
				}
			case "existing":
				if err != nil || fresh || row.reservation != "original" || reads != 1 || sets != 2 {
					t.Fatalf("existing fresh=%v row=%+v reads=%d sets=%d err=%v", fresh, row, reads, sets, err)
				}
			default:
				if err != nil || !fresh || sets != 2 {
					t.Fatalf("claim fresh=%v sets=%d err=%v", fresh, sets, err)
				}
			}
			if scenario == "expired" && deletes != 1 {
				t.Fatalf("expired deletes=%d", deletes)
			}
		})
	}
}
func TestUnitReservedCompletionAndConditionalRelease(t *testing.T) {
	for _, scenario := range []string{"complete", "replay", "missing", "expired", "foreign token", "release", "lost release", "bind", "replace resource"} {
		t.Run(scenario, func(t *testing.T) {
			e := unitEngine(t)
			r, _, _ := e.validate(request())
			token := Reservation{request: r, id: "ours"}
			row := record{hash: r.Hash, expires: e.now().Add(time.Hour), reservation: token.id}
			if scenario == "expired" {
				row.expires = e.now()
			}
			if scenario == "foreign token" {
				row.reservation = "theirs"
			}
			if scenario == "replay" {
				row.done = true
				row.payload, _, _ = e.encode(r, Policy{}, Result{Response: Response{Body: []byte("saved")}})
			}
			if scenario == "replace resource" {
				row.resource = Resource{Type: "operation", ID: "old"}
			}
			updated := 0
			tx := &adapterTx{query: func(sql string, args []any) pgx.Row {
				if strings.Contains(sql, "current_setting") {
					return timeoutRow()
				}
				if args[0] != r.Scope || args[1] != r.Endpoint || args[2] != r.Key {
					t.Fatal("unscoped lookup")
				}
				if scenario == "missing" {
					return rowScan(func(...any) error { return pgx.ErrNoRows })
				}
				return storedRow(row)
			}, exec: func(sql string, args []any) (pgconn.CommandTag, error) {
				if strings.Contains(sql, "set_config") {
					return pgconn.NewCommandTag("SELECT 1"), nil
				}
				updated++
				if args[0] != r.Scope || args[1] != r.Endpoint || args[2] != r.Key {
					t.Fatal("unscoped mutation")
				}
				if scenario == "lost release" {
					return pgconn.NewCommandTag("DELETE 0"), nil
				}
				return pgconn.NewCommandTag("UPDATE 1"), nil
			}}
			e.txs = adapterTransactions{tx}
			ctx, cancel := e.WithCompletion(context.Background())
			defer cancel()
			switch scenario {
			case "release", "lost release":
				released, err := e.ReleaseIfCurrent(ctx, token)
				if err != nil || released != (scenario == "release") || updated != 1 {
					t.Fatalf("release=%v updates=%d %v", released, updated, err)
				}
			case "bind", "replace resource":
				err := e.BindResource(ctx, token, Resource{Type: "operation", ID: "new"})
				if scenario == "bind" {
					if err != nil || updated != 1 {
						t.Fatalf("bind updates=%d %v", updated, err)
					}
				} else if !errors.Is(err, ErrInvalidRequest) || updated != 0 {
					t.Fatalf("replacement updates=%d %v", updated, err)
				}
			default:
				calls := 0
				result, err := e.DoReserved(ctx, token, func(context.Context) (Response, error) { calls++; return Response{Body: []byte("new")}, nil })
				switch scenario {
				case "complete":
					if err != nil || calls != 1 || updated != 1 || result.Replayed {
						t.Fatalf("complete %+v calls=%d updates=%d %v", result, calls, updated, err)
					}
				case "replay":
					if err != nil || calls != 0 || updated != 0 || !result.Replayed || string(result.Response.Body) != "saved" {
						t.Fatalf("replay %+v calls=%d updates=%d %v", result, calls, updated, err)
					}
				default:
					assertReason(t, err, "REQUEST_IN_PROGRESS")
					if calls != 0 || updated != 0 {
						t.Fatal("invalid reservation executed effect")
					}
				}
			}
		})
	}
}

func TestUnitPublicExecutionEntryPointsUseCallerTransaction(t *testing.T) {
	e := unitEngine(t)
	r, _, _ := e.validate(request())
	inserts, completed, released := 0, 0, 0
	tx := &adapterTx{query: func(sql string, args []any) pgx.Row {
		if strings.Contains(sql, "current_setting") {
			return timeoutRow()
		}
		if !strings.HasPrefix(sql, "INSERT INTO idempotency_keys") || args[0] != r.Scope || args[1] != r.Endpoint || args[2] != r.Key {
			t.Fatalf("unexpected claim %s %+v", sql, args)
		}
		inserts++
		return rowScan(func(dest ...any) error { *dest[0].(*bool) = true; return nil })
	}, exec: func(sql string, args []any) (pgconn.CommandTag, error) {
		switch {
		case strings.Contains(sql, "set_config"):
			return pgconn.NewCommandTag("SELECT 1"), nil
		case strings.HasPrefix(sql, "UPDATE"):
			completed++
			return pgconn.NewCommandTag("UPDATE 1"), nil
		case strings.HasPrefix(sql, "DELETE"):
			released++
			if args[0] != r.Scope || args[1] != r.Endpoint || args[2] != r.Key {
				t.Fatal("unscoped release")
			}
			return pgconn.NewCommandTag("DELETE 1"), nil
		case strings.HasPrefix(sql, "WITH expired"):
			if args[1] != 17 {
				t.Fatalf("cleanup limit=%v", args[1])
			}
			return pgconn.NewCommandTag("DELETE 3"), nil
		default:
			t.Fatalf("unexpected SQL %s", sql)
			return pgconn.CommandTag{}, nil
		}
	}}
	e.txs = adapterTransactions{tx}
	ctx, cancel := e.WithCompletion(context.Background())
	defer cancel()
	result, err := e.Do(ctx, r, func(context.Context) (Response, error) { return Response{Body: []byte("result")}, nil })
	if err != nil || result.Replayed || string(result.Response.Body) != "result" || completed != 1 {
		t.Fatalf("Do %+v updates=%d %v", result, completed, err)
	}
	token, result, err := e.Reserve(ctx, r)
	if err != nil || token.id == "" || token.request != r || result.Replayed || inserts != 2 {
		t.Fatalf("reserve %+v inserts=%d %v", result, inserts, err)
	}
	if err = e.Release(ctx, token); err != nil || released != 1 {
		t.Fatalf("release mutations=%d %v", released, err)
	}
	count, err := e.Cleanup(ctx, 17)
	if err != nil || count != 3 {
		t.Fatalf("cleanup count=%d %v", count, err)
	}
}

func TestUnitBindingAndTimeoutStorageFailures(t *testing.T) {
	for _, scenario := range []string{"missing binding", "binding update failed", "binding update lost", "timeout set failed", "timeout restore failed"} {
		t.Run(scenario, func(t *testing.T) {
			e := unitEngine(t)
			r, _, _ := e.validate(request())
			token := Reservation{request: r, id: "ours"}
			sets := 0
			tx := &adapterTx{query: func(sql string, _ []any) pgx.Row {
				if strings.Contains(sql, "current_setting") {
					return timeoutRow()
				}
				if scenario == "missing binding" {
					return rowScan(func(...any) error { return pgx.ErrNoRows })
				}
				return storedRow(record{hash: r.Hash, reservation: token.id, expires: e.now().Add(time.Minute)})
			}, exec: func(sql string, _ []any) (pgconn.CommandTag, error) {
				if strings.Contains(sql, "set_config") {
					sets++
					if (scenario == "timeout set failed" && sets == 1) || (scenario == "timeout restore failed" && sets == 2) {
						return pgconn.CommandTag{}, errors.New("secret setting")
					}
					return pgconn.NewCommandTag("SELECT 1"), nil
				}
				if scenario == "binding update failed" {
					return pgconn.CommandTag{}, errors.New("secret update")
				}
				return pgconn.NewCommandTag("UPDATE 0"), nil
			}}
			e.txs = adapterTransactions{tx}
			err := e.BindResource(context.Background(), token, Resource{Type: "operation", ID: "result"})
			switch scenario {
			case "missing binding", "binding update lost":
				assertReason(t, err, "REQUEST_IN_PROGRESS")
			default:
				if !errors.Is(err, ErrStorage) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestUnitConditionalReleaseNeverAuthorizesForeignOrMissingState(t *testing.T) {
	for _, scenario := range []string{"missing", "read failure", "foreign", "expired", "delete failure"} {
		t.Run(scenario, func(t *testing.T) {
			e := unitEngine(t)
			r, _, _ := e.validate(request())
			token := Reservation{request: r, id: "ours"}
			deleted := 0
			tx := &adapterTx{query: func(sql string, _ []any) pgx.Row {
				if strings.Contains(sql, "current_setting") {
					return timeoutRow()
				}
				switch scenario {
				case "missing":
					return rowScan(func(...any) error { return pgx.ErrNoRows })
				case "read failure":
					return rowScan(func(...any) error { return errors.New("private SQL") })
				}
				row := record{hash: r.Hash, reservation: token.id, expires: e.now().Add(time.Minute)}
				if scenario == "foreign" {
					row.reservation = "theirs"
				}
				if scenario == "expired" {
					row.expires = e.now()
				}
				return storedRow(row)
			}, exec: func(sql string, _ []any) (pgconn.CommandTag, error) {
				if strings.Contains(sql, "set_config") {
					return pgconn.NewCommandTag("SELECT 1"), nil
				}
				deleted++
				return pgconn.CommandTag{}, errors.New("private delete")
			}}
			e.txs = adapterTransactions{tx}
			released, err := e.ReleaseIfCurrent(context.Background(), token)
			if released {
				t.Fatal("unsafe compensation authorized")
			}
			if scenario == "read failure" || scenario == "delete failure" {
				if !errors.Is(err, ErrStorage) {
					t.Fatal(err)
				}
			} else if err != nil || deleted != 0 {
				t.Fatalf("unowned release mutated storage: %d %v", deleted, err)
			}
		})
	}
}
func TestUnitDuplicateDoAndReserveReturnStoredDomainWithoutEffect(t *testing.T) {
	e := unitEngine(t)
	r, _, _ := e.validate(request())
	payload, _, err := e.encode(r, Policy{}, Result{Err: finerr.WrongFormat()})
	if err != nil {
		t.Fatal(err)
	}
	tx := &adapterTx{query: func(sql string, _ []any) pgx.Row {
		if strings.Contains(sql, "current_setting") {
			return timeoutRow()
		}
		if strings.HasPrefix(sql, "INSERT") {
			return rowScan(func(...any) error { return pgx.ErrNoRows })
		}
		return storedRow(record{hash: r.Hash, done: true, payload: payload, expires: e.now().Add(time.Hour)})
	}, exec: func(sql string, _ []any) (pgconn.CommandTag, error) {
		if !strings.Contains(sql, "set_config") {
			t.Fatal("duplicate mutated saved result")
		}
		return pgconn.NewCommandTag("SELECT 1"), nil
	}}
	e.txs = adapterTransactions{tx}
	ctx, cancel := e.WithCompletion(context.Background())
	defer cancel()
	result, err := e.Do(ctx, r, func(context.Context) (Response, error) { t.Error("duplicate ran effect"); return Response{}, nil })
	if err != nil || !result.Replayed {
		t.Fatalf("Do replay %+v %v", result, err)
	}
	assertReason(t, result.Err, "WRONG_FORMAT")
	token, result, err := e.Reserve(ctx, r)
	if err != nil || token != (Reservation{}) || !result.Replayed {
		t.Fatalf("Reserve replay %+v %v", result, err)
	}
	assertReason(t, result.Err, "WRONG_FORMAT")
	if err := e.progress(r, 0); err == nil {
		t.Fatal("nonpositive retry lost refusal")
	}
}
