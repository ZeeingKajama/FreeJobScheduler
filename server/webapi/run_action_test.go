package webapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage/storetest"
	"github.com/ZeeingKajama/FreeJobScheduler/server/dispatcher"
	"github.com/ZeeingKajama/FreeJobScheduler/server/engine/runs"
)

// newActionTestEnv returns a store, the run service and a mux wired like production (no hub).
func newActionTestEnv(t *testing.T) (storage.Store, *runs.Service, *http.ServeMux) {
	store := storetest.New(t)
	svc := runs.NewService(store)
	disp := dispatcher.NewDispatcher(store, svc, nil, dispatcher.WithLogDir(t.TempDir()))
	mux := http.NewServeMux()
	NewAPIHandler(store, nil, disp, svc).RegisterRoutes(mux)
	return store, svc, mux
}

func doJSON(mux *http.ServeMux, method, url string, body any) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(method, url, &buf))
	return rec
}

func runState(t *testing.T, store storage.Store, runID string) storage.RunState {
	t.Helper()
	run, err := store.JobRun().GetByID(context.Background(), runID)
	if err != nil {
		t.Fatalf("get run %s: %v", runID, err)
	}
	return run.State
}

// Seeds A (out: A_OK, READY) and B (in: A_OK, WAIT and registered in the dispatcher index).
func seedUpstreamDownstream(t *testing.T, store storage.Store, svc *runs.Service, odate string) {
	t.Helper()
	ctx := context.Background()

	defA := &storage.JobDef{ID: "def-a", Name: "A", Command: "echo a", OutConditions: []string{"A_OK"}, Enabled: true}
	defB := &storage.JobDef{ID: "def-b", Name: "B", Command: "echo b", InConditions: []string{"A_OK"}, Enabled: true}
	_ = store.JobDef().Create(ctx, defA)
	_ = store.JobDef().Create(ctx, defB)

	_ = store.JobRun().Create(ctx, &storage.JobRun{
		RunID: "run-a", JobDefID: defA.ID, JobName: defA.Name, State: storage.StateReady,
		Command: defA.Command, CreatedDate: odate, ScheduledAt: time.Now(), ExitCode: -1,
	})
	_ = store.JobRun().Create(ctx, &storage.JobRun{
		RunID: "run-b", JobDefID: defB.ID, JobName: defB.Name, State: storage.StateWait,
		Command: defB.Command, CreatedDate: odate, ScheduledAt: time.Now(), ExitCode: -1,
	})
	svc.RegisterWaitingTask(ctx, "run-b", defB.ID, odate, defB.InConditions)
}

func TestRunAction_Bypass_ReleasesDownstream(t *testing.T) {
	store, svc, mux := newActionTestEnv(t)
	seedUpstreamDownstream(t, store, svc, "20260928")

	rec := doJSON(mux, http.MethodPost, "/api/v1/runs/action", map[string]any{
		"action": "BYPASS", "run_id": "run-a", "operator_id": "op", "reason": "skip upstream",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("bypass failed: %d %s", rec.Code, rec.Body.String())
	}

	if got := runState(t, store, "run-a"); got != storage.StateBypass {
		t.Fatalf("run-a expected BYPASS, got %s", got)
	}
	if got := runState(t, store, "run-b"); got != storage.StateReady {
		t.Fatalf("run-b should be released to READY after upstream BYPASS, got %s", got)
	}
}

func TestRunAction_SetOK_ReleasesDownstream(t *testing.T) {
	store, svc, mux := newActionTestEnv(t)
	seedUpstreamDownstream(t, store, svc, "20260928")

	rec := doJSON(mux, http.MethodPost, "/api/v1/runs/action", map[string]any{
		"action": "SET_OK", "run_id": "run-a", "operator_id": "op", "reason": "verified",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("set ok failed: %d %s", rec.Code, rec.Body.String())
	}
	if got := runState(t, store, "run-b"); got != storage.StateReady {
		t.Fatalf("run-b should be READY after upstream SET_OK, got %s", got)
	}
}

// Deleting a condition must also drop it from the dispatcher's in-memory index,
// otherwise later runs that require it are wrongly released.
func TestConditions_DeleteInvalidatesInMemoryCache(t *testing.T) {
	ctx := context.Background()
	store, svc, mux := newActionTestEnv(t)
	odate := "20260928"

	if rec := doJSON(mux, http.MethodPost, "/api/v1/conditions", map[string]any{"name": "X_OK", "odate": odate}); rec.Code != http.StatusOK {
		t.Fatalf("add condition failed: %d %s", rec.Code, rec.Body.String())
	}
	if rec := doJSON(mux, http.MethodDelete, "/api/v1/conditions?name=X_OK&date="+odate, nil); rec.Code != http.StatusOK {
		t.Fatalf("delete condition failed: %d %s", rec.Code, rec.Body.String())
	}

	def := &storage.JobDef{ID: "def-x", Name: "NEEDS_X", Command: "echo x", InConditions: []string{"X_OK"}, Enabled: true}
	_ = store.JobDef().Create(ctx, def)
	_ = store.JobRun().Create(ctx, &storage.JobRun{
		RunID: "run-x", JobDefID: def.ID, JobName: def.Name, State: storage.StateWait,
		Command: def.Command, CreatedDate: odate, ScheduledAt: time.Now(), ExitCode: -1,
	})
	svc.RegisterWaitingTask(ctx, "run-x", def.ID, odate, def.InConditions)

	if got := runState(t, store, "run-x"); got != storage.StateWait {
		t.Fatalf("run-x must stay WAIT after its condition was deleted, got %s", got)
	}
}

// Two triggers of the same job within one second must both succeed (run IDs may not collide).
func TestJobTrigger_TwiceInSameSecond(t *testing.T) {
	store, _, mux := newActionTestEnv(t)
	_ = store.JobDef().Create(context.Background(), &storage.JobDef{ID: "def-twice", Name: "TWICE", Command: "echo", Enabled: true})

	for i := 1; i <= 2; i++ {
		rec := doJSON(mux, http.MethodPost, "/api/v1/jobs/trigger", map[string]any{"job_id": "def-twice", "odate": "20260928"})
		if rec.Code != http.StatusCreated {
			t.Fatalf("trigger #%d: expected 201, got %d: %s", i, rec.Code, rec.Body.String())
		}
	}
	runs, _ := store.JobRun().ListByDate(context.Background(), "20260928")
	if len(runs) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(runs))
	}
}

// GET /api/v1/audits lists recent records of every target without target_id, and filters with it.
func TestListAudits_AllOrByTarget(t *testing.T) {
	store, _, mux := newActionTestEnv(t)
	ctx := context.Background()
	_ = store.Audit().Record(ctx, "kim", "RERUN", "run-1", "retry")
	_ = store.Audit().Record(ctx, "lee", "ORDER_PLAN", "20260928", "plan")

	list := func(url string) []storage.AuditRecord {
		t.Helper()
		rec := doJSON(mux, http.MethodGet, url, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s: expected 200, got %d: %s", url, rec.Code, rec.Body.String())
		}
		var records []storage.AuditRecord
		if err := json.Unmarshal(rec.Body.Bytes(), &records); err != nil {
			t.Fatalf("GET %s: decode: %v", url, err)
		}
		return records
	}

	if all := list("/api/v1/audits"); len(all) != 2 {
		t.Errorf("expected 2 records without target_id, got %d", len(all))
	}
	if one := list("/api/v1/audits?target_id=run-1"); len(one) != 1 || one[0].Action != "RERUN" {
		t.Errorf("expected only the RERUN record of run-1, got %+v", one)
	}
}
