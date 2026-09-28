package webapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage/storetest"
	"github.com/ZeeingKajama/FreeJobScheduler/server/dispatcher"
	"github.com/ZeeingKajama/FreeJobScheduler/server/engine/runs"
)

func TestDailyPlanOrder_And_CascadeExecution(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := runs.NewService(store)
	disp := dispatcher.NewDispatcher(store, svc, nil, dispatcher.WithLogDir(t.TempDir()))
	handler := NewAPIHandler(store, nil, disp, svc)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	odate := "20260923"

	// 1. Create a 3-tier DAG of Job Definitions
	// Tier 1: Parent (Root)
	parentDef := &storage.JobDef{
		ID:            "def-parent",
		Name:          "ParentJob",
		Group:         "TEST",
		Command:       "echo parent",
		OutConditions: []string{"COND_PARENT_OK"},
		Enabled:       true,
	}
	// Tier 2: Child (depends on Parent)
	childDef := &storage.JobDef{
		ID:            "def-child",
		Name:          "ChildJob",
		Group:         "TEST",
		Command:       "echo child",
		InConditions:  []string{"COND_PARENT_OK"},
		OutConditions: []string{"COND_CHILD_OK"},
		Enabled:       true,
	}
	// Tier 3: Grandchild (depends on Child)
	grandchildDef := &storage.JobDef{
		ID:           "def-grandchild",
		Name:         "GrandchildJob",
		Group:        "TEST",
		Command:      "echo grandchild",
		InConditions: []string{"COND_CHILD_OK"},
		Enabled:      true,
	}

	_ = store.JobDef().Create(ctx, parentDef)
	_ = store.JobDef().Create(ctx, childDef)
	_ = store.JobDef().Create(ctx, grandchildDef)

	// 2. Execute Daily Plan Order for 20260923
	orderReqBody := map[string]any{
		"odate": odate,
		"group": "TEST",
	}
	bodyBytes, _ := json.Marshal(orderReqBody)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/order", bytes.NewReader(bodyBytes))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from /api/v1/runs/order, got %d: %s", rec.Code, rec.Body.String())
	}

	var orderResp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &orderResp)
	if int(orderResp["ordered_count"].(float64)) != 3 {
		t.Errorf("expected 3 ordered jobs, got %v", orderResp["ordered_count"])
	}
	if int(orderResp["ready_count"].(float64)) != 1 {
		t.Errorf("expected 1 ready job, got %v", orderResp["ready_count"])
	}
	if int(orderResp["wait_count"].(float64)) != 2 {
		t.Errorf("expected 2 wait jobs, got %v", orderResp["wait_count"])
	}

	// Verify DB state
	runs, _ := store.JobRun().ListByDate(ctx, odate)
	if len(runs) != 3 {
		t.Fatalf("expected 3 runs in DB for %s, got %d", odate, len(runs))
	}

	runByDef := make(map[string]*storage.JobRun)
	for _, r := range runs {
		runByDef[r.JobDefID] = r
	}

	if runByDef["def-parent"].State != storage.StateReady {
		t.Errorf("expected Parent to be READY, got %s", runByDef["def-parent"].State)
	}
	if runByDef["def-child"].State != storage.StateWait {
		t.Errorf("expected Child to be WAIT, got %s", runByDef["def-child"].State)
	}
	if runByDef["def-grandchild"].State != storage.StateWait {
		t.Errorf("expected Grandchild to be WAIT, got %s", runByDef["def-grandchild"].State)
	}

	// Verify inverted condition index has 2 waiting tasks
	if svc.WaitingCount() != 2 {
		t.Fatalf("expected 2 waiting tasks in index, got %d", svc.WaitingCount())
	}

	// 3. Re-running Order should be idempotent
	rec2 := httptest.NewRecorder()
	mux.ServeHTTP(rec2, httptest.NewRequest(http.MethodPost, "/api/v1/runs/order", bytes.NewReader(bodyBytes)))
	var orderResp2 map[string]any
	_ = json.Unmarshal(rec2.Body.Bytes(), &orderResp2)
	if int(orderResp2["ordered_count"].(float64)) != 0 || int(orderResp2["skipped_count"].(float64)) != 3 {
		t.Errorf("expected 0 ordered and 3 skipped on re-order, got %v", orderResp2)
	}

	// 4. Simulate Parent Job Completion (Emitting COND_PARENT_OK)
	_ = svc.EmitCondition(ctx, "COND_PARENT_OK", odate)

	// Child must automatically transition to READY
	childRun, _ := store.JobRun().GetByID(ctx, runByDef["def-child"].RunID)
	if childRun.State != storage.StateReady {
		t.Fatalf("expected Child to transition to READY, got %s", childRun.State)
	}
	if svc.WaitingCount() != 1 {
		t.Errorf("expected 1 waiting task left, got %d", svc.WaitingCount())
	}

	// 5. Simulate Child Job Completion (Emitting COND_CHILD_OK)
	_ = svc.EmitCondition(ctx, "COND_CHILD_OK", odate)

	// Grandchild must automatically transition to READY
	gcRun, _ := store.JobRun().GetByID(ctx, runByDef["def-grandchild"].RunID)
	if gcRun.State != storage.StateReady {
		t.Fatalf("expected Grandchild to transition to READY, got %s", gcRun.State)
	}
	if svc.WaitingCount() != 0 {
		t.Errorf("expected 0 waiting tasks left, got %d", svc.WaitingCount())
	}
}

func TestSingleTrigger_CascadeDownstreamWait(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := runs.NewService(store)
	disp := dispatcher.NewDispatcher(store, svc, nil, dispatcher.WithLogDir(t.TempDir()))
	handler := NewAPIHandler(store, nil, disp, svc)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	odate := "20260924"

	// Job A emits COND_A_OK
	jobA := &storage.JobDef{
		ID:            "def-job-a",
		Name:          "Job_A",
		Group:         "CHAIN",
		Command:       "echo A",
		OutConditions: []string{"COND_A_OK"},
		Enabled:       true,
	}
	// Job B requires COND_A_OK
	jobB := &storage.JobDef{
		ID:           "def-job-b",
		Name:         "Job_B",
		Group:        "CHAIN",
		Command:      "echo B",
		InConditions: []string{"COND_A_OK"},
		Enabled:      true,
	}

	_ = store.JobDef().Create(ctx, jobA)
	_ = store.JobDef().Create(ctx, jobB)

	// Trigger ONLY Job A
	triggerBody, _ := json.Marshal(map[string]any{
		"job_id": "def-job-a",
		"odate":  odate,
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger", bytes.NewReader(triggerBody))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created from /api/v1/jobs/trigger, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify that Job B was automatically instantiated in WAIT state and registered in dispatcher!
	runs, _ := store.JobRun().ListByDate(ctx, odate)
	if len(runs) != 2 {
		t.Fatalf("expected 2 runs (A and cascaded B), got %d", len(runs))
	}

	runMap := make(map[string]*storage.JobRun)
	for _, r := range runs {
		runMap[r.JobDefID] = r
	}

	if runMap["def-job-a"].State != storage.StateReady {
		t.Errorf("Job A should be READY, got %s", runMap["def-job-a"].State)
	}
	if runMap["def-job-b"].State != storage.StateWait {
		t.Errorf("Cascaded Job B should be WAIT, got %s", runMap["def-job-b"].State)
	}

	if svc.WaitingCount() != 1 {
		t.Fatalf("expected 1 waiting task in index for Job B, got %d", svc.WaitingCount())
	}

	// Now Job A completes
	_ = svc.EmitCondition(ctx, "COND_A_OK", odate)

	bRun, _ := store.JobRun().GetByID(ctx, runMap["def-job-b"].RunID)
	if bRun.State != storage.StateReady {
		t.Errorf("Job B should have transitioned to READY upon A completion, got %s", bRun.State)
	}
}
