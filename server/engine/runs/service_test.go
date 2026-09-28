package runs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage/storetest"
)

func mustState(t *testing.T, store storage.Store, runID string) storage.RunState {
	t.Helper()
	run, err := store.JobRun().GetByID(context.Background(), runID)
	if err != nil {
		t.Fatalf("get run %s: %v", runID, err)
	}
	return run.State
}

// seedChain creates A (out: A_OK) and B (in: A_OK), orders both and returns the runs of A and B.
func seedChain(t *testing.T, svc *Service, store storage.Store, odate string) (runA, runB *storage.JobRun) {
	t.Helper()
	ctx := context.Background()
	defA := &storage.JobDef{ID: "def-a", Name: "A", Command: "echo a", OutConditions: []string{"A_OK"}, Enabled: true}
	defB := &storage.JobDef{ID: "def-b", Name: "B", Command: "echo b", InConditions: []string{"A_OK"}, Enabled: true}
	_ = store.JobDef().Create(ctx, defA)
	_ = store.JobDef().Create(ctx, defB)

	runA, err := svc.OrderRun(ctx, defA, odate)
	if err != nil {
		t.Fatalf("order A: %v", err)
	}
	runB, err = svc.OrderRun(ctx, defB, odate)
	if err != nil {
		t.Fatalf("order B: %v", err)
	}
	return runA, runB
}

func TestService_SetOK_EmitsConditionsAndAudits(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := NewService(store)

	odate := "20260916"

	jobDef := &storage.JobDef{
		ID:            "def-finance-01",
		Name:          "JOB_FINANCE_01",
		OutConditions: []string{"FINANCE_01_OK", "DAILY_CLOSING_OK"},
	}
	_ = store.JobDef().Create(ctx, jobDef)

	run := &storage.JobRun{
		RunID:       "run-finance-01",
		JobDefID:    jobDef.ID,
		JobName:     jobDef.Name,
		State:       storage.StateRunning,
		CreatedDate: odate,
	}
	_ = store.JobRun().Create(ctx, run)

	reason := "DB recovery executed manually by operator"
	if err := svc.SetOK(ctx, run.RunID, "operator_kim", reason); err != nil {
		t.Fatalf("SetOK failed: %v", err)
	}

	if got := mustState(t, store, run.RunID); got != storage.StateSuccess {
		t.Errorf("expected SUCCESS, got %s", got)
	}

	for _, cond := range jobDef.OutConditions {
		exists, err := store.Condition().Exists(ctx, cond, odate)
		if err != nil || !exists {
			t.Errorf("expected condition %s to exist after SetOK", cond)
		}
	}

	records, err := store.Audit().ListByTarget(ctx, run.RunID)
	if err != nil || len(records) != 1 {
		t.Fatalf("expected 1 audit record, got: %v", records)
	}
	if records[0].Action != "SET_OK" || records[0].OperatorID != "operator_kim" || records[0].Reason != reason {
		t.Errorf("audit record mismatch: %+v", records[0])
	}
}

func TestService_Rerun_CreatesReadyRun(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := NewService(store)

	run := &storage.JobRun{
		RunID:       "run-orig-01",
		JobDefID:    "def-01",
		JobName:     "SETTLE_CORE",
		State:       storage.StateFailed,
		Command:     "/opt/bin/settle",
		CreatedDate: "20260916",
	}
	_ = store.JobRun().Create(ctx, run)

	newRun, err := svc.Rerun(ctx, run.RunID, "operator_lee", "Rerun after network recovery")
	if err != nil {
		t.Fatalf("rerun failed: %v", err)
	}

	if newRun.State != storage.StateReady {
		t.Errorf("expected rerun state READY, got %s", newRun.State)
	}
	if newRun.RunID == run.RunID {
		t.Errorf("rerun should have a distinct RunID")
	}

	records, _ := store.Audit().ListByTarget(ctx, run.RunID)
	if len(records) != 1 || records[0].Action != "RERUN" {
		t.Errorf("audit record missing for rerun")
	}

	select {
	case <-svc.Dispatchable():
	default:
		t.Errorf("rerun must wake the dispatcher")
	}
}

func TestService_Rerun_TwiceInSameSecond(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := NewService(store)

	_ = store.JobRun().Create(ctx, &storage.JobRun{
		RunID: "run-orig", JobDefID: "def-1", JobName: "J", State: storage.StateFailed,
		Command: "echo", CreatedDate: "20260928", ScheduledAt: time.Now(), ExitCode: 1,
	})

	first, err := svc.Rerun(ctx, "run-orig", "op", "retry")
	if err != nil {
		t.Fatalf("first rerun failed: %v", err)
	}
	second, err := svc.Rerun(ctx, "run-orig", "op", "retry again")
	if err != nil {
		t.Fatalf("second rerun failed: %v", err)
	}
	if first.RunID == second.RunID {
		t.Fatalf("rerun IDs must differ, both %s", first.RunID)
	}
}

// SetOK and Bypass must publish the run's Out-Conditions to the index so the WAIT runs behind it are released.
func TestService_SetOKAndBypass_ReleaseDownstream(t *testing.T) {
	for _, action := range []string{"SET_OK", "BYPASS"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			store := storetest.New(t)
			svc := NewService(store)
			runA, runB := seedChain(t, svc, store, "20260928")

			if runB.State != storage.StateWait {
				t.Fatalf("B must wait for A_OK, got %s", runB.State)
			}
			var err error
			if action == "SET_OK" {
				err = svc.SetOK(ctx, runA.RunID, "op", "why")
			} else {
				err = svc.Bypass(ctx, runA.RunID, "op", "why", true)
			}
			if err != nil {
				t.Fatalf("%s failed: %v", action, err)
			}
			if got := mustState(t, store, runB.RunID); got != storage.StateReady {
				t.Fatalf("B should be READY after upstream %s, got %s", action, got)
			}
			if svc.WaitingCount() != 0 {
				t.Fatalf("index must be empty, has %d waiting", svc.WaitingCount())
			}
		})
	}
}

func TestService_Bypass_WithoutRelease_KeepsDownstreamWaiting(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := NewService(store)
	runA, runB := seedChain(t, svc, store, "20260928")

	if err := svc.Bypass(ctx, runA.RunID, "op", "why", false); err != nil {
		t.Fatalf("bypass failed: %v", err)
	}
	if got := mustState(t, store, runB.RunID); got != storage.StateWait {
		t.Fatalf("B must keep waiting when downstream release is off, got %s", got)
	}
}

// Deleting a condition removes it from DB and cache; a run ordered afterwards must wait.
func TestService_DeleteCondition_IsWriteThrough(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := NewService(store)
	odate := "20260928"

	if err := svc.EmitCondition(ctx, "X_OK", odate); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if exists, _ := store.Condition().Exists(ctx, "X_OK", odate); !exists {
		t.Fatalf("EmitCondition must write the condition to the DB")
	}
	if err := svc.DeleteCondition(ctx, "X_OK", odate); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if exists, _ := store.Condition().Exists(ctx, "X_OK", odate); exists {
		t.Fatalf("DeleteCondition must remove the condition from the DB")
	}

	def := &storage.JobDef{ID: "def-x", Name: "NEEDS_X", Command: "echo", InConditions: []string{"X_OK"}}
	run, err := svc.OrderRun(ctx, def, odate)
	if err != nil || run.State != storage.StateWait {
		t.Fatalf("run must wait after its condition was deleted, got %+v err=%v", run, err)
	}
}

// Deleting a condition the DB does not know must still clear a stale cache entry.
func TestService_DeleteCondition_ClearsCacheEvenWhenAbsentInDB(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := NewService(store)
	odate := "20260928"

	_ = svc.EmitCondition(ctx, "Y_OK", odate)
	_ = store.Condition().Delete(ctx, "Y_OK", odate) // DB and cache now disagree
	if err := svc.DeleteCondition(ctx, "Y_OK", odate); err == nil {
		t.Fatalf("expected ErrNotFound for a condition absent from the DB")
	}

	def := &storage.JobDef{ID: "def-y", Name: "NEEDS_Y", Command: "echo", InConditions: []string{"Y_OK"}}
	run, _ := svc.OrderRun(ctx, def, odate)
	if run.State != storage.StateWait {
		t.Fatalf("stale cache entry must be dropped, run is %s", run.State)
	}
}

func TestService_PruneConditionCache_FallsBackToDB(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := NewService(store)

	_ = svc.EmitCondition(ctx, "OLD_OK", "20260101")
	if dropped := svc.PruneConditionCache("20260901"); dropped != 1 {
		t.Fatalf("expected 1 ODate pruned, got %d", dropped)
	}

	// The DB still has the condition, so a run for that date is released.
	def := &storage.JobDef{ID: "def-old", Name: "OLD", Command: "echo", InConditions: []string{"OLD_OK"}}
	run, _ := svc.OrderRun(ctx, def, "20260101")
	if run.State != storage.StateReady {
		t.Fatalf("pruned condition must still be honoured via the DB, run is %s", run.State)
	}
}

func TestService_Trigger_CascadesDownstreamAndReportsMissingJob(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := NewService(store)
	odate := "20260924"

	_ = store.JobDef().Create(ctx, &storage.JobDef{ID: "def-a", Name: "Job_A", Command: "echo", OutConditions: []string{"A_OK"}, Enabled: true})
	_ = store.JobDef().Create(ctx, &storage.JobDef{ID: "def-b", Name: "Job_B", Command: "echo", InConditions: []string{"A_OK"}, Enabled: true})
	_ = store.JobDef().Create(ctx, &storage.JobDef{ID: "def-c", Name: "Job_C", Command: "echo", InConditions: []string{"Job_A"}, Enabled: true}) // by job name
	_ = store.JobDef().Create(ctx, &storage.JobDef{ID: "def-d", Name: "Job_D", Command: "echo", InConditions: []string{"OTHER"}, Enabled: true})

	run, err := svc.Trigger(ctx, "def-a", odate)
	if err != nil || run.State != storage.StateReady {
		t.Fatalf("trigger A: %+v err=%v", run, err)
	}
	runs, _ := store.JobRun().ListByDate(ctx, odate)
	got := map[string]bool{}
	for _, r := range runs {
		got[r.JobDefID] = true
	}
	if len(runs) != 3 || !got["def-a"] || !got["def-b"] || !got["def-c"] {
		t.Fatalf("expected A plus direct downstream B and C, got %v", got)
	}

	if _, err := svc.Trigger(ctx, "no-such-def", odate); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for unknown job, got %v", err)
	}
}

func TestService_OrderPlan_CountsAndReleasesExistingWaiters(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := NewService(store)
	odate := "20260923"

	_ = store.JobDef().Create(ctx, &storage.JobDef{ID: "def-p", Name: "P", Group: "G", Command: "echo", OutConditions: []string{"P_OK"}, Enabled: true})
	_ = store.JobDef().Create(ctx, &storage.JobDef{ID: "def-c", Name: "C", Group: "G", Command: "echo", InConditions: []string{"P_OK"}, Enabled: true})
	_ = store.JobDef().Create(ctx, &storage.JobDef{ID: "def-off", Name: "OFF", Group: "G", Command: "echo", Enabled: false})

	res, err := svc.OrderPlan(ctx, odate, "G", "op")
	if err != nil {
		t.Fatalf("order plan: %v", err)
	}
	if res.Ordered != 2 || res.Ready != 1 || res.Wait != 1 || res.Skipped != 0 {
		t.Fatalf("unexpected first plan result: %+v", res)
	}

	res, _ = svc.OrderPlan(ctx, odate, "G", "op")
	if res.Ordered != 0 || res.Skipped != 2 {
		t.Fatalf("second plan must be idempotent, got %+v", res)
	}

	// A condition that reached the DB behind the service's back is picked up by the next plan.
	_ = store.Condition().Add(ctx, "P_OK", odate)
	res, _ = svc.OrderPlan(ctx, odate, "G", "op")
	if res.Ready != 1 {
		t.Fatalf("existing WAIT run with satisfied conditions must be released, got %+v", res)
	}
	if svc.WaitingCount() != 0 {
		t.Fatalf("released run must leave the index, %d still waiting", svc.WaitingCount())
	}
}

func TestService_OnAgentStatus(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := NewService(store)
	runA, runB := seedChain(t, svc, store, "20260928")

	report := func(id string, state protocol.TaskState) error {
		return svc.OnAgentStatus(ctx, "agent-1", protocol.TaskStatusUpdatePayload{TaskID: id, State: state})
	}

	if err := report("run-unknown", protocol.TaskRunning); err == nil {
		t.Fatalf("unknown run must be reported as an error")
	}

	// READY -> ASSIGNED is the dispatcher's job; emulate it, then walk RUNNING -> SUCCESS.
	_ = store.JobRun().TransitionState(ctx, runA.RunID, storage.StateReady, storage.StateAssigned, 0, "")
	_ = report(runA.RunID, protocol.TaskRunning)
	if got := mustState(t, store, runA.RunID); got != storage.StateRunning {
		t.Fatalf("expected RUNNING, got %s", got)
	}
	_ = report(runA.RunID, protocol.TaskSuccess)
	if got := mustState(t, store, runA.RunID); got != storage.StateSuccess {
		t.Fatalf("expected SUCCESS, got %s", got)
	}
	if got := mustState(t, store, runB.RunID); got != storage.StateReady {
		t.Fatalf("A's success must release B, got %s", got)
	}

	// An operator-forced run reporting SUCCESS again fails the transition and must not error or re-emit.
	if err := report(runA.RunID, protocol.TaskSuccess); err != nil {
		t.Fatalf("duplicate terminal report must not be an error: %v", err)
	}
}
