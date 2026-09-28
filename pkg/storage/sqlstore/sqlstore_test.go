package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
	_ "modernc.org/sqlite"
)

func setupTestDB(t *testing.T) (*SQLStore, func()) {
	// In-memory SQLite with shared cache across pooled connections
	dbName := fmt.Sprintf("file:memdb_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := sql.Open("sqlite", dbName)
	if err != nil {
		t.Fatalf("failed to open test sqlite db: %v", err)
	}

	store := NewSQLStore(db)

	if err := store.InitializeSchema(context.Background(), SchemaDDL); err != nil {
		t.Fatalf("failed to initialize schema: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
	}
	return store, cleanup
}

func TestSQLStore_JobDefCRUD(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	def := &storage.JobDef{
		ID:            "def-sql-001",
		Name:          "SETTLE_SQL_BATCH",
		Group:         "FINANCE",
		CronExpr:      "0 1 * * *",
		Command:       "/bin/settle",
		Args:          []string{"--dry-run=false"},
		Env:           map[string]string{"ODATE": "20260916"},
		AgentLabels:   []string{"linux", "db-zone"},
		InConditions:  []string{"RAW_DATA_READY"},
		OutConditions: []string{"SETTLE_SQL_OK"},
		TimeoutSec:    300,
		Enabled:       true,
	}

	// 1. Create
	if err := store.JobDef().Create(ctx, def); err != nil {
		t.Fatalf("failed to create job def: %v", err)
	}

	// 2. GetByID
	fetched, err := store.JobDef().GetByID(ctx, def.ID)
	if err != nil {
		t.Fatalf("failed to get by id: %v", err)
	}
	if fetched.Name != def.Name || fetched.Command != def.Command {
		t.Errorf("mismatch in fetched job def: %+v", fetched)
	}

	// 3. List
	defs, err := store.JobDef().List(ctx, "FINANCE")
	if err != nil || len(defs) != 1 {
		t.Fatalf("expected 1 def, found %d: %v", len(defs), err)
	}

	// 4. Update
	def.CronExpr = "30 1 * * *"
	if err := store.JobDef().Update(ctx, def); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	updated, _ := store.JobDef().GetByID(ctx, def.ID)
	if updated.CronExpr != "30 1 * * *" {
		t.Errorf("cron expr was not updated in DB: %s", updated.CronExpr)
	}

	// 5. Delete
	if err := store.JobDef().Delete(ctx, def.ID); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if _, err := store.JobDef().GetByID(ctx, def.ID); err != storage.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// claimNext emulates the dispatcher: list READY candidates, then CAS READY -> ASSIGNED until one is won.
func claimNext(ctx context.Context, store storage.Store, agentID string) (*storage.JobRun, error) {
	candidates, err := store.JobRun().ListReady(ctx, 100)
	if err != nil {
		return nil, err
	}
	for _, c := range candidates {
		if err := store.JobRun().TransitionState(ctx, c.RunID, storage.StateReady, storage.StateAssigned, 0, ""); err != nil {
			continue // lost the race, try the next candidate
		}
		_ = store.JobRun().SetAgentID(ctx, c.RunID, agentID)
		return store.JobRun().GetByID(ctx, c.RunID)
	}
	return nil, nil
}

func TestSQLStore_ConcurrentAtomicClaim(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	const totalJobs = 3
	const concurrentWorkers = 20

	now := time.Now()
	for i := 0; i < totalJobs; i++ {
		run := &storage.JobRun{
			RunID:       fmt.Sprintf("sql-run-%03d", i+1),
			JobDefID:    fmt.Sprintf("sql-def-%03d", i+1),
			JobName:     fmt.Sprintf("BATCH_%03d", i+1),
			State:       storage.StateReady,
			Command:     "echo",
			ScheduledAt: now.Add(time.Duration(i) * time.Minute),
			CreatedDate: "20260916",
		}
		if err := store.JobRun().Create(ctx, run); err != nil {
			t.Fatalf("failed to insert job run: %v", err)
		}
	}

	var claimedCount int64
	var nilCount int64
	claimedIDs := sync.Map{}

	var wg sync.WaitGroup
	wg.Add(concurrentWorkers)

	startBarrier := make(chan struct{})

	for w := 0; w < concurrentWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			<-startBarrier

			agentName := fmt.Sprintf("sql-agent-%d", workerID)
			claimed, err := claimNext(ctx, store, agentName)
			if err != nil {
				t.Errorf("error during claim: %v", err)
				return
			}

			if claimed != nil {
				atomic.AddInt64(&claimedCount, 1)
				if _, exists := claimedIDs.LoadOrStore(claimed.RunID, agentName); exists {
					t.Errorf("DUPLICATE CLAIM: task %s claimed by multiple workers!", claimed.RunID)
				}
				if claimed.State != storage.StateAssigned {
					t.Errorf("expected state ASSIGNED, got %s", claimed.State)
				}
			} else {
				atomic.AddInt64(&nilCount, 1)
			}
		}(w)
	}

	close(startBarrier)
	wg.Wait()

	if claimedCount != totalJobs {
		t.Errorf("expected %d jobs claimed, got %d", totalJobs, claimedCount)
	}
	if nilCount != concurrentWorkers-totalJobs {
		t.Errorf("expected %d nil results, got %d", concurrentWorkers-totalJobs, nilCount)
	}
}

func TestSQLStore_ConditionAndAudit(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()

	// Condition
	condName := "SETTLE_DB_OK"
	odate := "20260916"

	if err := store.Condition().Add(ctx, condName, odate); err != nil {
		t.Fatalf("failed to add condition: %v", err)
	}

	exists, err := store.Condition().Exists(ctx, condName, odate)
	if err != nil || !exists {
		t.Errorf("expected condition to exist, got %v, err=%v", exists, err)
	}

	// Audit
	if err := store.Audit().Record(ctx, "operator_park", "SET_OK", "sql-run-001", "Manual DB patch verified"); err != nil {
		t.Fatalf("failed to record audit: %v", err)
	}

	records, err := store.Audit().ListByTarget(ctx, "sql-run-001")
	if err != nil || len(records) != 1 {
		t.Fatalf("expected 1 audit record, got: %v", records)
	}
	if records[0].OperatorID != "operator_park" || records[0].Action != "SET_OK" {
		t.Errorf("unexpected record: %+v", records[0])
	}

	// ListRecent spans targets, newest first, and honours the limit
	_ = store.Audit().Record(ctx, "operator_kim", "RERUN", "sql-run-002", "retry")
	recent, err := store.Audit().ListRecent(ctx, 10)
	if err != nil || len(recent) != 2 {
		t.Fatalf("expected 2 recent audit records, got %v (err=%v)", recent, err)
	}
	if recent[0].TargetID != "sql-run-002" {
		t.Errorf("expected newest record first, got %+v", recent[0])
	}
	if limited, _ := store.Audit().ListRecent(ctx, 1); len(limited) != 1 {
		t.Errorf("expected limit 1 to return 1 record, got %d", len(limited))
	}
}

func TestSQLStore_ListReady(t *testing.T) {
	store, cleanup := setupTestDB(t)
	defer cleanup()

	ctx := context.Background()
	base := time.Now()

	mk := func(id string, state storage.RunState, age time.Duration) {
		run := &storage.JobRun{
			RunID: id, JobDefID: "def-list", JobName: id, State: state, Command: "echo test",
			ScheduledAt: base.Add(age), CreatedDate: "20260916", ExitCode: -1,
		}
		if err := store.JobRun().Create(ctx, run); err != nil {
			t.Fatalf("failed to create run %s: %v", id, err)
		}
	}
	mk("r3", storage.StateReady, 3*time.Second)
	mk("r1", storage.StateReady, 1*time.Second)
	mk("w0", storage.StateWait, 0)
	mk("r2", storage.StateReady, 2*time.Second)
	mk("a0", storage.StateAssigned, 0)

	got, err := store.JobRun().ListReady(ctx, 2)
	if err != nil {
		t.Fatalf("ListReady failed: %v", err)
	}
	if len(got) != 2 || got[0].RunID != "r1" || got[1].RunID != "r2" {
		t.Fatalf("expected [r1 r2] (READY only, oldest first, limited), got %v", got)
	}

	// ListReady must not change state.
	all, _ := store.JobRun().ListReady(ctx, 10)
	if len(all) != 3 {
		t.Fatalf("expected 3 READY runs still, got %d", len(all))
	}
}

func TestSQLStore_StateTransitionRules(t *testing.T) {
	ctx := context.Background()
	store, cleanup := setupTestDB(t)
	defer cleanup()

	run := &storage.JobRun{
		RunID:       "run-test-01",
		JobDefID:    "def-01",
		JobName:     "SETTLE_CORE",
		State:       storage.StateWait,
		ScheduledAt: time.Now(),
		CreatedDate: "20260916",
	}

	if err := store.JobRun().Create(ctx, run); err != nil {
		t.Fatalf("failed to create run: %v", err)
	}

	// 1. Legal: WAIT -> READY
	if err := store.JobRun().TransitionState(ctx, run.RunID, storage.StateWait, storage.StateReady, 0, ""); err != nil {
		t.Fatalf("expected legal transition WAIT -> READY, got error: %v", err)
	}

	// 2. Illegal: READY -> RUNNING (skipping ASSIGNED)
	err := store.JobRun().TransitionState(ctx, run.RunID, storage.StateReady, storage.StateRunning, 0, "")
	if !errors.Is(err, storage.ErrInvalidStateTransition) {
		t.Fatalf("expected ErrInvalidStateTransition, got: %v", err)
	}

	// 3. Legal: READY -> ASSIGNED
	if err := store.JobRun().TransitionState(ctx, run.RunID, storage.StateReady, storage.StateAssigned, 0, ""); err != nil {
		t.Fatalf("expected legal transition READY -> ASSIGNED, got error: %v", err)
	}

	// 4. Legal: ASSIGNED -> RUNNING
	if err := store.JobRun().TransitionState(ctx, run.RunID, storage.StateAssigned, storage.StateRunning, 0, ""); err != nil {
		t.Fatalf("expected legal transition ASSIGNED -> RUNNING, got error: %v", err)
	}

	// Verify StartedAt is populated
	updated, err := store.JobRun().GetByID(ctx, run.RunID)
	if err != nil || updated.StartedAt == nil {
		t.Fatalf("expected StartedAt to be set upon RUNNING")
	}

	// 5. Legal: RUNNING -> FAILED
	if err := store.JobRun().TransitionState(ctx, run.RunID, storage.StateRunning, storage.StateFailed, 137, "OOM killed"); err != nil {
		t.Fatalf("expected legal transition RUNNING -> FAILED, got error: %v", err)
	}

	// Verify FinishedAt and ExitCode
	finished, err := store.JobRun().GetByID(ctx, run.RunID)
	if err != nil || finished.FinishedAt == nil || finished.ExitCode != 137 || finished.ErrorMessage != "OOM killed" {
		t.Fatalf("expected FinishedAt, exit code 137, and error message to be set, got %+v", finished)
	}

	// 6. Illegal: FAILED -> RUNNING (terminal state cannot transition)
	err = store.JobRun().TransitionState(ctx, run.RunID, storage.StateFailed, storage.StateRunning, 0, "")
	if !errors.Is(err, storage.ErrInvalidStateTransition) {
		t.Fatalf("expected ErrInvalidStateTransition from terminal state, got: %v", err)
	}
}
