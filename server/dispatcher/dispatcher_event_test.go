package dispatcher

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage/storetest"
	"github.com/ZeeingKajama/FreeJobScheduler/server/agenthub"
	"github.com/ZeeingKajama/FreeJobScheduler/server/engine/runs"
)

func TestDispatcher_EventDrivenCascade(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storetest.New(t)
	svc := runs.NewService(store)

	odate := "20260916"

	// Def 1: Produces COND_STEP1_DONE
	def1 := &storage.JobDef{
		ID:            "def-step1",
		Name:          "STEP1_JOB",
		Command:       "echo step1",
		OutConditions: []string{"COND_STEP1_DONE"},
	}
	_ = store.JobDef().Create(ctx, def1)

	// Def 2: Requires COND_STEP1_DONE
	def2 := &storage.JobDef{
		ID:           "def-step2",
		Name:         "STEP2_JOB",
		Command:      "echo step2",
		InConditions: []string{"COND_STEP1_DONE"},
	}
	_ = store.JobDef().Create(ctx, def2)

	// Create Run 2 in WAIT state
	run2 := &storage.JobRun{
		RunID:       "run-step2",
		JobDefID:    def2.ID,
		JobName:     def2.Name,
		State:       storage.StateWait,
		Command:     def2.Command,
		CreatedDate: odate,
		ScheduledAt: time.Now(),
		ExitCode:    -1,
	}
	_ = store.JobRun().Create(ctx, run2)

	// Register Run 2 in inverted index
	svc.RegisterWaitingTask(ctx, run2.RunID, run2.JobDefID, odate, def2.InConditions)

	// Verify Run 2 is waiting in index
	if svc.WaitingCount() != 1 {
		t.Fatalf("expected 1 waiting task in index, got %d", svc.WaitingCount())
	}

	// Emit COND_STEP1_DONE (simulating Step 1 completion)
	_ = svc.EmitCondition(ctx, "COND_STEP1_DONE", odate)

	// Verify Run 2 transitioned to READY immediately in O(1)
	updatedRun2, err := store.JobRun().GetByID(ctx, run2.RunID)
	if err != nil || updatedRun2.State != storage.StateReady {
		t.Fatalf("expected run2 state to be READY, got %v, err=%v", updatedRun2.State, err)
	}
	if svc.WaitingCount() != 0 {
		t.Fatalf("expected 0 waiting tasks after transition, got %d", svc.WaitingCount())
	}
}

func TestDispatcher_SlotThrottling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storetest.New(t)
	hub := agenthub.NewHub("token", nil)
	svc := runs.NewService(store)
	disp := NewDispatcher(store, svc, hub, WithLogDir(t.TempDir()))

	odate := "20260916"

	// Create 10 READY tasks
	for i := 0; i < 10; i++ {
		def := &storage.JobDef{
			ID:      fmt.Sprintf("def-throttle-%d", i),
			Name:    fmt.Sprintf("JOB_THROTTLE_%d", i),
			Command: "sleep 1",
		}
		_ = store.JobDef().Create(ctx, def)

		run := &storage.JobRun{
			RunID:       fmt.Sprintf("run-throttle-%02d", i),
			JobDefID:    def.ID,
			JobName:     def.Name,
			State:       storage.StateReady,
			Command:     def.Command,
			CreatedDate: odate,
			ScheduledAt: time.Now().Add(time.Duration(i) * time.Millisecond),
			ExitCode:    -1,
		}
		_ = store.JobRun().Create(ctx, run)
	}

	// 0 agents connected -> PollAndDispatch should claim 0
	disp.PollAndDispatch(ctx)
	readyRuns, _ := store.JobRun().ListByState(ctx, []storage.RunState{storage.StateReady})
	if len(readyRuns) != 10 {
		t.Fatalf("no agents connected, all 10 tasks must remain READY, got %d", len(readyRuns))
	}

	// When agents with slots exist, TotalAvailableSlots throttles chunk claiming
	// Tested further in Unit 6 Scale benchmark
}

func TestDispatcher_BootstrapRecovery(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)

	odate := "20260916"

	// Pre-seed condition in DB
	_ = store.Condition().Add(ctx, "BOOTSTRAP_COND_OK", odate)

	def := &storage.JobDef{
		ID:           "def-boot",
		Name:         "BOOT_JOB",
		Command:      "echo boot",
		InConditions: []string{"BOOTSTRAP_COND_OK"},
	}
	_ = store.JobDef().Create(ctx, def)

	// Pre-seed WAIT task in DB
	run := &storage.JobRun{
		RunID:       "run-boot",
		JobDefID:    def.ID,
		JobName:     def.Name,
		State:       storage.StateWait,
		Command:     def.Command,
		CreatedDate: odate,
		ScheduledAt: time.Now(),
		ExitCode:    -1,
	}
	_ = store.JobRun().Create(ctx, run)

	// Create a fresh service (empty cache) and bootstrap
	svc := runs.NewService(store)
	if err := svc.Bootstrap(ctx); err != nil {
		t.Fatalf("Bootstrap failed: %v", err)
	}

	// Verify task was automatically transitioned to READY during bootstrap recovery
	recoveredRun, err := store.JobRun().GetByID(ctx, "run-boot")
	if err != nil || recoveredRun.State != storage.StateReady {
		t.Fatalf("expected run-boot to transition to READY upon bootstrap, got %s, err=%v", recoveredRun.State, err)
	}
}

func TestDispatcher_BoundedLogsAndSpooling(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := runs.NewService(store)
	disp := NewDispatcher(store, svc, nil, WithLogDir(t.TempDir()))

	// Spool into a temp dir: the default logs/runs is relative to cwd and appended to across runs.
	spooler, err := NewLogSpooler(t.TempDir())
	if err != nil {
		t.Fatalf("NewLogSpooler failed: %v", err)
	}
	defer spooler.CloseAll()
	disp.logSpooler = spooler

	taskID := "task-bounded-disp"

	// Send 1,200 log chunks
	for i := 1; i <= 1200; i++ {
		// Agents send lines without the trailing newline (bufio.Scanner.Text())
		disp.HandleLogChunk(ctx, "agent-1", protocol.TaskLogChunkPayload{
			TaskID:   taskID,
			Sequence: int64(i),
			Content:  fmt.Sprintf("log line %d", i),
		})
	}

	// Historical logs in memory should be capped at 1,000
	logs := disp.GetHistoricalLogs(taskID)
	if len(logs) != 1000 {
		t.Fatalf("expected 1,000 logs in memory, got %d", len(logs))
	}

	// Oldest log retained should be sequence 201
	if logs[0].Sequence != 201 {
		t.Fatalf("expected oldest in-memory log sequence 201, got %d", logs[0].Sequence)
	}

	// Full disk spooler log should contain all 1,200 lines
	diskLog, err := disp.GetFullDiskLog(taskID)
	if err != nil {
		t.Fatalf("GetFullDiskLog failed: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(diskLog, "\n"), "\n")
	if len(lines) != 1200 {
		t.Fatalf("expected exactly 1,200 newline-terminated lines on disk, got %d", len(lines))
	}
	if lines[0] != "log line 1" || lines[1199] != "log line 1200" {
		t.Fatalf("unexpected disk log content: first=%q last=%q", lines[0], lines[1199])
	}
	if !strings.HasSuffix(diskLog, "\n") {
		t.Fatalf("disk log must end with a newline")
	}
}

// Old READY tasks that no connected agent can run (label mismatch) must not starve newer runnable tasks.
func TestDispatcher_LabelMismatchDoesNotStarveQueue(t *testing.T) {
	ctx := context.Background()

	store := storetest.New(t)
	hub := agenthub.NewHub("token", nil)
	svc := runs.NewService(store)
	disp := NewDispatcher(store, svc, hub, WithLogDir(t.TempDir()))
	connectFakeAgent(t, hub, "agent-linux", []string{"linux"}, 1)

	base := time.Now().Add(-time.Hour)
	mkRun := func(id string, labels []string, age time.Duration) {
		def := &storage.JobDef{ID: "def-" + id, Name: "JOB_" + id, Command: "echo " + id, AgentLabels: labels}
		_ = store.JobDef().Create(ctx, def)
		_ = store.JobRun().Create(ctx, &storage.JobRun{
			RunID: id, JobDefID: def.ID, JobName: def.Name, State: storage.StateReady,
			Command: def.Command, CreatedDate: "20260916", ScheduledAt: base.Add(age), ExitCode: -1,
		})
	}
	// Three older windows-only jobs, then one unlabeled job.
	mkRun("win-1", []string{"windows"}, 0)
	mkRun("win-2", []string{"windows"}, time.Second)
	mkRun("win-3", []string{"windows"}, 2*time.Second)
	mkRun("any-1", nil, 3*time.Second)

	disp.PollAndDispatch(ctx)

	state := func(id string) storage.RunState {
		r, err := store.JobRun().GetByID(ctx, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		return r.State
	}
	if got := state("any-1"); got != storage.StateAssigned {
		t.Errorf("any-1: expected ASSIGNED, got %s", got)
	}
	for _, id := range []string{"win-1", "win-2", "win-3"} {
		if got := state(id); got != storage.StateReady {
			t.Errorf("%s: expected READY (no matching agent), got %s", id, got)
		}
	}
}

// If an operator forces a RUNNING run to SUCCESS, the agent's later SUCCESS report fails the
// SUCCESS -> SUCCESS transition, but the agent slot it occupied must still be released.
func TestDispatcher_SlotReleasedAfterOperatorSetOK(t *testing.T) {
	ctx := context.Background()

	store := storetest.New(t)
	hub := agenthub.NewHub("token", nil)
	svc := runs.NewService(store)
	disp := NewDispatcher(store, svc, hub, WithLogDir(t.TempDir()))
	connectFakeAgent(t, hub, "agent-1", nil, 1)

	def := &storage.JobDef{ID: "def-slot", Name: "SLOT_JOB", Command: "sleep 100"}
	_ = store.JobDef().Create(ctx, def)
	_ = store.JobRun().Create(ctx, &storage.JobRun{
		RunID: "run-slot", JobDefID: def.ID, JobName: def.Name, State: storage.StateReady,
		Command: def.Command, CreatedDate: "20260928", ScheduledAt: time.Now(), ExitCode: -1,
	})

	availableSlots := func() int32 {
		agents := hub.GetConnectedAgents()
		if len(agents) != 1 {
			t.Fatalf("expected 1 connected agent, got %d", len(agents))
		}
		return agents[0].AvailableSlots
	}

	disp.PollAndDispatch(ctx)
	if got := availableSlots(); got != 0 {
		t.Fatalf("slot should be taken after dispatch, available=%d", got)
	}
	disp.HandleStatusUpdate(ctx, "agent-1", protocol.TaskStatusUpdatePayload{TaskID: "run-slot", State: "RUNNING"})

	// Operator sets the still-running job OK.
	if err := store.JobRun().TransitionState(ctx, "run-slot", storage.StateRunning, storage.StateSuccess, 0, "operator"); err != nil {
		t.Fatalf("operator SET_OK transition failed: %v", err)
	}

	// The agent then reports the real outcome.
	disp.HandleStatusUpdate(ctx, "agent-1", protocol.TaskStatusUpdatePayload{TaskID: "run-slot", State: "SUCCESS"})

	if got := availableSlots(); got != 1 {
		t.Fatalf("slot leaked: expected 1 available slot after agent SUCCESS report, got %d", got)
	}
}
