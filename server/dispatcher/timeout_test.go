package dispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage/storetest"
	"github.com/ZeeingKajama/FreeJobScheduler/server/agenthub"
	"github.com/ZeeingKajama/FreeJobScheduler/server/engine/runs"
)

// dispatchedTimeout runs one READY job with the given JobDef.TimeoutSec through the dispatcher
// and returns the TimeoutSeconds carried by the TASK_DISPATCH the agent receives.
func dispatchedTimeout(t *testing.T, timeoutSec int) int {
	t.Helper()
	ctx := context.Background()
	store := storetest.New(t)
	hub := agenthub.NewHub("token", nil)
	svc := runs.NewService(store)
	disp := NewDispatcher(store, svc, hub, WithLogDir(t.TempDir()))

	agentConn := connectFakeAgent(t, hub, "agent-timeout", nil, 1)

	def := &storage.JobDef{ID: "def-to", Name: "TO_JOB", Command: "echo hi", TimeoutSec: timeoutSec}
	_ = store.JobDef().Create(ctx, def)
	_ = store.JobRun().Create(ctx, &storage.JobRun{
		RunID: "run-to", JobDefID: def.ID, JobName: def.Name, State: storage.StateReady,
		Command: def.Command, CreatedDate: "20260916", ScheduledAt: time.Now(), ExitCode: -1,
	})

	disp.PollAndDispatch(ctx)

	_ = agentConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var env protocol.Envelope
	if err := agentConn.ReadJSON(&env); err != nil || env.Type != protocol.TypeTaskDispatch {
		t.Fatalf("expected TASK_DISPATCH, got %+v, err=%v", env, err)
	}
	var payload protocol.TaskDispatchPayload
	if err := env.DecodePayload(&payload); err != nil {
		t.Fatalf("decode dispatch: %v", err)
	}
	return payload.TimeoutSeconds
}

func TestDispatcher_UsesJobDefTimeout(t *testing.T) {
	if got := dispatchedTimeout(t, 42); got != 42 {
		t.Fatalf("expected JobDef.TimeoutSec 42 to be dispatched, got %d", got)
	}
}

func TestDispatcher_DefaultTimeoutWhenUnset(t *testing.T) {
	if got := dispatchedTimeout(t, 0); got != defaultTimeoutSec {
		t.Fatalf("expected default timeout %d when TimeoutSec is 0, got %d", defaultTimeoutSec, got)
	}
}
