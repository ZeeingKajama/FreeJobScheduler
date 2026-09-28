package dispatcher

import (
	"context"
	"testing"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage/storetest"
	"github.com/ZeeingKajama/FreeJobScheduler/server/engine/runs"
)

func TestOrderRun_StateFollowsConditions(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := runs.NewService(store)
	odate := "20260916"

	free := &storage.JobDef{ID: "def-free", Name: "FREE", Command: "echo free"}
	gated := &storage.JobDef{ID: "def-gated", Name: "GATED", Command: "echo gated", Args: []string{"a"},
		Env: map[string]string{"K": "V"}, InConditions: []string{" A_OK ", ""}}

	run, err := svc.OrderRun(ctx, free, odate)
	if err != nil || run.State != storage.StateReady {
		t.Fatalf("job without conditions must be READY, got %+v, err=%v", run, err)
	}

	run, err = svc.OrderRun(ctx, gated, odate)
	if err != nil || run.State != storage.StateWait {
		t.Fatalf("job with unmet condition must be WAIT, got %+v, err=%v", run, err)
	}
	if run.Command != "echo gated" || run.Args[0] != "a" || run.Env["K"] != "V" || run.CreatedDate != odate || run.JobName != "GATED" {
		t.Fatalf("run fields were not copied from the definition: %+v", run)
	}
	if svc.WaitingCount() != 1 {
		t.Fatalf("WAIT run must be registered in the index")
	}

	// Emitting the condition releases it (blank condition names are ignored)
	_ = svc.EmitCondition(ctx, "A_OK", odate)
	got, _ := store.JobRun().GetByID(ctx, run.RunID)
	if got.State != storage.StateReady {
		t.Fatalf("expected READY after condition emitted, got %s", got.State)
	}
}

// After a server restart the in-memory cache is empty while the condition still exists in the DB.
// Ordering a run must judge conditions by the DB as well, otherwise it would wait forever.
func TestOrderRun_UsesConditionsPersistedInDB(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := runs.NewService(store)
	odate := "20260916"

	_ = store.Condition().Add(ctx, "OLD_OK", odate)
	def := &storage.JobDef{ID: "def-after-restart", Name: "AFTER_RESTART", Command: "echo", InConditions: []string{"OLD_OK"}}

	run, err := svc.OrderRun(ctx, def, odate)
	if err != nil || run.State != storage.StateReady {
		t.Fatalf("condition present in DB must make the run READY, got %+v, err=%v", run, err)
	}
}

func TestOrderRun_DeletedConditionIsNotResurrectedFromDB(t *testing.T) {
	ctx := context.Background()
	store := storetest.New(t)
	svc := runs.NewService(store)
	odate := "20260916"

	_ = svc.EmitCondition(ctx, "GONE_OK", odate)
	_ = svc.DeleteCondition(ctx, "GONE_OK", odate)

	def := &storage.JobDef{ID: "def-gone", Name: "GONE", Command: "echo", InConditions: []string{"GONE_OK"}}
	run, err := svc.OrderRun(ctx, def, odate)
	if err != nil || run.State != storage.StateWait {
		t.Fatalf("deleted condition must keep the run WAIT, got %+v, err=%v", run, err)
	}
}
