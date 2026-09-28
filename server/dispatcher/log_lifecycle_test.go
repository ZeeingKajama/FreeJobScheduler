package dispatcher

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage/storetest"
	"github.com/ZeeingKajama/FreeJobScheduler/server/engine/runs"
)

// newLogTestDispatcher returns a dispatcher spooling into a temp dir and a RUNNING run to report on.
func newLogTestDispatcher(t *testing.T, runID string) *Dispatcher {
	t.Helper()
	ctx := context.Background()

	store := storetest.New(t)
	svc := runs.NewService(store)
	disp := NewDispatcher(store, svc, nil, WithLogDir(t.TempDir()))
	spooler, err := NewLogSpooler(t.TempDir())
	if err != nil {
		t.Fatalf("NewLogSpooler failed: %v", err)
	}
	t.Cleanup(spooler.CloseAll)
	disp.logSpooler = spooler

	def := &storage.JobDef{ID: "def-" + runID, Name: "JOB_" + runID, Command: "echo"}
	_ = store.JobDef().Create(ctx, def)
	_ = store.JobRun().Create(ctx, &storage.JobRun{
		RunID: runID, JobDefID: def.ID, JobName: def.Name, State: storage.StateRunning,
		Command: def.Command, CreatedDate: "20260928", ScheduledAt: time.Now(), ExitCode: -1,
	})
	return disp
}

func recvChunk(t *testing.T, ch <-chan protocol.TaskLogChunkPayload) (protocol.TaskLogChunkPayload, bool) {
	t.Helper()
	select {
	case c, ok := <-ch:
		return c, ok
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting on log channel")
		return protocol.TaskLogChunkPayload{}, false
	}
}

// A terminal report closes subscriber channels and frees the in-memory buffer;
// history is then served from the disk spool.
func TestDispatcher_TerminalStateReleasesLogResources(t *testing.T) {
	ctx := context.Background()
	disp := newLogTestDispatcher(t, "run-log")

	ch, unsubscribe := disp.Subscribe("run-log")
	defer unsubscribe()

	for i, line := range []string{"alpha", "beta", "gamma"} {
		disp.HandleLogChunk(ctx, "agent-1", protocol.TaskLogChunkPayload{
			TaskID: "run-log", Sequence: int64(i + 1), Stream: protocol.StreamStdout, Content: line,
		})
	}
	for _, want := range []string{"alpha", "beta", "gamma"} {
		if c, ok := recvChunk(t, ch); !ok || c.Content != want {
			t.Fatalf("expected live chunk %q, got %+v ok=%v", want, c, ok)
		}
	}

	disp.HandleStatusUpdate(ctx, "agent-1", protocol.TaskStatusUpdatePayload{TaskID: "run-log", State: "SUCCESS"})

	if _, ok := recvChunk(t, ch); ok {
		t.Fatalf("subscriber channel must be closed after terminal state")
	}
	if _, ok := disp.taskLogs.Load("run-log"); ok {
		t.Fatalf("in-memory ring buffer must be released after terminal state")
	}
	disp.logMu.Lock()
	remaining := len(disp.logSubs)
	disp.logMu.Unlock()
	if remaining != 0 {
		t.Fatalf("subscription registry must be empty after terminal state, has %d tasks", remaining)
	}

	hist := disp.GetHistoricalLogs("run-log")
	if len(hist) != 3 || hist[0].Content != "alpha" || hist[2].Content != "gamma" || hist[2].Sequence != 3 {
		t.Fatalf("expected 3 historical chunks reconstructed from disk, got %+v", hist)
	}

	unsubscribe() // must be safe after the channel was already closed
}

func TestDispatcher_HistoricalLogsFallbackKeepsLast1000Lines(t *testing.T) {
	ctx := context.Background()
	disp := newLogTestDispatcher(t, "run-tail")

	for i := 1; i <= 1500; i++ {
		disp.HandleLogChunk(ctx, "agent-1", protocol.TaskLogChunkPayload{
			TaskID: "run-tail", Sequence: int64(i), Content: fmt.Sprintf("line %d", i),
		})
	}
	disp.HandleStatusUpdate(ctx, "agent-1", protocol.TaskStatusUpdatePayload{TaskID: "run-tail", State: "FAILED", ExitCode: 1})

	hist := disp.GetHistoricalLogs("run-tail")
	if len(hist) != 1000 {
		t.Fatalf("expected last 1000 lines from disk, got %d", len(hist))
	}
	if hist[0].Sequence != 501 || hist[0].Content != "line 501" || hist[999].Sequence != 1500 {
		t.Fatalf("unexpected tail window: first=%+v last=%+v", hist[0], hist[999])
	}
}

func TestDispatcher_UnsubscribeStopsDeliveryAndFreesEntry(t *testing.T) {
	ctx := context.Background()
	disp := newLogTestDispatcher(t, "run-unsub")

	ch, unsubscribe := disp.Subscribe("run-unsub")
	unsubscribe()
	unsubscribe() // idempotent

	disp.HandleLogChunk(ctx, "agent-1", protocol.TaskLogChunkPayload{TaskID: "run-unsub", Sequence: 1, Content: "x"})

	select {
	case c := <-ch:
		t.Fatalf("unsubscribed channel received %+v", c)
	case <-time.After(100 * time.Millisecond):
	}
	disp.logMu.Lock()
	remaining := len(disp.logSubs)
	disp.logMu.Unlock()
	if remaining != 0 {
		t.Fatalf("subscription registry leaked %d tasks after unsubscribe", remaining)
	}
}

// Concurrent subscribers must all be registered (the old LoadOrStore+Store lost listeners).
func TestDispatcher_ConcurrentSubscribersAllReceive(t *testing.T) {
	ctx := context.Background()
	disp := newLogTestDispatcher(t, "run-conc")

	const n = 50
	chans := make([]<-chan protocol.TaskLogChunkPayload, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ch, unsub := disp.Subscribe("run-conc")
			t.Cleanup(unsub)
			chans[i] = ch
		}(i)
	}
	wg.Wait()

	disp.HandleLogChunk(ctx, "agent-1", protocol.TaskLogChunkPayload{TaskID: "run-conc", Sequence: 1, Content: "hello"})
	for i, ch := range chans {
		if c, ok := recvChunk(t, ch); !ok || c.Content != "hello" {
			t.Fatalf("subscriber %d missed the chunk: %+v ok=%v", i, c, ok)
		}
	}
}
