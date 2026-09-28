package webapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage/storetest"
	"github.com/ZeeingKajama/FreeJobScheduler/server/dispatcher"
	"github.com/ZeeingKajama/FreeJobScheduler/server/engine/runs"
	"github.com/gorilla/websocket"
)

func newLogStreamEnv(t *testing.T, state storage.RunState) (*dispatcher.Dispatcher, string) {
	t.Helper()
	ctx := context.Background()

	store := storetest.New(t)
	svc := runs.NewService(store)
	disp := dispatcher.NewDispatcher(store, svc, nil, dispatcher.WithLogDir(t.TempDir()))
	_ = store.JobDef().Create(ctx, &storage.JobDef{ID: "def-ws", Name: "WS_JOB", Command: "echo"})
	_ = store.JobRun().Create(ctx, &storage.JobRun{
		RunID: "run-ws", JobDefID: "def-ws", JobName: "WS_JOB", State: state,
		Command: "echo", CreatedDate: "20260928", ScheduledAt: time.Now(), ExitCode: -1,
	})

	mux := http.NewServeMux()
	NewAPIHandler(store, nil, disp, svc).RegisterRoutes(mux)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return disp, "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws/logs?run_id=run-ws"
}

func dialLogStream(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial log stream: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	return conn
}

func expectTermination(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	var chunk protocol.TaskLogChunkPayload
	if err := conn.ReadJSON(&chunk); err != nil {
		t.Fatalf("expected termination message, got error: %v", err)
	}
	if !strings.Contains(chunk.Content, "EXECUTION TERMINATED") {
		t.Fatalf("expected termination message, got %q", chunk.Content)
	}
	// The server closes the stream after the termination message.
	if err := conn.ReadJSON(&chunk); err == nil {
		t.Fatalf("expected the server to close the stream after termination, got %+v", chunk)
	}
}

func TestLogStream_AlreadyFinishedRunGetsTerminationAndCloses(t *testing.T) {
	_, url := newLogStreamEnv(t, storage.StateSuccess)
	expectTermination(t, dialLogStream(t, url))
}

// A stream opened on a live run ends by itself when the agent reports a terminal state.
func TestLogStream_EndsWhenRunFinishes(t *testing.T) {
	disp, url := newLogStreamEnv(t, storage.StateRunning)
	conn := dialLogStream(t, url)

	time.Sleep(100 * time.Millisecond) // let the server subscribe
	disp.HandleStatusUpdate(context.Background(), "agent-1", protocol.TaskStatusUpdatePayload{TaskID: "run-ws", State: "SUCCESS"})

	expectTermination(t, conn)
}

// When the browser disconnects from a run that never finishes, the handler goroutines must exit.
func TestLogStream_ClientDisconnectReleasesGoroutines(t *testing.T) {
	_, url := newLogStreamEnv(t, storage.StateRunning)

	time.Sleep(100 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	conns := make([]*websocket.Conn, 10)
	for i := range conns {
		conns[i] = dialLogStream(t, url)
	}
	time.Sleep(200 * time.Millisecond)
	if runtime.NumGoroutine() <= baseline {
		t.Fatalf("expected extra goroutines while streams are open")
	}

	for _, c := range conns {
		_ = c.Close()
	}
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > baseline+2 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > baseline+2 {
		t.Fatalf("goroutines leaked after clients disconnected: baseline=%d now=%d", baseline, got)
	}
}
