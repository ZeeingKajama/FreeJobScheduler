package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/agent/executor"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
	"github.com/gorilla/websocket"
)

// burstExecutor emits n log chunks (more than the client's log channel can buffer) and exits 0.
type burstExecutor struct{ n int }

func (b burstExecutor) Execute(_ context.Context, spec executor.TaskSpec, logChan chan<- protocol.TaskLogChunkPayload) (*executor.Result, error) {
	for i := 1; i <= b.n; i++ {
		logChan <- protocol.TaskLogChunkPayload{TaskID: spec.TaskID, Sequence: int64(i), Stream: protocol.StreamStdout, Content: "line"}
	}
	return &executor.Result{TaskID: spec.TaskID, ExitCode: 0}, nil
}

func (burstExecutor) Cancel(string, string) error { return nil }

// The final SUCCESS/FAILED status must reach the server only after every log chunk of that task.
func TestClient_FinalStatusSentAfterAllLogChunks(t *testing.T) {
	const logLines = 300

	received := make(chan protocol.Envelope, 4*logLines)
	upgrader := websocket.Upgrader{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		var reg protocol.Envelope
		if err := conn.ReadJSON(&reg); err != nil {
			return
		}
		dispatch, _ := protocol.NewEnvelope(protocol.TypeTaskDispatch, "disp-1", protocol.TaskDispatchPayload{
			TaskID: "task-order", Command: "ignored",
		})
		if err := conn.WriteJSON(dispatch); err != nil {
			return
		}
		for {
			var env protocol.Envelope
			if err := conn.ReadJSON(&env); err != nil {
				return
			}
			received <- env
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := NewClient(AgentConfig{
		ServerURL: "ws" + strings.TrimPrefix(ts.URL, "http"),
		AgentID:   "agent-order",
	}, burstExecutor{n: logLines})
	c.Start(ctx)
	defer c.Stop()

	logsSeen := 0
	for {
		select {
		case env := <-received:
			switch env.Type {
			case protocol.TypeTaskLogChunk:
				logsSeen++
			case protocol.TypeTaskStatusUpdate:
				var st protocol.TaskStatusUpdatePayload
				if err := env.DecodePayload(&st); err != nil {
					t.Fatalf("decode status: %v", err)
				}
				if st.State == "RUNNING" {
					continue
				}
				if logsSeen != logLines {
					t.Fatalf("final status %q arrived after only %d of %d log chunks", st.State, logsSeen, logLines)
				}
				return
			}
		case <-ctx.Done():
			t.Fatalf("timed out; saw %d log chunks and no final status", logsSeen)
		}
	}
}

// failingExecutor ends the task abnormally the way OSExecutor reports a timeout: a Result carrying the error.
type failingExecutor struct{}

func (failingExecutor) Execute(_ context.Context, spec executor.TaskSpec, _ chan<- protocol.TaskLogChunkPayload) (*executor.Result, error) {
	return &executor.Result{TaskID: spec.TaskID, ExitCode: 124, Error: executor.ErrTaskTimeout}, nil
}

func (failingExecutor) Cancel(string, string) error { return nil }

// A FAILED status must carry the reason from the execution result, not only errors that prevented the start.
func TestClient_FailedStatusCarriesResultError(t *testing.T) {
	final := make(chan protocol.TaskStatusUpdatePayload, 1)
	upgrader := websocket.Upgrader{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		var reg protocol.Envelope
		if err := conn.ReadJSON(&reg); err != nil {
			return
		}
		dispatch, _ := protocol.NewEnvelope(protocol.TypeTaskDispatch, "disp-1", protocol.TaskDispatchPayload{
			TaskID: "task-fail", Command: "ignored",
		})
		if err := conn.WriteJSON(dispatch); err != nil {
			return
		}
		for {
			var env protocol.Envelope
			if err := conn.ReadJSON(&env); err != nil {
				return
			}
			var st protocol.TaskStatusUpdatePayload
			if env.Type == protocol.TypeTaskStatusUpdate && env.DecodePayload(&st) == nil && st.State != protocol.TaskRunning {
				final <- st
				return
			}
		}
	}))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := NewClient(AgentConfig{
		ServerURL: "ws" + strings.TrimPrefix(ts.URL, "http"),
		AgentID:   "agent-fail",
	}, failingExecutor{})
	c.Start(ctx)
	defer c.Stop()

	select {
	case st := <-final:
		if st.State != protocol.TaskFailed || st.ExitCode != 124 {
			t.Fatalf("expected FAILED with exit code 124, got %s / %d", st.State, st.ExitCode)
		}
		if st.ErrorMsg != executor.ErrTaskTimeout.Error() {
			t.Fatalf("expected error message %q, got %q", executor.ErrTaskTimeout.Error(), st.ErrorMsg)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for the final status")
	}
}
