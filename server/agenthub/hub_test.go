package agenthub

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
	"github.com/gorilla/websocket"
)

// dialAndRegister connects to the hub, registers agentID and waits for the ACK.
// A background reader keeps running so gorilla's default handler answers server pings with pongs.
func dialAndRegister(t *testing.T, ts *httptest.Server, agentID string) *websocket.Conn {
	t.Helper()

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial hub: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	reg, _ := protocol.NewEnvelope(protocol.TypeAgentRegister, "reg-"+agentID, protocol.AgentRegisterPayload{
		AgentID: agentID, Hostname: "h", OS: "linux", MaxConcurrency: 1,
	})
	if err := conn.WriteJSON(reg); err != nil {
		t.Fatalf("send register: %v", err)
	}
	var ack protocol.Envelope
	if err := conn.ReadJSON(&ack); err != nil || ack.Type != protocol.TypeAgentRegisterAck {
		t.Fatalf("expected register ack, got %+v, err=%v", ack, err)
	}

	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
	return conn
}

// An idle agent sends nothing, so the server itself must ping to keep the connection alive.
func TestHub_IdleConnectionSurvivesReadTimeout(t *testing.T) {
	hub := NewHub("", nil)
	hub.pingInterval = 50 * time.Millisecond
	hub.readTimeout = 200 * time.Millisecond

	ts := httptest.NewServer(hub)
	defer ts.Close()

	dialAndRegister(t, ts, "agent-idle")

	time.Sleep(1200 * time.Millisecond) // 6x readTimeout
	if n := hub.ConnectedAgentsCount(); n != 1 {
		t.Fatalf("idle agent should still be connected after 6x read timeout, got %d sessions", n)
	}
}

type fakeHandler struct {
	mu          sync.Mutex
	disconnects []string
}

func (f *fakeHandler) HandleLogChunk(context.Context, string, protocol.TaskLogChunkPayload)         {}
func (f *fakeHandler) HandleStatusUpdate(context.Context, string, protocol.TaskStatusUpdatePayload) {}
func (f *fakeHandler) HandleTaskAck(context.Context, string, protocol.TaskAckPayload)               {}
func (f *fakeHandler) HandleAgentDisconnect(_ context.Context, agentID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.disconnects = append(f.disconnects, agentID)
}
func (f *fakeHandler) disconnectCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.disconnects)
}

// When an agent reconnects under the same ID before the old connection is noticed as dead,
// the old connection's teardown must not evict the new session or requeue its tasks.
func TestHub_ReconnectSameAgentIDKeepsNewSession(t *testing.T) {
	handler := &fakeHandler{}
	hub := NewHub("", handler)
	ts := httptest.NewServer(hub)
	defer ts.Close()

	oldConn := dialAndRegister(t, ts, "agent-x")
	dialAndRegister(t, ts, "agent-x") // new connection registers while the old one is still open

	hub.mu.RLock()
	newSession := hub.sessions["agent-x"]
	hub.mu.RUnlock()

	// Old connection goes away; its server-side teardown runs now.
	_ = oldConn.Close()
	time.Sleep(300 * time.Millisecond)

	hub.mu.RLock()
	current := hub.sessions["agent-x"]
	hub.mu.RUnlock()
	if current == nil || current != newSession {
		t.Fatalf("new session was evicted by the old connection's teardown")
	}
	if current.closed.Load() {
		t.Fatalf("new session was closed by the old connection's teardown")
	}
	if n := handler.disconnectCount(); n != 0 {
		t.Fatalf("HandleAgentDisconnect must not fire while a newer session is registered, got %d calls", n)
	}
}

// A genuine disconnect of the current session still reports HandleAgentDisconnect exactly once.
func TestHub_DisconnectCurrentSessionNotifiesHandler(t *testing.T) {
	handler := &fakeHandler{}
	hub := NewHub("", handler)
	ts := httptest.NewServer(hub)
	defer ts.Close()

	conn := dialAndRegister(t, ts, "agent-y")
	_ = conn.Close()

	deadline := time.Now().Add(2 * time.Second)
	for handler.disconnectCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := handler.disconnectCount(); n != 1 {
		t.Fatalf("expected 1 disconnect notification, got %d", n)
	}
	if hub.ConnectedAgentsCount() != 0 {
		t.Fatalf("session should be removed after disconnect")
	}
}
