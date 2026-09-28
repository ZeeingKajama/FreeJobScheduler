package dispatcher

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
	"github.com/ZeeingKajama/FreeJobScheduler/server/agenthub"
	"github.com/gorilla/websocket"
)

// connectFakeAgent dials hub over a real WebSocket, registers as agentID and waits for the ACK.
// The returned conn lets the test read TASK_DISPATCH messages the hub sends to the agent.
func connectFakeAgent(t *testing.T, hub *agenthub.Hub, agentID string, labels []string, maxConcurrency int32) *websocket.Conn {
	t.Helper()

	ts := httptest.NewServer(hub)
	t.Cleanup(ts.Close)

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial hub: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	reg, _ := protocol.NewEnvelope(protocol.TypeAgentRegister, "reg-"+agentID, protocol.AgentRegisterPayload{
		AgentID:        agentID,
		Hostname:       "test-host",
		OS:             "linux",
		Labels:         labels,
		MaxConcurrency: maxConcurrency,
		AuthToken:      "token",
	})
	if err := conn.WriteJSON(reg); err != nil {
		t.Fatalf("send register: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var ack protocol.Envelope
	if err := conn.ReadJSON(&ack); err != nil || ack.Type != protocol.TypeAgentRegisterAck {
		t.Fatalf("expected register ack, got %+v, err=%v", ack, err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	return conn
}
