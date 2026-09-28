package agenthub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
	"github.com/gorilla/websocket"
)

// MessageHandler handles application-level messages received from an Agent.
type MessageHandler interface {
	HandleLogChunk(ctx context.Context, agentID string, chunk protocol.TaskLogChunkPayload)
	HandleStatusUpdate(ctx context.Context, agentID string, status protocol.TaskStatusUpdatePayload)
	HandleTaskAck(ctx context.Context, agentID string, ack protocol.TaskAckPayload)
	HandleAgentDisconnect(ctx context.Context, agentID string)
}

// Hub manages active agent WebSocket connections and lifecycle.
type Hub struct {
	mu        sync.RWMutex
	sessions  map[string]*AgentSession
	upgrader  websocket.Upgrader
	authToken string
	handler   MessageHandler

	// Liveness timing: the server pings every pingInterval and drops a connection that
	// stays silent (no message and no pong) for readTimeout.
	pingInterval time.Duration
	readTimeout  time.Duration
}

func NewHub(authToken string, handler MessageHandler) *Hub {
	return &Hub{
		sessions:  make(map[string]*AgentSession),
		authToken: authToken,
		handler:   handler,

		pingInterval: 30 * time.Second,
		readTimeout:  90 * time.Second,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow internal network agents
			},
			ReadBufferSize:  64 * 1024,
			WriteBufferSize: 64 * 1024,
		},
	}
}

// ServeHTTP handles the WebSocket upgrade and event loop.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, "Could not upgrade connection", http.StatusBadRequest)
		return
	}

	go h.handleConnection(conn)
}

func (h *Hub) handleConnection(conn *websocket.Conn) {
	var session *AgentSession
	defer func() {
		if session != nil {
			h.unregister(session)
		}
		_ = conn.Close()
	}()

	// 1. Initial Handshake with timeout (10 seconds)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, msgBytes, err := conn.ReadMessage()
	if err != nil {
		return
	}

	var env protocol.Envelope
	if err := json.Unmarshal(msgBytes, &env); err != nil || env.Type != protocol.TypeAgentRegister {
		return
	}

	var reg protocol.AgentRegisterPayload
	if err := env.DecodePayload(&reg); err != nil {
		return
	}

	// Verify AuthToken
	if h.authToken != "" && reg.AuthToken != h.authToken {
		ack, _ := protocol.NewEnvelope(protocol.TypeAgentRegisterAck, env.ID, protocol.AgentRegisterAckPayload{
			Success:      false,
			ErrorMessage: "invalid auth token",
		})
		ackBytes, _ := json.Marshal(ack)
		_ = conn.WriteMessage(websocket.TextMessage, ackBytes)
		return
	}

	session = newAgentSession(reg.AgentID, reg.Hostname, reg.OS, reg.Labels, reg.MaxConcurrency, conn)
	h.register(session)

	// Send Register ACK
	ack, _ := protocol.NewEnvelope(protocol.TypeAgentRegisterAck, env.ID, protocol.AgentRegisterAckPayload{
		Success:    true,
		AssignedID: reg.AgentID,
		ServerTime: time.Now().UnixMilli(),
	})
	if err := session.Send(ack); err != nil {
		return
	}

	// 2. Main read loop
	_ = conn.SetReadDeadline(time.Now().Add(h.readTimeout))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(h.readTimeout))
		return nil
	})

	pingDone := make(chan struct{})
	defer close(pingDone)
	go h.pingLoop(conn, pingDone)

	for {
		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			break
		}

		_ = conn.SetReadDeadline(time.Now().Add(h.readTimeout))

		var incomingEnv protocol.Envelope
		if err := json.Unmarshal(msgBytes, &incomingEnv); err != nil {
			continue
		}

		ctx := context.Background()
		switch incomingEnv.Type {
		case protocol.TypeTaskAck:
			var ack protocol.TaskAckPayload
			if err := incomingEnv.DecodePayload(&ack); err == nil && h.handler != nil {
				h.handler.HandleTaskAck(ctx, session.AgentID, ack)
			}

		case protocol.TypeTaskLogChunk:
			var chunk protocol.TaskLogChunkPayload
			if err := incomingEnv.DecodePayload(&chunk); err == nil && h.handler != nil {
				h.handler.HandleLogChunk(ctx, session.AgentID, chunk)
			}

		case protocol.TypeTaskStatusUpdate:
			var status protocol.TaskStatusUpdatePayload
			if err := incomingEnv.DecodePayload(&status); err == nil && h.handler != nil {
				h.handler.HandleStatusUpdate(ctx, session.AgentID, status)
			}
		}
	}
}

// pingLoop sends a WebSocket ping every pingInterval so idle agents keep refreshing the read deadline
// through their pongs. WriteControl is safe to call concurrently with other writes.
func (h *Hub) pingLoop(conn *websocket.Conn, done <-chan struct{}) {
	ticker := time.NewTicker(h.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
				return
			}
		}
	}
}

// register makes s the session for its agent ID. A previous session with the same ID is closed;
// its read loop then ends and its unregister becomes a no-op because it is no longer current.
func (h *Hub) register(s *AgentSession) {
	h.mu.Lock()
	old := h.sessions[s.AgentID]
	h.sessions[s.AgentID] = s
	h.mu.Unlock()

	if old != nil && old != s {
		_ = old.Close()
	}
}

// unregister closes s and, only if s is still the registered session for its agent ID,
// removes it and notifies the handler. A superseded session must not evict its replacement
// or requeue tasks that now belong to it.
func (h *Hub) unregister(s *AgentSession) {
	_ = s.Close()

	h.mu.Lock()
	current := h.sessions[s.AgentID] == s
	if current {
		delete(h.sessions, s.AgentID)
	}
	h.mu.Unlock()

	if current && h.handler != nil {
		h.handler.HandleAgentDisconnect(context.Background(), s.AgentID)
	}
}

// AcquireSlotFromAvailableAgent finds an active matching agent and atomically acquires 1 execution slot.
func (h *Hub) AcquireSlotFromAvailableAgent(requiredLabels []string) (*AgentSession, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, session := range h.sessions {
		if session.closed.Load() || !session.MatchesLabels(requiredLabels) {
			continue
		}
		if session.TryAcquireSlot() {
			return session, nil
		}
	}
	return nil, fmt.Errorf("no agent available with matching labels and free execution slots")
}

// TotalAvailableSlots returns the total sum of idle execution slots across matching connected agents.
func (h *Hub) TotalAvailableSlots(requiredLabels []string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	total := 0
	for _, session := range h.sessions {
		if !session.closed.Load() && session.MatchesLabels(requiredLabels) {
			total += int(session.AvailableSlots())
		}
	}
	return total
}

// ReleaseAgentSlot atomically frees 1 execution slot on the specified agent session.
func (h *Hub) ReleaseAgentSlot(agentID string) {
	h.mu.RLock()
	session, exists := h.sessions[agentID]
	h.mu.RUnlock()

	if exists && session != nil {
		session.ReleaseSlot()
	}
}

// ConnectedAgentsCount returns the number of active agents.
func (h *Hub) ConnectedAgentsCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.sessions)
}

// AgentInfo holds snapshot metadata of a connected agent worker.
type AgentInfo struct {
	AgentID        string   `json:"agent_id"`
	Hostname       string   `json:"hostname"`
	OS             string   `json:"os"`
	Labels         []string `json:"labels"`
	MaxConcurrency int32    `json:"max_concurrency"`
	ActiveTasks    int32    `json:"active_tasks"`
	AvailableSlots int32    `json:"available_slots"`
}

// GetConnectedAgents returns a list of active connected agent snapshots.
func (h *Hub) GetConnectedAgents() []AgentInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make([]AgentInfo, 0, len(h.sessions))
	for _, s := range h.sessions {
		if s != nil && !s.closed.Load() {
			result = append(result, AgentInfo{
				AgentID:        s.AgentID,
				Hostname:       s.Hostname,
				OS:             s.OS,
				Labels:         s.Labels,
				MaxConcurrency: s.MaxConcurrency,
				ActiveTasks:    s.ActiveTasks(),
				AvailableSlots: s.AvailableSlots(),
			})
		}
	}
	return result
}
