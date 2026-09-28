package agenthub

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
	"github.com/gorilla/websocket"
)

// AgentSession represents an active WebSocket connection with a worker Agent.
type AgentSession struct {
	AgentID        string
	Hostname       string
	OS             string
	Labels         []string
	MaxConcurrency int32 // Maximum simultaneous task executions allowed on this agent
	activeTasks    int32 // Current running tasks on this agent (atomic)
	conn           *websocket.Conn
	writeMu        sync.Mutex
	closed         atomic.Bool
}

func newAgentSession(agentID, hostname, os string, labels []string, maxConcurrency int32, conn *websocket.Conn) *AgentSession {
	if maxConcurrency <= 0 {
		maxConcurrency = 10 // Default fallback capacity
	}
	return &AgentSession{
		AgentID:        agentID,
		Hostname:       hostname,
		OS:             os,
		Labels:         labels,
		MaxConcurrency: maxConcurrency,
		conn:           conn,
	}
}

// TryAcquireSlot atomically checks and consumes 1 execution slot if available.
// Returns true if acquired, false if at capacity.
func (s *AgentSession) TryAcquireSlot() bool {
	for {
		current := atomic.LoadInt32(&s.activeTasks)
		if current >= s.MaxConcurrency {
			return false
		}
		if atomic.CompareAndSwapInt32(&s.activeTasks, current, current+1) {
			return true
		}
	}
}

// ReleaseSlot atomically decrements the active task count upon task completion or failure.
func (s *AgentSession) ReleaseSlot() {
	for {
		current := atomic.LoadInt32(&s.activeTasks)
		if current <= 0 {
			return // Prevent underflow
		}
		if atomic.CompareAndSwapInt32(&s.activeTasks, current, current-1) {
			return
		}
	}
}

// AvailableSlots returns the number of remaining execution slots.
func (s *AgentSession) AvailableSlots() int32 {
	avail := s.MaxConcurrency - atomic.LoadInt32(&s.activeTasks)
	if avail < 0 {
		return 0
	}
	return avail
}

// ActiveTasks returns the current number of actively running tasks.
func (s *AgentSession) ActiveTasks() int32 {
	return atomic.LoadInt32(&s.activeTasks)
}

func (s *AgentSession) Send(env *protocol.Envelope) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.closed.Load() {
		return fmt.Errorf("session is closed")
	}

	bytes, err := json.Marshal(env)
	if err != nil {
		return err
	}

	_ = s.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return s.conn.WriteMessage(websocket.TextMessage, bytes)
}

func (s *AgentSession) Close() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.closed.Swap(true) {
		return nil
	}
	return s.conn.Close()
}

func (s *AgentSession) MatchesLabels(required []string) bool {
	if len(required) == 0 {
		return true
	}
	labelMap := make(map[string]bool, len(s.Labels))
	for _, l := range s.Labels {
		labelMap[l] = true
	}
	for _, r := range required {
		if !labelMap[r] {
			return false
		}
	}
	return true
}
