package agenthub

import (
	"sync"
	"testing"
)

func TestAgentSession_SlotSemaphore(t *testing.T) {
	session := newAgentSession("agent-1", "host-1", "linux", []string{"prod"}, 5, nil)

	if session.AvailableSlots() != 5 {
		t.Fatalf("expected 5 available slots, got %d", session.AvailableSlots())
	}
	if session.ActiveTasks() != 0 {
		t.Fatalf("expected 0 active tasks, got %d", session.ActiveTasks())
	}

	// Acquire all 5 slots
	for i := 0; i < 5; i++ {
		if !session.TryAcquireSlot() {
			t.Fatalf("expected to acquire slot %d successfully", i+1)
		}
	}

	if session.AvailableSlots() != 0 {
		t.Fatalf("expected 0 available slots, got %d", session.AvailableSlots())
	}
	if session.ActiveTasks() != 5 {
		t.Fatalf("expected 5 active tasks, got %d", session.ActiveTasks())
	}

	// 6th attempt should fail
	if session.TryAcquireSlot() {
		t.Fatalf("expected 6th acquire attempt to fail (capacity reached)")
	}

	// Release 1 slot
	session.ReleaseSlot()
	if session.AvailableSlots() != 1 {
		t.Fatalf("expected 1 available slot after release, got %d", session.AvailableSlots())
	}

	// Acquire again
	if !session.TryAcquireSlot() {
		t.Fatalf("expected acquire to succeed after slot release")
	}

	// Test underflow defense
	for i := 0; i < 10; i++ {
		session.ReleaseSlot()
	}
	if session.ActiveTasks() != 0 {
		t.Fatalf("activeTasks should not drop below 0, got %d", session.ActiveTasks())
	}
}

func TestAgentSession_ConcurrentSlotAcquire(t *testing.T) {
	maxSlots := int32(10)
	session := newAgentSession("agent-concurrent", "host-c", "linux", nil, maxSlots, nil)

	var wg sync.WaitGroup
	var successCount int32
	var mu sync.Mutex

	competitors := 50
	for i := 0; i < competitors; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if session.TryAcquireSlot() {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	if successCount != maxSlots {
		t.Fatalf("expected exactly %d successful acquisitions, got %d", maxSlots, successCount)
	}
	if session.AvailableSlots() != 0 {
		t.Fatalf("expected 0 remaining slots, got %d", session.AvailableSlots())
	}
}

func TestHub_AcquireSlotAndAvailable(t *testing.T) {
	hub := NewHub("secret", nil)

	// Register 2 sessions directly
	s1 := newAgentSession("agent-1", "host-1", "linux", []string{"gpu", "prod"}, 5, nil)
	s2 := newAgentSession("agent-2", "host-2", "linux", []string{"cpu", "prod"}, 3, nil)

	hub.register(s1)
	hub.register(s2)

	// Total available slots for "prod" = 5 + 3 = 8
	if total := hub.TotalAvailableSlots([]string{"prod"}); total != 8 {
		t.Fatalf("expected 8 available slots for prod, got %d", total)
	}
	// Total available slots for "gpu" = 5
	if total := hub.TotalAvailableSlots([]string{"gpu"}); total != 5 {
		t.Fatalf("expected 5 available slots for gpu, got %d", total)
	}

	// Acquire slot for gpu
	acquired, err := hub.AcquireSlotFromAvailableAgent([]string{"gpu"})
	if err != nil || acquired.AgentID != "agent-1" {
		t.Fatalf("expected to acquire from agent-1 for gpu, got %v, err=%v", acquired, err)
	}
	if acquired.AvailableSlots() != 4 {
		t.Fatalf("expected 4 remaining slots on agent-1, got %d", acquired.AvailableSlots())
	}

	// Release slot
	hub.ReleaseAgentSlot("agent-1")
	if acquired.AvailableSlots() != 5 {
		t.Fatalf("expected 5 available slots on agent-1 after release, got %d", acquired.AvailableSlots())
	}
}
