package storage

import "testing"

func TestIsValidStateTransition(t *testing.T) {
	tests := []struct {
		current  RunState
		next     RunState
		expected bool
	}{
		// From WAIT
		{StateWait, StateReady, true},
		{StateWait, StateBypass, true},
		{StateWait, StateRunning, false},
		{StateWait, StateSuccess, true},

		// From READY
		{StateReady, StateAssigned, true},
		{StateReady, StateBypass, true},
		{StateReady, StateSuccess, true},

		// From ASSIGNED
		{StateAssigned, StateRunning, true},
		{StateAssigned, StateFailed, true},
		{StateAssigned, StateReady, true}, // Re-queue allowed
		{StateAssigned, StateSuccess, true},

		// From RUNNING
		{StateRunning, StateSuccess, true},
		{StateRunning, StateFailed, true},
		{StateRunning, StateReady, false},
		{StateRunning, StateBypass, true},

		// From Terminal states (cannot transition)
		{StateSuccess, StateRunning, false},
		{StateFailed, StateRunning, false},
		{StateBypass, StateReady, false},
	}

	for _, tt := range tests {
		actual := IsValidStateTransition(tt.current, tt.next)
		if actual != tt.expected {
			t.Errorf("IsValidStateTransition(%s -> %s): expected %v, got %v", tt.current, tt.next, tt.expected, actual)
		}
	}
}
