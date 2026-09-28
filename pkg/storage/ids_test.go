package storage

import (
	"strings"
	"testing"
)

func TestNewID_PrefixAndUniqueness(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 20000; i++ {
		id := NewID("run-def-1")
		if !strings.HasPrefix(id, "run-def-1-") {
			t.Fatalf("id %q lost its prefix", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id generated: %s", id)
		}
		seen[id] = true
	}
}
