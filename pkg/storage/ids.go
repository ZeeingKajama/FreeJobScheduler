package storage

import "github.com/google/uuid"

// NewID returns prefix + "-" + 8 random hex chars, e.g. NewID("run-def-1") -> "run-def-1-3f9a01bc".
// It replaces second/nanosecond timestamp suffixes, which collide when two calls land in the same tick.
func NewID(prefix string) string {
	return prefix + "-" + uuid.NewString()[:8]
}
