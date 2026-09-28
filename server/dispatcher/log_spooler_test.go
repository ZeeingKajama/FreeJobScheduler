package dispatcher

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
)

func TestRingBufferLog_BoundedCapacity(t *testing.T) {
	capacity := 5
	rb := NewRingBufferLog(capacity)

	for i := 1; i <= 10; i++ {
		rb.Append(protocol.TaskLogChunkPayload{
			TaskID:   "task-test",
			Sequence: int64(i),
			Content:  fmt.Sprintf("line %d\n", i),
		})
	}

	if rb.Count() != capacity {
		t.Fatalf("expected count %d, got %d", capacity, rb.Count())
	}

	chunks := rb.GetAll()
	if len(chunks) != capacity {
		t.Fatalf("expected len %d, got %d", capacity, len(chunks))
	}

	// Should contain sequence 6, 7, 8, 9, 10
	for i, chunk := range chunks {
		expectedSeq := int64(i + 6)
		if chunk.Sequence != expectedSeq {
			t.Errorf("chunk index %d: expected sequence %d, got %d", i, expectedSeq, chunk.Sequence)
		}
	}
}

func TestLogSpooler_DiskPersistence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "spooler_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	spooler, err := NewLogSpooler(tempDir)
	if err != nil {
		t.Fatalf("NewLogSpooler failed: %v", err)
	}
	defer spooler.CloseAll()

	taskID := "task-spool-001"
	lineCount := 100
	var sb strings.Builder

	for i := 1; i <= lineCount; i++ {
		line := fmt.Sprintf("[%04d] Sysout message batch execution", i)
		sb.WriteString(line + "\n")
		if err := spooler.WriteChunk(taskID, line); err != nil {
			t.Fatalf("WriteChunk failed at line %d: %v", i, err)
		}
	}

	// Read full log while still in spooler
	fullLog, err := spooler.ReadFullLog(taskID)
	if err != nil {
		t.Fatalf("ReadFullLog failed: %v", err)
	}

	expectedStr := sb.String()
	if fullLog != expectedStr {
		t.Fatalf("spooled log mismatch: len expected %d, got %d", len(expectedStr), len(fullLog))
	}

	// Close task and verify file exists on disk
	spooler.CloseTask(taskID)
	filePath := filepath.Join(tempDir, fmt.Sprintf("%s.log", taskID))
	info, err := os.Stat(filePath)
	if err != nil || info.Size() == 0 {
		t.Fatalf("expected log file on disk with non-zero size, err=%v", err)
	}
}

func TestRingBufferLog_Concurrent(t *testing.T) {
	rb := NewRingBufferLog(50)
	var wg sync.WaitGroup

	workers := 10
	loops := 200

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < loops; i++ {
				rb.Append(protocol.TaskLogChunkPayload{
					TaskID:   "task-concurrent",
					Sequence: int64(i),
					Content:  fmt.Sprintf("worker %d line %d", workerID, i),
				})
				if i%10 == 0 {
					_ = rb.GetAll()
				}
			}
		}(w)
	}

	wg.Wait()
	if rb.Count() != 50 {
		t.Fatalf("expected count 50, got %d", rb.Count())
	}
}
