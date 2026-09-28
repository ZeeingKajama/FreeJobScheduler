//go:build !windows

package executor

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
)

func TestOSExecutor_SuccessAndStreaming(t *testing.T) {
	exec := NewOSExecutor()
	ctx := context.Background()

	logChan := make(chan protocol.TaskLogChunkPayload, 100)

	spec := TaskSpec{
		TaskID:         "test-task-01",
		Command:        "sh",
		Args:           []string{"-c", "echo 'Line 1'; echo 'Line 2'; echo 'Line 3'"},
		TimeoutSeconds: 5,
	}

	result, err := exec.Execute(ctx, spec, logChan)
	close(logChan)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", result.ExitCode)
	}

	var logs []string
	for chunk := range logChan {
		logs = append(logs, chunk.Content)
	}

	if len(logs) != 3 {
		t.Fatalf("expected 3 log lines, got %d: %v", len(logs), logs)
	}

	if logs[0] != "Line 1" || logs[1] != "Line 2" || logs[2] != "Line 3" {
		t.Errorf("unexpected log content: %v", logs)
	}
}

func TestOSExecutor_NonZeroExitCode(t *testing.T) {
	exec := NewOSExecutor()
	ctx := context.Background()

	logChan := make(chan protocol.TaskLogChunkPayload, 10)

	spec := TaskSpec{
		TaskID:         "test-task-02",
		Command:        "sh",
		Args:           []string{"-c", "echo 'Failing task' >&2; exit 42"},
		TimeoutSeconds: 5,
	}

	result, err := exec.Execute(ctx, spec, logChan)
	close(logChan)

	if err != nil {
		t.Fatalf("unexpected error during execution dispatch: %v", err)
	}

	if result.ExitCode != 42 {
		t.Errorf("expected exit code 42, got %d", result.ExitCode)
	}

	var errLog string
	for chunk := range logChan {
		if chunk.Stream == protocol.StreamStderr {
			errLog += chunk.Content
		}
	}
	if !strings.Contains(errLog, "Failing task") {
		t.Errorf("expected stderr to contain 'Failing task', got '%s'", errLog)
	}
}

func TestOSExecutor_TimeoutKiller(t *testing.T) {
	exec := NewOSExecutor()
	ctx := context.Background()

	logChan := make(chan protocol.TaskLogChunkPayload, 10)

	spec := TaskSpec{
		TaskID:         "test-task-03",
		Command:        "sleep",
		Args:           []string{"10"},
		TimeoutSeconds: 1, // Timeout after 1 second
	}

	start := time.Now()
	result, err := exec.Execute(ctx, spec, logChan)
	elapsed := time.Since(start)
	close(logChan)

	if err != nil {
		t.Fatalf("unexpected execute error: %v", err)
	}

	if result.ExitCode != 124 {
		t.Errorf("expected timeout exit code 124, got %d", result.ExitCode)
	}

	if elapsed > 3*time.Second {
		t.Errorf("task should have timed out within ~1-2 seconds, took %v", elapsed)
	}
}

// A grandchild (sleep) inherits the stdout pipe; the timeout must still kill the whole group.
func TestOSExecutor_TimeoutKillsGrandchildren(t *testing.T) {
	exec := NewOSExecutor()
	logChan := make(chan protocol.TaskLogChunkPayload, 10)

	spec := TaskSpec{
		TaskID:         "test-task-03b",
		Command:        "echo hi && sleep 30",
		TimeoutSeconds: 1,
	}

	start := time.Now()
	result, err := exec.Execute(context.Background(), spec, logChan)
	elapsed := time.Since(start)
	close(logChan)

	if err != nil {
		t.Fatalf("unexpected execute error: %v", err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("timeout did not fire promptly, took %v", elapsed)
	}
	if result.ExitCode != 124 {
		t.Errorf("expected timeout exit code 124, got %d", result.ExitCode)
	}
	if result.Error != ErrTaskTimeout {
		t.Errorf("expected ErrTaskTimeout, got %v", result.Error)
	}
}

func TestOSExecutor_CancelProcessGroup(t *testing.T) {
	exec := NewOSExecutor()
	ctx := context.Background()

	logChan := make(chan protocol.TaskLogChunkPayload, 10)

	// Spawns background sleep in child process group
	spec := TaskSpec{
		TaskID:         "test-task-04",
		Command:        "sh",
		Args:           []string{"-c", "sleep 100"},
		TimeoutSeconds: 30,
	}

	done := make(chan *Result)
	go func() {
		res, _ := exec.Execute(ctx, spec, logChan)
		done <- res
	}()

	// Wait for process to start
	time.Sleep(200 * time.Millisecond)

	// Cancel task
	if err := exec.Cancel("test-task-04", "SIGTERM"); err != nil {
		t.Fatalf("failed to cancel: %v", err)
	}

	select {
	case res := <-done:
		close(logChan)
		if res.ExitCode != 130 && res.ExitCode != 143 && res.ExitCode != 137 {
			t.Logf("Process terminated with signal exit code: %d", res.ExitCode)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("process group was not terminated in time")
	}
}
