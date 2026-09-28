package executor

import (
	"context"
	"errors"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
)

var (
	ErrTaskTimeout   = errors.New("executor: task execution timed out")
	ErrTaskCancelled = errors.New("executor: task execution cancelled")
	ErrAlreadyEnded  = errors.New("executor: process already terminated")
)

// TaskSpec defines the command and environment to run.
type TaskSpec struct {
	TaskID         string
	Command        string
	Args           []string
	Env            map[string]string
	WorkingDir     string
	RunAsUser      string
	TimeoutSeconds int
}

// Result holds the final outcome of an execution.
type Result struct {
	TaskID     string
	ExitCode   int
	Error      error
	StartedAt  time.Time
	FinishedAt time.Time
}

// ProcessExecutor defines the interface for running isolated process groups.
type ProcessExecutor interface {
	Execute(ctx context.Context, spec TaskSpec, logChan chan<- protocol.TaskLogChunkPayload) (*Result, error)
	Cancel(taskID string, signal string) error
}
