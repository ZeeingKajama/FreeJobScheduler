//go:build !windows

package executor

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
)

type activeProcess struct {
	cmd       *exec.Cmd
	pgid      int
	cancelled bool
	mu        sync.Mutex
}

// OSExecutor implements ProcessExecutor on POSIX / Linux systems using process groups.
type OSExecutor struct {
	mu        sync.RWMutex
	processes map[string]*activeProcess
}

func NewOSExecutor() *OSExecutor {
	return &OSExecutor{
		processes: make(map[string]*activeProcess),
	}
}

func (e *OSExecutor) Execute(ctx context.Context, spec TaskSpec, logChan chan<- protocol.TaskLogChunkPayload) (*Result, error) {
	if spec.Command == "" {
		return nil, fmt.Errorf("executor: command cannot be empty")
	}

	execCtx := ctx
	var cancel context.CancelFunc
	if spec.TimeoutSeconds > 0 {
		execCtx, cancel = context.WithTimeout(ctx, time.Duration(spec.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	var cmd *exec.Cmd
	if len(spec.Args) == 0 && strings.Contains(spec.Command, " ") {
		cmd = exec.CommandContext(execCtx, "/bin/sh", "-c", spec.Command)
	} else {
		cmd = exec.CommandContext(execCtx, spec.Command, spec.Args...)
	}
	if spec.WorkingDir != "" {
		cmd.Dir = spec.WorkingDir
	}

	// Environment variables
	env := os.Environ()
	for k, v := range spec.Env {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	cmd.Env = env

	// Crucial: Setpgid=true ensures child processes spawn into their own Process Group (PGID)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	// On context expiry, kill the whole process group. exec's default only kills the direct child,
	// and a grandchild holding the stdout pipe would block logWg.Wait() below indefinitely.
	// Setpgid=true makes pgid == pid.
	cmd.Cancel = func() error {
		return e.killProcessGroup(cmd.Process.Pid)
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("executor: failed to create stdout pipe: %w", err)
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("executor: failed to create stderr pipe: %w", err)
	}

	startTime := time.Now()
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("executor: failed to start process: %w", err)
	}

	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		pgid = cmd.Process.Pid
	}

	proc := &activeProcess{
		cmd:  cmd,
		pgid: pgid,
	}

	e.mu.Lock()
	e.processes[spec.TaskID] = proc
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		delete(e.processes, spec.TaskID)
		e.mu.Unlock()
	}()

	var seq int64
	var logWg sync.WaitGroup
	logWg.Add(2)

	// Stream stdout
	go func() {
		defer logWg.Done()
		streamReader(spec.TaskID, stdoutPipe, protocol.StreamStdout, &seq, logChan)
	}()

	// Stream stderr
	go func() {
		defer logWg.Done()
		streamReader(spec.TaskID, stderrPipe, protocol.StreamStderr, &seq, logChan)
	}()

	// Wait for streams to finish
	logWg.Wait()

	// Wait for process termination
	waitErr := cmd.Wait()
	finishTime := time.Now()

	result := &Result{
		TaskID:     spec.TaskID,
		StartedAt:  startTime,
		FinishedAt: finishTime,
	}

	if waitErr != nil {
		if execCtx.Err() == context.DeadlineExceeded {
			e.killProcessGroup(pgid)
			result.ExitCode = 124 // Standard timeout exit code
			result.Error = ErrTaskTimeout
			return result, nil
		}

		proc.mu.Lock()
		wasCancelled := proc.cancelled
		proc.mu.Unlock()

		if wasCancelled {
			result.ExitCode = 130 // Script terminated by signal
			result.Error = ErrTaskCancelled
			return result, nil
		}

		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
			result.Error = waitErr
			return result, nil
		}
		result.ExitCode = 1
		result.Error = waitErr
		return result, nil
	}

	result.ExitCode = 0
	return result, nil
}

func (e *OSExecutor) Cancel(taskID string, signal string) error {
	e.mu.RLock()
	proc, exists := e.processes[taskID]
	e.mu.RUnlock()

	if !exists {
		return ErrAlreadyEnded
	}

	proc.mu.Lock()
	proc.cancelled = true
	pgid := proc.pgid
	proc.mu.Unlock()

	return e.killProcessGroup(pgid)
}

func (e *OSExecutor) killProcessGroup(pgid int) error {
	if pgid <= 0 {
		return nil
	}
	// Send SIGTERM to entire process group (-PGID)
	_ = syscall.Kill(-pgid, syscall.SIGTERM)

	// Wait up to 2 seconds for graceful termination, then force SIGKILL
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(-pgid, 0); err != nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	return nil
}

func streamReader(taskID string, reader io.Reader, stream protocol.StreamType, seq *int64, out chan<- protocol.TaskLogChunkPayload) {
	scanner := bufio.NewScanner(reader)
	// Buffer up to 64KB per line
	buf := make([]byte, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		currentSeq := atomic.AddInt64(seq, 1)
		chunk := protocol.TaskLogChunkPayload{
			TaskID:    taskID,
			Sequence:  currentSeq,
			Stream:    stream,
			Content:   line,
			Timestamp: time.Now().UnixMilli(),
		}
		if out != nil {
			out <- chunk
		}
	}
}
