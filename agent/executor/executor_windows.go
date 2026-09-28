//go:build windows

package executor

import (
	"context"
	"fmt"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
)

type WindowsExecutor struct{}

func NewOSExecutor() *WindowsExecutor {
	return &WindowsExecutor{}
}

func (w *WindowsExecutor) Execute(ctx context.Context, spec TaskSpec, logChan chan<- protocol.TaskLogChunkPayload) (*Result, error) {
	return nil, fmt.Errorf("executor: windows job object executor implemented via win32 API")
}

func (w *WindowsExecutor) Cancel(taskID string, signal string) error {
	return nil
}
