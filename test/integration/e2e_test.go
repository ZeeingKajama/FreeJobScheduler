package integration

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/agent/client"
	"github.com/ZeeingKajama/FreeJobScheduler/agent/executor"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage/storetest"
	"github.com/ZeeingKajama/FreeJobScheduler/server/agenthub"
	"github.com/ZeeingKajama/FreeJobScheduler/server/dispatcher"
	"github.com/ZeeingKajama/FreeJobScheduler/server/engine/runs"
)

func TestMasterAgent_EndToEndIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Setup Server Components
	store := storetest.New(t)
	var hub *agenthub.Hub
	svc := runs.NewService(store)
	disp := dispatcher.NewDispatcher(store, svc, nil, dispatcher.WithLogDir(t.TempDir()))
	hub = agenthub.NewHub("secret-token-123", disp)
	disp.SetHub(hub)

	ts := httptest.NewServer(hub)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")

	// 2. Setup Agent Component
	agentExec := executor.NewOSExecutor()
	agentConfig := client.AgentConfig{
		ServerURL: wsURL,
		AgentID:   "agent-integration-01",
		Hostname:  "test-host",
		OS:        "linux",
		Arch:      "amd64",
		Version:   "v1.0.0",
		Labels:    []string{"linux", "batch"},
		AuthToken: "secret-token-123",
	}

	agentClient := client.NewClient(agentConfig, agentExec)
	agentClient.Start(ctx)
	defer agentClient.Stop()

	// Wait for Agent to connect and register
	for i := 0; i < 30; i++ {
		if hub.ConnectedAgentsCount() > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if hub.ConnectedAgentsCount() == 0 {
		t.Fatalf("agent failed to register within timeout")
	}

	// 3. Seed JobDef and JobRun in READY state
	jobDef := &storage.JobDef{
		ID:            "def-e2e-01",
		Name:          "JOB_E2E_ECHO",
		Group:         "INTEGRATION",
		Command:       "echo",
		Args:          []string{"HELLO FJS E2E SUCCESS"},
		AgentLabels:   []string{"linux"},
		OutConditions: []string{"E2E_JOB_OK"},
		Enabled:       true,
	}
	if err := store.JobDef().Create(ctx, jobDef); err != nil {
		t.Fatalf("failed to create job def: %v", err)
	}

	runID := "run-e2e-001"
	jobRun := &storage.JobRun{
		RunID:       runID,
		JobDefID:    jobDef.ID,
		JobName:     jobDef.Name,
		State:       storage.StateReady,
		Command:     jobDef.Command,
		Args:        jobDef.Args,
		ScheduledAt: time.Now(),
		CreatedDate: "20260916",
	}
	if err := store.JobRun().Create(ctx, jobRun); err != nil {
		t.Fatalf("failed to create job run: %v", err)
	}

	// Setup log collection listener
	var logs []string
	var logMu sync.Mutex
	logReceived := make(chan struct{})

	logCh, unsubscribe := disp.Subscribe(runID)
	defer unsubscribe()
	go func() {
		for chunk := range logCh {
			logMu.Lock()
			logs = append(logs, chunk.Content)
			logMu.Unlock()
			if strings.Contains(chunk.Content, "HELLO FJS E2E SUCCESS") {
				select {
				case <-logReceived:
				default:
					close(logReceived)
				}
			}
		}
	}()

	// 4. Trigger Dispatch
	disp.PollAndDispatch(ctx)

	// 5. Wait for log output
	select {
	case <-logReceived:
		// Log received successfully!
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for execution logs")
	}

	// 6. Wait for terminal status in store
	var finalRun *storage.JobRun
	for i := 0; i < 30; i++ {
		r, err := store.JobRun().GetByID(ctx, runID)
		if err == nil && (r.State == storage.StateSuccess || r.State == storage.StateFailed) {
			finalRun = r
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if finalRun == nil || finalRun.State != storage.StateSuccess {
		t.Fatalf("expected job run state SUCCESS, got: %+v", finalRun)
	}

	if finalRun.ExitCode != 0 {
		t.Errorf("expected exit code 0, got %d", finalRun.ExitCode)
	}

	// 7. Verify Out-Condition was automatically published
	// (the server adds it right after the SUCCESS transition, so poll instead of checking once)
	var condExists bool
	var err error
	for i := 0; i < 30; i++ {
		condExists, err = store.Condition().Exists(ctx, "E2E_JOB_OK", "20260916")
		if err == nil && condExists {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || !condExists {
		t.Errorf("expected Out-Condition E2E_JOB_OK to be published, exists=%v, err=%v", condExists, err)
	}
}
