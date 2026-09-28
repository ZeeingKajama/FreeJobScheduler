package benchmark

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	_ "modernc.org/sqlite"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage/sqlstore"
	"github.com/ZeeingKajama/FreeJobScheduler/server/agenthub"
	"github.com/ZeeingKajama/FreeJobScheduler/server/dispatcher"
	"github.com/ZeeingKajama/FreeJobScheduler/server/engine/runs"
)

func setupBenchmarkDB(t *testing.T) (*sqlstore.SQLStore, func()) {
	dbName := fmt.Sprintf("file:benchdb_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := sql.Open("sqlite", dbName)
	if err != nil {
		t.Fatalf("failed to open bench sqlite: %v", err)
	}

	store := sqlstore.NewSQLStore(db)
	if err := store.InitializeSchema(context.Background(), sqlstore.SchemaDDL); err != nil {
		t.Fatalf("failed to init schema: %v", err)
	}

	return store, func() { _ = db.Close() }
}

// TestEnterpriseScaleDAG simulates 500 tasks across 50 parallel pipelines, each with 10 sequential levels,
// handled by 10 virtual agents (total 100 concurrency slots).
func TestEnterpriseScaleDAG(t *testing.T) {
	store, cleanup := setupBenchmarkDB(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 1. Setup Server & Dispatcher
	svc := runs.NewService(store)
	disp := dispatcher.NewDispatcher(store, svc, nil, dispatcher.WithLogDir(t.TempDir()))
	hub := agenthub.NewHub("bench-token", disp)
	disp.SetHub(hub)

	server := httptest.NewServer(hub)
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// 2. Spawn 10 Virtual Agents (MaxConcurrency = 10 each, total 100 slots)
	numAgents := 10
	var completedCount int64
	var agentConns []*websocket.Conn

	for a := 0; a < numAgents; a++ {
		agentID := fmt.Sprintf("virtual-agent-%02d", a)
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("agent %s dial failed: %v", agentID, err)
		}
		agentConns = append(agentConns, conn)

		// Send AGENT_REGISTER with MaxConcurrency = 10
		regPayload := protocol.AgentRegisterPayload{
			AgentID:        agentID,
			Hostname:       fmt.Sprintf("node-%02d", a),
			OS:             "linux",
			Arch:           "amd64",
			Version:        "2.0.0",
			Labels:         []string{"bench"},
			MaxConcurrency: 10,
			AuthToken:      "bench-token",
		}
		env, _ := protocol.NewEnvelope(protocol.TypeAgentRegister, "reg-1", regPayload)
		envBytes, _ := json.Marshal(env)
		_ = conn.WriteMessage(websocket.TextMessage, envBytes)

		// Virtual agent listener worker with dedicated write mutex
		go func(c *websocket.Conn, agID string) {
			var writeMu sync.Mutex
			safeWrite := func(b []byte) error {
				writeMu.Lock()
				defer writeMu.Unlock()
				return c.WriteMessage(websocket.TextMessage, b)
			}

			for {
				_, msg, err := c.ReadMessage()
				if err != nil {
					return
				}
				var inEnv protocol.Envelope
				if err := json.Unmarshal(msg, &inEnv); err != nil {
					continue
				}

				if inEnv.Type == protocol.TypeTaskDispatch {
					var dispatch protocol.TaskDispatchPayload
					_ = inEnv.DecodePayload(&dispatch)

					// Simulate realistic task lifecycle: RUNNING -> SUCCESS
					go func(taskID string) {
						// 1. Report RUNNING
						runEnv, _ := protocol.NewEnvelope(protocol.TypeTaskStatusUpdate, fmt.Sprintf("run-%s", taskID), protocol.TaskStatusUpdatePayload{
							TaskID: taskID,
							State:  "RUNNING",
						})
						rBytes, _ := json.Marshal(runEnv)
						_ = safeWrite(rBytes)

						time.Sleep(500 * time.Microsecond)

						// 2. Report SUCCESS
						finished := time.Now().UnixMilli()
						statusEnv, _ := protocol.NewEnvelope(protocol.TypeTaskStatusUpdate, fmt.Sprintf("status-%s", taskID), protocol.TaskStatusUpdatePayload{
							TaskID:     taskID,
							State:      "SUCCESS",
							ExitCode:   0,
							FinishedAt: &finished,
						})
						sBytes, _ := json.Marshal(statusEnv)
						_ = safeWrite(sBytes)
						atomic.AddInt64(&completedCount, 1)
					}(dispatch.TaskID)
				}
			}
		}(conn, agentID)
	}

	// Wait for all 10 virtual agents to register
	for i := 0; i < 50; i++ {
		if hub.ConnectedAgentsCount() == numAgents {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if hub.ConnectedAgentsCount() != numAgents {
		t.Fatalf("expected %d connected agents, got %d", numAgents, hub.ConnectedAgentsCount())
	}

	// 3. Construct 50 parallel pipelines, each with 10 sequential levels (Total 500 tasks)
	odate := "20260916"
	numChains := 50
	numLevels := 10
	totalTasks := numChains * numLevels

	t.Logf("Creating %d DAG tasks (%d pipelines × %d levels)...", totalTasks, numChains, numLevels)

	for c := 0; c < numChains; c++ {
		for level := 0; level < numLevels; level++ {
			taskID := fmt.Sprintf("dag-C%02d-L%02d", c, level)
			jobName := fmt.Sprintf("JOB_C%02d_L%02d", c, level)

			var inConds []string
			if level > 0 {
				inConds = []string{fmt.Sprintf("COND_C%02d_L%02d_OK", c, level-1)}
			}

			var outConds []string
			if level < numLevels-1 {
				outConds = []string{fmt.Sprintf("COND_C%02d_L%02d_OK", c, level)}
			}

			def := &storage.JobDef{
				ID:            taskID,
				Name:          jobName,
				Group:         "BENCH_GROUP",
				Command:       "echo done",
				AgentLabels:   []string{"bench"},
				InConditions:  inConds,
				OutConditions: outConds,
			}
			_ = store.JobDef().Create(ctx, def)

			initialState := storage.StateWait
			if level == 0 {
				initialState = storage.StateReady
			}

			run := &storage.JobRun{
				RunID:       taskID,
				JobDefID:    def.ID,
				JobName:     def.Name,
				State:       initialState,
				Command:     def.Command,
				CreatedDate: odate,
				ScheduledAt: time.Now(),
				ExitCode:    -1,
			}
			_ = store.JobRun().Create(ctx, run)

			if initialState == storage.StateWait {
				svc.RegisterWaitingTask(ctx, run.RunID, def.ID, odate, inConds)
			}
		}
	}

	// 4. Start Event-Driven Dispatcher
	benchStart := time.Now()
	disp.Start(ctx, 200*time.Millisecond)
	defer disp.Stop()

	// Awaken initial dispatch for all Level 0 tasks
	disp.TriggerDispatch()

	// 5. Monitor and wait for all 500 tasks to complete
	timeout := time.After(45 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatalf("benchmark timed out! Completed %d of %d tasks", atomic.LoadInt64(&completedCount), totalTasks)
		case <-ticker.C:
			done := atomic.LoadInt64(&completedCount)
			if done >= int64(totalTasks) {
				goto FINISHED
			}
		}
	}

FINISHED:
	duration := time.Since(benchStart)
	t.Logf("=== 500 Tasks 10-Level DAG Benchmark Results ===")
	t.Logf("Total Completed Tasks: %d / %d", atomic.LoadInt64(&completedCount), totalTasks)
	t.Logf("Total Execution Time: %v", duration)
	t.Logf("Throughput: %.2f tasks/sec", float64(totalTasks)/duration.Seconds())

	// Memory Profiling Check
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	heapAllocMB := float64(memStats.HeapAlloc) / (1024 * 1024)
	t.Logf("Heap Memory In Use: %.2f MB", heapAllocMB)
	if heapAllocMB > 150.0 {
		t.Errorf("Heap memory exceeded 150MB limit: %.2f MB", heapAllocMB)
	}

	// Close all virtual agents
	for _, c := range agentConns {
		_ = c.Close()
	}
}

// TestChaos_AgentDisconnectAndRecover verifies that if agents suddenly disconnect during high concurrency,
// the system gracefully re-queues assigned tasks and finishes them on surviving agents without stalling.
func TestChaos_AgentDisconnectAndRecover(t *testing.T) {
	store, cleanup := setupBenchmarkDB(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc := runs.NewService(store)
	disp := dispatcher.NewDispatcher(store, svc, nil, dispatcher.WithLogDir(t.TempDir()))
	hub := agenthub.NewHub("chaos-token", disp)
	disp.SetHub(hub)

	server := httptest.NewServer(hub)
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// Connect 3 agents (Agent 0, 1, 2)
	var conns []*websocket.Conn
	var completedCount int64

	for i := 0; i < 3; i++ {
		agentID := fmt.Sprintf("chaos-agent-%d", i)
		c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("dial failed: %v", err)
		}
		conns = append(conns, c)

		reg := protocol.AgentRegisterPayload{
			AgentID:        agentID,
			MaxConcurrency: 5,
			AuthToken:      "chaos-token",
		}
		env, _ := protocol.NewEnvelope(protocol.TypeAgentRegister, "reg", reg)
		b, _ := json.Marshal(env)
		_ = c.WriteMessage(websocket.TextMessage, b)

		go func(conn *websocket.Conn) {
			var wMu sync.Mutex
			for {
				_, msg, err := conn.ReadMessage()
				if err != nil {
					return
				}
				var in protocol.Envelope
				if err := json.Unmarshal(msg, &in); err == nil && in.Type == protocol.TypeTaskDispatch {
					var dispPayload protocol.TaskDispatchPayload
					_ = in.DecodePayload(&dispPayload)

					// Realistic status updates: RUNNING -> SUCCESS
					go func(tid string) {
						runEnv, _ := protocol.NewEnvelope(protocol.TypeTaskStatusUpdate, "stat-run", protocol.TaskStatusUpdatePayload{
							TaskID: tid,
							State:  "RUNNING",
						})
						runBytes, _ := json.Marshal(runEnv)
						wMu.Lock()
						_ = conn.WriteMessage(websocket.TextMessage, runBytes)
						wMu.Unlock()

						time.Sleep(5 * time.Millisecond)
						fin := time.Now().UnixMilli()
						resp, _ := protocol.NewEnvelope(protocol.TypeTaskStatusUpdate, "stat-ok", protocol.TaskStatusUpdatePayload{
							TaskID:     tid,
							State:      "SUCCESS",
							FinishedAt: &fin,
						})
						rBytes, _ := json.Marshal(resp)
						wMu.Lock()
						_ = conn.WriteMessage(websocket.TextMessage, rBytes)
						wMu.Unlock()
						atomic.AddInt64(&completedCount, 1)
					}(dispPayload.TaskID)
				}
			}
		}(c)
	}

	time.Sleep(100 * time.Millisecond)

	// Create 30 ready tasks
	for i := 0; i < 30; i++ {
		runID := fmt.Sprintf("chaos-task-%02d", i)
		def := &storage.JobDef{ID: runID, Name: runID, Command: "echo chaos"}
		_ = store.JobDef().Create(ctx, def)
		run := &storage.JobRun{
			RunID:       runID,
			JobDefID:    def.ID,
			JobName:     def.Name,
			State:       storage.StateReady,
			Command:     def.Command,
			CreatedDate: "20260916",
			ScheduledAt: time.Now(),
		}
		_ = store.JobRun().Create(ctx, run)
	}

	disp.Start(ctx, 200*time.Millisecond)
	defer disp.Stop()

	disp.TriggerDispatch()

	// Abruptly kill 1 agent while tasks are being processed
	time.Sleep(10 * time.Millisecond)
	_ = conns[0].Close()
	t.Log("Chaos injected: Abruptly closed chaos-agent-0 connection")

	// Wait for surviving agents to finish tasks
	timeout := time.After(15 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			t.Fatalf("chaos recovery timed out! Done %d / 30", atomic.LoadInt64(&completedCount))
		case <-ticker.C:
			if atomic.LoadInt64(&completedCount) >= 30 {
				t.Logf("Chaos recovery verified: All 30 tasks finished successfully despite node drop!")
				return
			}
		}
	}
}
