package dispatcher

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
	"github.com/ZeeingKajama/FreeJobScheduler/server/agenthub"
	"github.com/ZeeingKajama/FreeJobScheduler/server/engine/runs"
)

// historyLines is how many recent log lines are retained in memory / served as history.
const historyLines = 1000

// AgentPool is the part of agenthub.Hub the dispatcher needs: agents' execution slots.
type AgentPool interface {
	TotalAvailableSlots(requiredLabels []string) int
	AcquireSlotFromAvailableAgent(requiredLabels []string) (*agenthub.AgentSession, error)
	ReleaseAgentSlot(agentID string)
}

// Dispatcher assigns READY runs to agents with free slots, tracks their slots and handles their logs.
// Run lifecycle (ordering, conditions, applying reported status) belongs to runs.Service.
type Dispatcher struct {
	store      storage.Store
	runs       *runs.Service
	hub        AgentPool
	logDir     string
	logSpooler *LogSpooler
	taskLogs   sync.Map // taskID -> *RingBufferLog (bounded 1,000 chunks); dropped when the task finishes

	logMu     sync.Mutex
	logSubs   map[string]map[int]chan protocol.TaskLogChunkPayload // taskID -> subscription ID -> live log channel
	nextSubID int

	wakeChan chan struct{}
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// DefaultLogDir is where run logs are spooled unless WithLogDir says otherwise.
const DefaultLogDir = "logs/runs"

// Option customizes a Dispatcher at construction.
type Option func(*Dispatcher)

// WithLogDir sets the directory holding the per-run log files.
func WithLogDir(dir string) Option {
	return func(d *Dispatcher) { d.logDir = dir }
}

// NewDispatcher builds a dispatcher. hub may be nil and supplied later with SetHub: the hub delivers
// agent messages to the dispatcher, so the two cannot both be handed their counterpart at construction.
func NewDispatcher(store storage.Store, svc *runs.Service, hub AgentPool, opts ...Option) *Dispatcher {
	d := &Dispatcher{
		store:    store,
		runs:     svc,
		hub:      hub,
		logDir:   DefaultLogDir,
		logSubs:  make(map[string]map[int]chan protocol.TaskLogChunkPayload),
		wakeChan: make(chan struct{}, 1),
		stopChan: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(d)
	}

	spooler, err := NewLogSpooler(d.logDir)
	if err != nil {
		log.Printf("[Dispatcher] Warning: log spooler unavailable, run logs will not be persisted: %v", err)
	} else {
		d.logSpooler = spooler
	}
	return d
}

// SetHub supplies the agent pool when it could not be passed to NewDispatcher.
func (d *Dispatcher) SetHub(hub AgentPool) {
	d.hub = hub
}

// TriggerDispatch non-blockingly signals the event loop to awaken immediately.
func (d *Dispatcher) TriggerDispatch() {
	select {
	case d.wakeChan <- struct{}{}:
	default:
		// Coalesce duplicate wakes
	}
}

// Start begins the event-driven dispatch loop with jittered fallback safety polling.
func (d *Dispatcher) Start(ctx context.Context, fallbackInterval ...time.Duration) {
	interval := 5 * time.Second
	if len(fallbackInterval) > 0 && fallbackInterval[0] > 0 {
		interval = fallbackInterval[0]
	}

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()

		// Jittered safety fallback timer
		getJitterTimer := func() *time.Timer {
			jitter := time.Duration(rand.Int63n(int64(interval / 2)))
			return time.NewTimer(interval + jitter)
		}

		fallbackTimer := getJitterTimer()
		defer fallbackTimer.Stop()

		// Instant event trigger: poll now and push the fallback poll back
		pollNow := func() {
			d.PollAndDispatch(ctx)
			if !fallbackTimer.Stop() {
				select {
				case <-fallbackTimer.C:
				default:
				}
			}
			fallbackTimer.Reset(interval)
		}

		// Initial poll on startup
		d.PollAndDispatch(ctx)

		for {
			select {
			case <-ctx.Done():
				return
			case <-d.stopChan:
				return

			case <-d.wakeChan:
				// Slot released / agent disconnected
				pollNow()

			case <-d.runs.Dispatchable():
				// A run became READY (ordered, released by a condition, rerun)
				pollNow()

			case <-fallbackTimer.C:
				// Fallback safety poll for lost-wakeup defense
				d.PollAndDispatch(ctx)
				fallbackTimer = getJitterTimer()
			}
		}
	}()
}

// Stop gracefully shuts down the dispatcher and closes open log spoolers.
func (d *Dispatcher) Stop() {
	close(d.stopChan)
	d.wg.Wait()
	if d.logSpooler != nil {
		d.logSpooler.CloseAll()
	}
}

// defaultTimeoutSec applies to jobs whose definition does not set TimeoutSec.
const defaultTimeoutSec = 300

// readyScanWindow bounds how many READY runs one poll inspects when looking for runnable work.
const readyScanWindow = 500

// PollAndDispatch matches READY runs to agents with free slots (oldest first) and dispatches them.
// A run is only claimed (READY -> ASSIGNED) after a slot on a label-matching agent has been acquired,
// so runs that no connected agent can execute never block runnable ones behind them.
func (d *Dispatcher) PollAndDispatch(ctx context.Context) {
	if d.hub == nil {
		return
	}

	// 1. Check total available slots across connected agents
	if d.hub.TotalAvailableSlots(nil) <= 0 {
		return // All agents fully occupied, prevent fork storms
	}

	// 2. Fetch READY candidates (FIFO)
	ready, err := d.store.JobRun().ListReady(ctx, readyScanWindow)
	if err != nil || len(ready) == 0 {
		return // Nothing ready
	}

	// 3. Match candidates to agents with free slots, claiming only those that got a slot
	defs := make(map[string]*storage.JobDef)
	for _, run := range ready {
		if d.hub.TotalAvailableSlots(nil) <= 0 {
			return // Every slot is taken; remaining candidates wait for the next wake
		}

		def, cached := defs[run.JobDefID]
		if !cached {
			def, _ = d.store.JobDef().GetByID(ctx, run.JobDefID)
			defs[run.JobDefID] = def
		}
		var agentLabels []string
		timeoutSec := defaultTimeoutSec
		if def != nil {
			agentLabels = def.AgentLabels
			if def.TimeoutSec > 0 {
				timeoutSec = def.TimeoutSec
			}
		}

		// Atomically acquire slot from eligible agent
		agent, err := d.hub.AcquireSlotFromAvailableAgent(agentLabels)
		if err != nil || agent == nil {
			// No matching agent with a free slot: leave the run READY and look at the next candidate
			continue
		}

		// Claim the run (CAS). Another dispatcher pass may have taken it already.
		if err := d.store.JobRun().TransitionState(ctx, run.RunID, storage.StateReady, storage.StateAssigned, 0, ""); err != nil {
			agent.ReleaseSlot()
			continue
		}

		// Bind Agent ID
		_ = d.store.JobRun().SetAgentID(ctx, run.RunID, agent.AgentID)

		// Build TaskDispatch payload
		payload := protocol.TaskDispatchPayload{
			TaskID:         run.RunID,
			JobName:        run.JobName,
			Command:        run.Command,
			Args:           run.Args,
			Env:            run.Env,
			TimeoutSeconds: timeoutSec,
		}

		env, err := protocol.NewEnvelope(protocol.TypeTaskDispatch, fmt.Sprintf("disp-%s", run.RunID), payload)
		if err != nil {
			agent.ReleaseSlot()
			_ = d.store.JobRun().TransitionState(ctx, run.RunID, storage.StateAssigned, storage.StateReady, 0, "Payload build failed")
			continue
		}

		// Send to agent
		if err := agent.Send(env); err != nil {
			agent.ReleaseSlot()
			_ = d.store.JobRun().TransitionState(ctx, run.RunID, storage.StateAssigned, storage.StateReady, 0, "Agent dispatch send failed")
		}
	}
}

// HandleTaskAck implements agenthub.MessageHandler.
func (d *Dispatcher) HandleTaskAck(ctx context.Context, agentID string, ack protocol.TaskAckPayload) {
	// Acknowledged by agent
}

// HandleAgentDisconnect recovers tasks assigned to the disconnected agent and triggers re-dispatch.
func (d *Dispatcher) HandleAgentDisconnect(ctx context.Context, agentID string) {
	assignedRuns, err := d.store.JobRun().ListByState(ctx, []storage.RunState{storage.StateAssigned})
	if err != nil {
		return
	}

	requeued := false
	for _, run := range assignedRuns {
		if run.AgentID == agentID {
			_ = d.store.JobRun().TransitionState(ctx, run.RunID, storage.StateAssigned, storage.StateReady, 0, "Agent disconnected, task re-queued")
			requeued = true
		}
	}

	if requeued {
		d.TriggerDispatch()
	}
}

// HandleLogChunk implements agenthub.MessageHandler.
func (d *Dispatcher) HandleLogChunk(ctx context.Context, agentID string, chunk protocol.TaskLogChunkPayload) {
	// 1. Maintain bounded 1,000 chunks ring-buffer in memory (Zero OOM risk)
	bufVal, _ := d.taskLogs.LoadOrStore(chunk.TaskID, NewRingBufferLog(historyLines))
	rb := bufVal.(*RingBufferLog)
	rb.Append(chunk)

	// 2. Persist to disk spooler asynchronously with 64KB buffering (Compliance guarantee)
	if d.logSpooler != nil {
		_ = d.logSpooler.WriteChunk(chunk.TaskID, chunk.Content)
	}

	// 3. Dispatch to live streaming subscribers (non-blocking: a slow reader drops chunks)
	d.logMu.Lock()
	for _, ch := range d.logSubs[chunk.TaskID] {
		select {
		case ch <- chunk:
		default:
		}
	}
	d.logMu.Unlock()
}

// GetHistoricalLogs returns the most recent log chunks of a task: the in-memory ring buffer while the
// task is active, or the tail of the disk spool file once it finished (Sequence is then the line number).
func (d *Dispatcher) GetHistoricalLogs(taskID string) []protocol.TaskLogChunkPayload {
	if bufVal, ok := d.taskLogs.Load(taskID); ok {
		return bufVal.(*RingBufferLog).GetAll()
	}
	if d.logSpooler == nil {
		return nil
	}

	lines, first, err := d.logSpooler.ReadTail(taskID, historyLines)
	if err != nil || len(lines) == 0 {
		return nil
	}
	chunks := make([]protocol.TaskLogChunkPayload, len(lines))
	for i, line := range lines {
		chunks[i] = protocol.TaskLogChunkPayload{
			TaskID:   taskID,
			Sequence: int64(first + i),
			Stream:   protocol.StreamStdout, // the spool file does not record the stream
			Content:  line,
		}
	}
	return chunks
}

// GetFullDiskLog returns complete audit-grade historical logs from disk spool file.
func (d *Dispatcher) GetFullDiskLog(taskID string) (string, error) {
	if d.logSpooler == nil {
		return "", fmt.Errorf("log spooler not initialized")
	}
	return d.logSpooler.ReadFullLog(taskID)
}

// HandleStatusUpdate implements agenthub.MessageHandler. The run itself is updated by runs.Service;
// here the agent's slot and the run's log resources are released once it reported a terminal state.
func (d *Dispatcher) HandleStatusUpdate(ctx context.Context, agentID string, status protocol.TaskStatusUpdatePayload) {
	if err := d.runs.OnAgentStatus(ctx, agentID, status); err != nil {
		return
	}

	// The agent has finished regardless of whether the transition applied (an operator may
	// have already forced the run to SUCCESS/BYPASS), so its slot and log spool are released.
	if status.State == protocol.TaskSuccess || status.State == protocol.TaskFailed {
		d.finishTask(agentID, status.TaskID)
		// Awaken dispatch loop immediately to fill newly freed agent slot
		d.TriggerDispatch()
	}
}

// finishTask releases the agent slot held by a task that reported a terminal state,
// flushes/closes its log spool file, closes live log subscriptions and frees the in-memory log buffer.
func (d *Dispatcher) finishTask(agentID, taskID string) {
	if d.hub != nil {
		d.hub.ReleaseAgentSlot(agentID)
	}
	if d.logSpooler != nil {
		d.logSpooler.CloseTask(taskID)
	}

	// Close under logMu so HandleLogChunk can never send on a closed channel.
	d.logMu.Lock()
	for _, ch := range d.logSubs[taskID] {
		close(ch)
	}
	delete(d.logSubs, taskID)
	d.logMu.Unlock()

	d.taskLogs.Delete(taskID)
}

// Subscribe registers for live log chunks of a task. The channel is closed when the task reports a
// terminal state. Call unsubscribe (idempotent) when the reader stops earlier so the entry is released.
func (d *Dispatcher) Subscribe(taskID string) (<-chan protocol.TaskLogChunkPayload, func()) {
	ch := make(chan protocol.TaskLogChunkPayload, 50)

	d.logMu.Lock()
	d.nextSubID++
	id := d.nextSubID
	if d.logSubs[taskID] == nil {
		d.logSubs[taskID] = make(map[int]chan protocol.TaskLogChunkPayload)
	}
	d.logSubs[taskID][id] = ch
	d.logMu.Unlock()

	unsubscribe := func() {
		d.logMu.Lock()
		defer d.logMu.Unlock()
		if subs := d.logSubs[taskID]; subs != nil {
			delete(subs, id) // no close here: only finishTask closes, and only channels still registered
			if len(subs) == 0 {
				delete(d.logSubs, taskID)
			}
		}
	}
	return ch, unsubscribe
}
