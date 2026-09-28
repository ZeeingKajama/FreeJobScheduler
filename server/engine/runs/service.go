// Package runs owns the run lifecycle: ordering runs, releasing them when their In-Conditions hold,
// operator actions (Rerun / Set OK / Bypass) and applying status reported by agents.
// Every change to run state or condition state goes through Service, so the database and the in-memory
// condition index cannot drift apart. The dispatcher only assigns READY runs to agents.
package runs

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
	"github.com/ZeeingKajama/FreeJobScheduler/server/engine/condition"
)

// Service coordinates run state transitions and condition state (DB + write-through in-memory index).
type Service struct {
	store storage.Store
	conds *condition.SubscriptionIndex

	dispatchable chan struct{}
}

func NewService(store storage.Store) *Service {
	return &Service{
		store:        store,
		conds:        condition.NewSubscriptionIndex(),
		dispatchable: make(chan struct{}, 1),
	}
}

// Dispatchable receives a (coalesced) signal whenever a run became READY, so the dispatcher can assign it
// without waiting for its fallback poll.
func (s *Service) Dispatchable() <-chan struct{} {
	return s.dispatchable
}

func (s *Service) signal() {
	select {
	case s.dispatchable <- struct{}{}:
	default: // a wake is already pending
	}
}

// WaitingCount returns how many WAIT runs are currently tracked by the condition index.
func (s *Service) WaitingCount() int {
	return s.conds.WaitingCount()
}

// PruneConditionCache drops cached conditions of ODates older than odate and returns how many dates were
// dropped. Only the cache is affected; conditions stay in the database and are re-read on demand.
func (s *Service) PruneConditionCache(odate string) int {
	return s.conds.ForgetBefore(odate)
}

// Bootstrap hydrates the in-memory index with the existing WAIT runs after a server restart.
// Runs whose conditions were already satisfied before the restart go straight to READY.
func (s *Service) Bootstrap(ctx context.Context) error {
	waitRuns, err := s.store.JobRun().ListByState(ctx, []storage.RunState{storage.StateWait})
	if err != nil {
		return err
	}

	for _, run := range waitRuns {
		def, err := s.store.JobDef().GetByID(ctx, run.JobDefID)
		if err != nil || def == nil {
			continue
		}

		wt := &condition.WaitingTask{
			RunID:        run.RunID,
			JobDefID:     run.JobDefID,
			ODate:        run.CreatedDate,
			InConditions: def.InConditions,
		}
		if s.allConditionsMet(ctx, wt) {
			s.release(ctx, wt, "")
			continue
		}
		s.conds.RegisterWaitTask(wt)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Ordering
// ---------------------------------------------------------------------------

// OrderRun instantiates def for odate: the run is created in WAIT, then released to READY at once
// if its In-Conditions are already satisfied, otherwise left waiting in the condition index.
// The returned run reflects the resulting state.
func (s *Service) OrderRun(ctx context.Context, def *storage.JobDef, odate string) (*storage.JobRun, error) {
	run := def.NewRun(odate, storage.StateWait)
	if err := s.store.JobRun().Create(ctx, run); err != nil {
		return nil, err
	}

	s.RegisterWaitingTask(ctx, run.RunID, def.ID, odate, def.InConditions)

	if current, err := s.store.JobRun().GetByID(ctx, run.RunID); err == nil && current != nil {
		return current, nil
	}
	return run, nil
}

// RegisterWaitingTask tracks a run that was created in WAIT state, releasing it immediately
// if it has no conditions or they are all satisfied.
func (s *Service) RegisterWaitingTask(ctx context.Context, runID, jobDefID, odate string, inConditions []string) {
	wt := &condition.WaitingTask{
		RunID:        runID,
		JobDefID:     jobDefID,
		ODate:        odate,
		InConditions: inConditions,
	}

	if s.allConditionsMet(ctx, wt) {
		s.release(ctx, wt, "")
		return
	}
	s.conds.RegisterWaitTask(wt)
}

// PlanResult summarizes an OrderPlan call.
type PlanResult struct {
	ODate   string
	Ordered int
	Ready   int
	Wait    int
	Skipped int
}

// OrderPlan orders every enabled job definition of group ("" = all) that has no run for odate yet,
// then re-evaluates the WAIT runs that already existed. The operation is recorded in the audit trail.
func (s *Service) OrderPlan(ctx context.Context, odate, group, operatorID string) (*PlanResult, error) {
	defs, err := s.store.JobDef().List(ctx, group)
	if err != nil {
		return nil, err
	}
	existingRuns, err := s.store.JobRun().ListByDate(ctx, odate)
	if err != nil {
		return nil, err
	}

	existingDefIDs := make(map[string]bool)
	for _, r := range existingRuns {
		existingDefIDs[r.JobDefID] = true
	}

	res := &PlanResult{ODate: odate}

	// 1. Order uninstantiated job definitions
	for _, def := range defs {
		if !def.Enabled {
			continue
		}
		if existingDefIDs[def.ID] {
			res.Skipped++
			continue
		}

		run, err := s.OrderRun(ctx, def, odate)
		if err != nil {
			continue
		}
		res.Ordered++
		if run.State == storage.StateReady {
			res.Ready++
		} else {
			res.Wait++
		}
	}

	// 2. Evaluate any existing WAIT runs in case their conditions are already satisfied
	for _, r := range existingRuns {
		if r.State != storage.StateWait {
			continue
		}
		def, _ := s.store.JobDef().GetByID(ctx, r.JobDefID)
		if def == nil || len(def.InConditions) == 0 {
			continue
		}
		wt := &condition.WaitingTask{RunID: r.RunID, JobDefID: r.JobDefID, ODate: odate, InConditions: def.InConditions}
		if s.allConditionsMet(ctx, wt) && s.release(ctx, wt, "Conditions met during plan ordering") {
			res.Ready++
		}
	}

	_ = s.store.Audit().Record(ctx, operatorID, "ORDER_PLAN", odate,
		fmt.Sprintf("Ordered daily plan for %s: %d total (%d ready, %d waiting, %d already existed)",
			odate, res.Ordered, res.Ready, res.Wait, res.Skipped))
	return res, nil
}

// Trigger orders the job definition defID for odate and, if it has Out-Conditions, also orders its direct
// downstream jobs (those with a matching In-Condition, or the job name as In-Condition) that have no run
// for odate yet, so they wait in WAIT until this run completes. A missing definition yields storage.ErrNotFound.
func (s *Service) Trigger(ctx context.Context, defID, odate string) (*storage.JobRun, error) {
	def, err := s.store.JobDef().GetByID(ctx, defID)
	if err != nil {
		return nil, err
	}

	run, err := s.OrderRun(ctx, def, odate)
	if err != nil {
		return nil, err
	}

	if len(def.OutConditions) == 0 {
		return run, nil
	}

	allDefs, _ := s.store.JobDef().List(ctx, "")
	existingRuns, _ := s.store.JobRun().ListByDate(ctx, odate)
	existingDefIDs := make(map[string]bool)
	for _, er := range existingRuns {
		existingDefIDs[er.JobDefID] = true
	}
	existingDefIDs[def.ID] = true

	outCondSet := make(map[string]bool)
	for _, oc := range def.OutConditions {
		outCondSet[strings.TrimSpace(oc)] = true
	}

	for _, dDef := range allDefs {
		if !dDef.Enabled || existingDefIDs[dDef.ID] {
			continue
		}
		for _, inC := range dDef.InConditions {
			inC = strings.TrimSpace(inC)
			if outCondSet[inC] || inC == def.Name {
				if _, err := s.OrderRun(ctx, dDef, odate); err == nil {
					existingDefIDs[dDef.ID] = true
				}
				break
			}
		}
	}
	return run, nil
}

// ---------------------------------------------------------------------------
// Conditions
// ---------------------------------------------------------------------------

// EmitCondition records a condition in the database and the index, then releases the WAIT runs it completes.
// The index is only touched once the database write succeeded.
func (s *Service) EmitCondition(ctx context.Context, condName, odate string) error {
	if err := s.store.Condition().Add(ctx, condName, odate); err != nil {
		return err
	}

	candidates := s.conds.RecordCondition(condName, odate)
	for _, task := range candidates {
		if s.allConditionsMet(ctx, task) {
			s.release(ctx, task, "")
		}
	}
	return nil
}

// DeleteCondition removes a condition from the database and the index. Runs it already released stay
// released; runs ordered afterwards see the condition as missing. The cache entry is dropped even if the
// database reports the condition as absent, so the cache never claims more than the database holds.
func (s *Service) DeleteCondition(ctx context.Context, condName, odate string) error {
	err := s.store.Condition().Delete(ctx, condName, odate)
	s.conds.ForgetCondition(condName, odate)
	return err
}

// allConditionsMet reports whether every In-Condition of task holds. The in-memory cache answers first;
// anything it does not know is checked in the DB (the cache is empty after a restart) and cached when found.
func (s *Service) allConditionsMet(ctx context.Context, task *condition.WaitingTask) bool {
	for _, cond := range task.InConditions {
		cond = strings.TrimSpace(cond)
		if cond == "" || s.conds.HasCondition(cond, task.ODate) {
			continue
		}
		if exists, err := s.store.Condition().Exists(ctx, cond, task.ODate); err != nil || !exists {
			return false
		}
		s.conds.SeedCondition(cond, task.ODate)
	}
	return true
}

// release moves a WAIT run to READY, stops tracking it and wakes the dispatcher.
// It reports whether the transition applied (it does not if the run left WAIT in the meantime).
func (s *Service) release(ctx context.Context, task *condition.WaitingTask, reason string) bool {
	if err := s.store.JobRun().TransitionState(ctx, task.RunID, storage.StateWait, storage.StateReady, 0, reason); err != nil {
		return false
	}
	s.conds.UnregisterWaitTask(task.RunID, task.ODate)
	s.signal()
	return true
}

// emitOutConditions publishes the Out-Conditions of a run that reached a finished state.
func (s *Service) emitOutConditions(ctx context.Context, run *storage.JobRun) {
	def, err := s.store.JobDef().GetByID(ctx, run.JobDefID)
	if err != nil || def == nil {
		return
	}
	for _, cond := range def.OutConditions {
		if err := s.EmitCondition(ctx, cond, run.CreatedDate); err != nil {
			log.Printf("[Runs] failed to emit condition %s (%s) of run %s: %v", cond, run.CreatedDate, run.RunID, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Operator actions
// ---------------------------------------------------------------------------

// Rerun creates a new run based on an existing one and queues it in READY state, regardless of conditions.
func (s *Service) Rerun(ctx context.Context, originalRunID, operatorID, reason string) (*storage.JobRun, error) {
	if operatorID == "" || reason == "" {
		return nil, fmt.Errorf("%w: operatorID and reason are mandatory for rerun", storage.ErrInvalidInput)
	}

	orig, err := s.store.JobRun().GetByID(ctx, originalRunID)
	if err != nil {
		return nil, err
	}

	newRun := orig.NewRerun()
	if err := s.store.JobRun().Create(ctx, newRun); err != nil {
		return nil, fmt.Errorf("failed to create rerun instance: %w", err)
	}

	_ = s.store.Audit().Record(ctx, operatorID, "RERUN", originalRunID, reason)
	s.signal()
	return newRun, nil
}

// SetOK forces a run into SUCCESS, publishes its Out-Conditions to release downstream runs and audits it.
func (s *Service) SetOK(ctx context.Context, runID, operatorID, reason string) error {
	if operatorID == "" || reason == "" {
		return fmt.Errorf("%w: operatorID and reason are mandatory for SetOK", storage.ErrInvalidInput)
	}

	run, err := s.store.JobRun().GetByID(ctx, runID)
	if err != nil {
		return err
	}

	if err := s.store.JobRun().TransitionState(ctx, runID, run.State, storage.StateSuccess, 0, "Manually Set OK by operator: "+reason); err != nil {
		return fmt.Errorf("failed to transition to SUCCESS: %w", err)
	}

	s.emitOutConditions(ctx, run)

	_ = s.store.Audit().Record(ctx, operatorID, "SET_OK", runID, reason)
	return nil
}

// Bypass skips a run's execution and marks it BYPASS. With releaseDownstream its Out-Conditions are published.
func (s *Service) Bypass(ctx context.Context, runID, operatorID, reason string, releaseDownstream bool) error {
	if operatorID == "" || reason == "" {
		return fmt.Errorf("%w: operatorID and reason are mandatory for Bypass", storage.ErrInvalidInput)
	}

	run, err := s.store.JobRun().GetByID(ctx, runID)
	if err != nil {
		return err
	}

	if err := s.store.JobRun().TransitionState(ctx, runID, run.State, storage.StateBypass, 0, "Bypassed by operator: "+reason); err != nil {
		return fmt.Errorf("failed to transition to BYPASS: %w", err)
	}

	if releaseDownstream {
		s.emitOutConditions(ctx, run)
	}

	_ = s.store.Audit().Record(ctx, operatorID, "BYPASS", runID, reason)
	return nil
}

// ---------------------------------------------------------------------------
// Agent reports
// ---------------------------------------------------------------------------

// OnAgentStatus applies a status update reported by an agent to its run. Out-Conditions are published
// only when this report is what completed the run (an operator may have forced it to a finished state
// already). It returns storage.ErrNotFound if the run is unknown.
func (s *Service) OnAgentStatus(ctx context.Context, agentID string, status protocol.TaskStatusUpdatePayload) error {
	run, err := s.store.JobRun().GetByID(ctx, status.TaskID)
	if err != nil {
		return err
	}
	if run == nil {
		return storage.ErrNotFound
	}

	if agentID != "" {
		_ = s.store.JobRun().SetAgentID(ctx, status.TaskID, agentID)
	}

	switch status.State {
	case protocol.TaskRunning:
		_ = s.store.JobRun().TransitionState(ctx, status.TaskID, run.State, storage.StateRunning, 0, "")

	case protocol.TaskSuccess:
		if err := s.store.JobRun().TransitionState(ctx, status.TaskID, run.State, storage.StateSuccess, status.ExitCode, ""); err == nil {
			s.emitOutConditions(ctx, run)
		}

	case protocol.TaskFailed:
		_ = s.store.JobRun().TransitionState(ctx, status.TaskID, run.State, storage.StateFailed, status.ExitCode, status.ErrorMsg)
	}
	return nil
}
