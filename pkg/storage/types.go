package storage

import (
	"fmt"
	"time"
)

// RunState represents the lifecycle status of a JobRun instance.
type RunState string

const (
	StateWait     RunState = "WAIT"     // Waiting for In-Conditions to be satisfied
	StateReady    RunState = "READY"    // Conditions satisfied, waiting in queue to be claimed
	StateAssigned RunState = "ASSIGNED" // Claimed by a dispatcher, dispatched to Agent
	StateRunning  RunState = "RUNNING"  // Agent reported process has started
	StateSuccess  RunState = "SUCCESS"  // Process finished with Exit Code 0 or Set OK
	StateFailed   RunState = "FAILED"   // Process finished with non-zero exit code or timeout
	StateBypass   RunState = "BYPASS"   // Operator manually bypassed this execution
)

// IsValidStateTransition checks whether transitioning from currentState to nextState is permitted.
func IsValidStateTransition(current, next RunState) bool {
	switch current {
	case StateWait:
		return next == StateReady || next == StateBypass || next == StateSuccess
	case StateReady:
		return next == StateAssigned || next == StateBypass || next == StateSuccess
	case StateAssigned:
		return next == StateRunning || next == StateFailed || next == StateReady || next == StateSuccess || next == StateBypass
	case StateRunning:
		return next == StateSuccess || next == StateFailed || next == StateBypass
	case StateFailed:
		return next == StateSuccess || next == StateBypass // Operator Set OK or Bypass on failed runs
	case StateSuccess, StateBypass:
		return false
	default:
		return false
	}
}

// JobRun represents an individual execution run of a JobDef for a specific date/schedule.
type JobRun struct {
	RunID        string            `json:"run_id"`
	JobDefID     string            `json:"job_def_id"`
	JobName      string            `json:"job_name"`
	State        RunState          `json:"state"`
	AgentID      string            `json:"agent_id,omitempty"`
	Command      string            `json:"command"`
	Args         []string          `json:"args"`
	Env          map[string]string `json:"env"`
	ExitCode     int               `json:"exit_code"`
	ScheduledAt  time.Time         `json:"scheduled_at"`
	StartedAt    *time.Time        `json:"started_at,omitempty"`
	FinishedAt   *time.Time        `json:"finished_at,omitempty"`
	CreatedDate  string            `json:"created_date"` // YYYYMMDD (ODATE)
	ErrorMessage string            `json:"error_message,omitempty"`
}

// JobDef defines the template and schedule configuration for a workload.
type JobDef struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Group         string            `json:"group"`
	CronExpr      string            `json:"cron_expr"`
	Command       string            `json:"command"`
	Args          []string          `json:"args"`
	Env           map[string]string `json:"env"`
	AgentLabels   []string          `json:"agent_labels"`
	InConditions  []string          `json:"in_conditions"`  // AND list of condition names
	OutConditions []string          `json:"out_conditions"` // Generated upon successful completion
	TimeoutSec    int               `json:"timeout_sec"`
	Enabled       bool              `json:"enabled"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
}

// Condition represents an emitted signal for dependency resolution.
type Condition struct {
	Name      string    `json:"name"`
	ODate     string    `json:"odate"` // YYYYMMDD
	CreatedAt time.Time `json:"created_at"`
}

// AuditRecord stores immutable history of manual operator actions.
type AuditRecord struct {
	AuditID    string    `json:"audit_id"`
	OperatorID string    `json:"operator_id"`
	Action     string    `json:"action"`    // "RERUN", "SET_OK", "BYPASS", "HOLD", "RELEASE"
	TargetID   string    `json:"target_id"` // RunID or JobDefID
	Reason     string    `json:"reason"`    // Mandatory reason for compliance
	CreatedAt  time.Time `json:"created_at"`
}

// NewRun builds a run instance of the definition for the given ODATE in the given initial state.
func (d *JobDef) NewRun(odate string, state RunState) *JobRun {
	return newRun(NewID("run-"+d.ID), d.ID, d.Name, d.Command, d.Args, d.Env, odate, state)
}

// NewRerun builds a READY copy of this run (same definition, command and ODATE) with a fresh ID.
func (r *JobRun) NewRerun() *JobRun {
	return newRun(NewID(r.RunID+"-rerun"), r.JobDefID, r.JobName, r.Command, r.Args, r.Env, r.CreatedDate, StateReady)
}

func newRun(runID, defID, jobName, command string, args []string, env map[string]string, odate string, state RunState) *JobRun {
	return &JobRun{
		RunID:       runID,
		JobDefID:    defID,
		JobName:     jobName,
		State:       state,
		Command:     command,
		Args:        args,
		Env:         env,
		ScheduledAt: time.Now(),
		CreatedDate: odate,
	}
}

func (r *JobRun) String() string {
	return fmt.Sprintf("JobRun[id=%s, job=%s, state=%s, odate=%s]", r.RunID, r.JobName, r.State, r.CreatedDate)
}
