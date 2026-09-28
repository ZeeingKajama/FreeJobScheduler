package storage

import "context"

// Store is the root data access abstraction, decoupling the scheduler and agents from any specific database engine.
type Store interface {
	JobDef() JobDefStore
	JobRun() JobRunStore
	Condition() ConditionStore
	Audit() AuditStore
}

// JobDefStore manages workflow definitions and templates.
type JobDefStore interface {
	Create(ctx context.Context, def *JobDef) error
	GetByID(ctx context.Context, id string) (*JobDef, error)
	GetByName(ctx context.Context, name string) (*JobDef, error)
	List(ctx context.Context, group string) ([]*JobDef, error)
	Update(ctx context.Context, def *JobDef) error
	Delete(ctx context.Context, id string) error
}

// JobRunStore manages runtime execution instances and atomic state transitions.
type JobRunStore interface {
	Create(ctx context.Context, run *JobRun) error
	GetByID(ctx context.Context, runID string) (*JobRun, error)
	ListByState(ctx context.Context, states []RunState) ([]*JobRun, error)
	ListByDate(ctx context.Context, odate string) ([]*JobRun, error)

	// ListReady returns up to limit READY runs ordered oldest-first (FIFO by ScheduledAt).
	// Claiming is done by the caller via TransitionState(READY -> ASSIGNED), which is an atomic CAS.
	ListReady(ctx context.Context, limit int) ([]*JobRun, error)

	// TransitionState moves runID from expectedCurrent to next, enforcing valid state machine rules.
	TransitionState(ctx context.Context, runID string, expectedCurrent RunState, next RunState, exitCode int, errMsg string) error

	// SetAgentID binds an agent ID to the run instance.
	SetAgentID(ctx context.Context, runID string, agentID string) error
}

// ConditionStore manages dependency signals for DAG evaluation.
type ConditionStore interface {
	Add(ctx context.Context, condName string, odate string) error
	Exists(ctx context.Context, condName string, odate string) (bool, error)
	Delete(ctx context.Context, condName string, odate string) error
	ListByDate(ctx context.Context, odate string) ([]*Condition, error)
}

// AuditStore provides immutable record keeping for manual operator interventions.
type AuditStore interface {
	Record(ctx context.Context, operatorID, action, targetID, reason string) error
	ListByTarget(ctx context.Context, targetID string) ([]*AuditRecord, error)
	// ListRecent returns up to limit records of any target, newest first.
	ListRecent(ctx context.Context, limit int) ([]*AuditRecord, error)
}
