package sqlstore

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
)

// SchemaDDL is the table/index DDL, embedded so the binary does not depend on the working directory.
//
//go:embed schema.sql
var SchemaDDL string

// Column lists shared by every SELECT; the order must match scanJobDef / scanJobRun.
const (
	jobDefColumns = `JOB_DEF_ID, JOB_NAME, JOB_GROUP, CRON_EXPR, COMMAND, ARGS_JSON, ENV_JSON,
		AGENT_LABELS, IN_CONDS, OUT_CONDS, TIMEOUT_SEC, ENABLED, CREATED_AT, UPDATED_AT`
	jobRunColumns = `RUN_ID, JOB_DEF_ID, JOB_NAME, STATE, AGENT_ID, COMMAND, ARGS_JSON, ENV_JSON,
		EXIT_CODE, SCHEDULED_AT, STARTED_AT, FINISHED_AT, CREATED_DATE, ERROR_MSG`
)

// isUniqueViolation reports whether err is a primary-key / unique-constraint failure.
func isUniqueViolation(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique") || strings.Contains(msg, "primary")
}

// SQLStore implements storage.Store backed by any ANSI-SQL compliant RDBMS via *sql.DB (ODBC / native driver).
type SQLStore struct {
	db        *sql.DB
	jobDef    *SQLJobDefStore
	jobRun    *SQLJobRunStore
	condition *SQLConditionStore
	audit     *SQLAuditStore
}

func NewSQLStore(db *sql.DB) *SQLStore {
	return &SQLStore{
		db:        db,
		jobDef:    &SQLJobDefStore{db: db},
		jobRun:    &SQLJobRunStore{db: db},
		condition: &SQLConditionStore{db: db},
		audit:     &SQLAuditStore{db: db},
	}
}

func (s *SQLStore) JobDef() storage.JobDefStore       { return s.jobDef }
func (s *SQLStore) JobRun() storage.JobRunStore       { return s.jobRun }
func (s *SQLStore) Condition() storage.ConditionStore { return s.condition }
func (s *SQLStore) Audit() storage.AuditStore         { return s.audit }

// InitializeSchema executes schema DDL to create tables if they do not exist.
func (s *SQLStore) InitializeSchema(ctx context.Context, ddl string) error {
	queries := strings.Split(ddl, ";")
	for _, q := range queries {
		trimmed := strings.TrimSpace(q)
		if trimmed == "" {
			continue
		}
		if _, err := s.db.ExecContext(ctx, trimmed); err != nil {
			// Ignore table already exists errors for idempotency
			if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
				return fmt.Errorf("failed executing schema ddl: %w, query: %s", err, trimmed)
			}
		}
	}
	return nil
}

// ----------------------------------------------------------------------------
// SQLJobDefStore
// ----------------------------------------------------------------------------
type SQLJobDefStore struct {
	db *sql.DB
}

// jobDefFields holds the serialized form of the JobDef columns that are not plain scalars.
type jobDefFields struct {
	args, env, labels, inConds, outConds string
	enabled                              int
}

func encodeJobDef(def *storage.JobDef) jobDefFields {
	argsJSON, _ := json.Marshal(def.Args)
	envJSON, _ := json.Marshal(def.Env)
	inConds, _ := json.Marshal(def.InConditions)
	outConds, _ := json.Marshal(def.OutConditions)
	f := jobDefFields{
		args:     string(argsJSON),
		env:      string(envJSON),
		labels:   strings.Join(def.AgentLabels, ","),
		inConds:  string(inConds),
		outConds: string(outConds),
	}
	if def.Enabled {
		f.enabled = 1
	}
	return f
}

func (s *SQLJobDefStore) Create(ctx context.Context, def *storage.JobDef) error {
	f := encodeJobDef(def)

	query := `INSERT INTO FJS_JOB_DEF (
		JOB_DEF_ID, JOB_NAME, JOB_GROUP, CRON_EXPR, COMMAND, ARGS_JSON, ENV_JSON,
		AGENT_LABELS, IN_CONDS, OUT_CONDS, TIMEOUT_SEC, ENABLED, CREATED_AT, UPDATED_AT
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	now := time.Now()
	_, err := s.db.ExecContext(ctx, query,
		def.ID, def.Name, def.Group, def.CronExpr, def.Command, f.args, f.env,
		f.labels, f.inConds, f.outConds, def.TimeoutSec, f.enabled, now, now,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return storage.ErrAlreadyExists
		}
		return err
	}
	return nil
}

func (s *SQLJobDefStore) GetByID(ctx context.Context, id string) (*storage.JobDef, error) {
	query := `SELECT ` + jobDefColumns + `
		FROM FJS_JOB_DEF WHERE JOB_DEF_ID = ?`

	row := s.db.QueryRowContext(ctx, query, id)
	return scanJobDef(row)
}

func (s *SQLJobDefStore) GetByName(ctx context.Context, name string) (*storage.JobDef, error) {
	query := `SELECT ` + jobDefColumns + `
		FROM FJS_JOB_DEF WHERE JOB_NAME = ?`

	row := s.db.QueryRowContext(ctx, query, name)
	return scanJobDef(row)
}

func (s *SQLJobDefStore) List(ctx context.Context, group string) ([]*storage.JobDef, error) {
	query := `SELECT ` + jobDefColumns + ` FROM FJS_JOB_DEF`
	var args []any
	if group != "" {
		query += ` WHERE JOB_GROUP = ?`
		args = append(args, group)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*storage.JobDef
	for rows.Next() {
		def, err := scanJobDef(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, def)
	}
	return result, nil
}

func (s *SQLJobDefStore) Update(ctx context.Context, def *storage.JobDef) error {
	f := encodeJobDef(def)

	query := `UPDATE FJS_JOB_DEF SET
		JOB_NAME = ?, JOB_GROUP = ?, CRON_EXPR = ?, COMMAND = ?, ARGS_JSON = ?, ENV_JSON = ?,
		AGENT_LABELS = ?, IN_CONDS = ?, OUT_CONDS = ?, TIMEOUT_SEC = ?, ENABLED = ?, UPDATED_AT = ?
		WHERE JOB_DEF_ID = ?`

	res, err := s.db.ExecContext(ctx, query,
		def.Name, def.Group, def.CronExpr, def.Command, f.args, f.env,
		f.labels, f.inConds, f.outConds, def.TimeoutSec, f.enabled, time.Now(), def.ID,
	)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return storage.ErrNotFound
	}
	return nil
}

func (s *SQLJobDefStore) Delete(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM FJS_JOB_DEF WHERE JOB_DEF_ID = ?`, id)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return storage.ErrNotFound
	}
	return nil
}

// ----------------------------------------------------------------------------
// SQLJobRunStore (Atomic CAS Task Claiming)
// ----------------------------------------------------------------------------
type SQLJobRunStore struct {
	db *sql.DB
}

func (s *SQLJobRunStore) Create(ctx context.Context, run *storage.JobRun) error {
	argsJSON, _ := json.Marshal(run.Args)
	envJSON, _ := json.Marshal(run.Env)

	query := `INSERT INTO FJS_JOB_RUN (
		RUN_ID, JOB_DEF_ID, JOB_NAME, STATE, AGENT_ID, COMMAND, ARGS_JSON, ENV_JSON,
		EXIT_CODE, SCHEDULED_AT, STARTED_AT, FINISHED_AT, CREATED_DATE, ERROR_MSG
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.ExecContext(ctx, query,
		run.RunID, run.JobDefID, run.JobName, string(run.State), run.AgentID, run.Command,
		string(argsJSON), string(envJSON), run.ExitCode, run.ScheduledAt, run.StartedAt, run.FinishedAt,
		run.CreatedDate, run.ErrorMessage,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return storage.ErrAlreadyExists
		}
		return err
	}
	return nil
}

func (s *SQLJobRunStore) GetByID(ctx context.Context, runID string) (*storage.JobRun, error) {
	query := `SELECT ` + jobRunColumns + `
		FROM FJS_JOB_RUN WHERE RUN_ID = ?`

	row := s.db.QueryRowContext(ctx, query, runID)
	return scanJobRun(row)
}

func (s *SQLJobRunStore) ListByState(ctx context.Context, states []storage.RunState) ([]*storage.JobRun, error) {
	if len(states) == 0 {
		rows, err := s.db.QueryContext(ctx, `SELECT `+jobRunColumns+` FROM FJS_JOB_RUN`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanJobRuns(rows)
	}

	placeholders := make([]string, len(states))
	args := make([]any, len(states))
	for i, st := range states {
		placeholders[i] = "?"
		args[i] = string(st)
	}

	query := fmt.Sprintf(`SELECT `+jobRunColumns+`
		FROM FJS_JOB_RUN WHERE STATE IN (%s)`, strings.Join(placeholders, ","))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobRuns(rows)
}

func (s *SQLJobRunStore) ListByDate(ctx context.Context, odate string) ([]*storage.JobRun, error) {
	query := `SELECT ` + jobRunColumns + `
		FROM FJS_JOB_RUN WHERE CREATED_DATE = ? ORDER BY SCHEDULED_AT DESC, RUN_ID DESC`

	rows, err := s.db.QueryContext(ctx, query, odate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobRuns(rows)
}

// ListReady returns up to limit READY runs ordered oldest-first.
func (s *SQLJobRunStore) ListReady(ctx context.Context, limit int) ([]*storage.JobRun, error) {
	if limit <= 0 {
		return nil, nil
	}
	query := `SELECT ` + jobRunColumns + `
		FROM FJS_JOB_RUN WHERE STATE = 'READY' ORDER BY SCHEDULED_AT ASC, RUN_ID ASC LIMIT ?`

	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobRuns(rows)
}

func (s *SQLJobRunStore) TransitionState(ctx context.Context, runID string, expectedCurrent storage.RunState, next storage.RunState, exitCode int, errMsg string) error {
	run, err := s.GetByID(ctx, runID)
	if err != nil {
		return err
	}

	if run.State != expectedCurrent {
		return fmt.Errorf("%w: current is %s, expected %s", storage.ErrInvalidStateTransition, run.State, expectedCurrent)
	}
	if !storage.IsValidStateTransition(run.State, next) {
		return fmt.Errorf("%w: cannot transition from %s to %s", storage.ErrInvalidStateTransition, run.State, next)
	}

	now := time.Now()
	var startedAt *time.Time = run.StartedAt
	var finishedAt *time.Time = run.FinishedAt

	if next == storage.StateRunning && startedAt == nil {
		startedAt = &now
	}
	if next == storage.StateSuccess || next == storage.StateFailed || next == storage.StateBypass {
		finishedAt = &now
	}

	updateQuery := `UPDATE FJS_JOB_RUN SET
		STATE = ?, EXIT_CODE = ?, STARTED_AT = ?, FINISHED_AT = ?, ERROR_MSG = ?
		WHERE RUN_ID = ? AND STATE = ?`

	res, err := s.db.ExecContext(ctx, updateQuery,
		string(next), exitCode, startedAt, finishedAt, errMsg, runID, string(expectedCurrent),
	)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return storage.ErrInvalidStateTransition
	}
	return nil
}

func (s *SQLJobRunStore) SetAgentID(ctx context.Context, runID string, agentID string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE FJS_JOB_RUN SET AGENT_ID = ? WHERE RUN_ID = ?", agentID, runID)
	return err
}

// ----------------------------------------------------------------------------
// SQLConditionStore
// ----------------------------------------------------------------------------
type SQLConditionStore struct {
	db *sql.DB
}

func (s *SQLConditionStore) Add(ctx context.Context, condName string, odate string) error {
	query := `INSERT INTO FJS_CONDITION (COND_NAME, ODATE, CREATED_AT) VALUES (?, ?, ?)`
	_, err := s.db.ExecContext(ctx, query, condName, odate, time.Now())
	if err != nil {
		// Ignore duplicate condition insertion
		if isUniqueViolation(err) {
			return nil
		}
		return err
	}
	return nil
}

func (s *SQLConditionStore) Exists(ctx context.Context, condName string, odate string) (bool, error) {
	query := `SELECT COUNT(1) FROM FJS_CONDITION WHERE COND_NAME = ? AND ODATE = ?`
	var count int
	err := s.db.QueryRowContext(ctx, query, condName, odate).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *SQLConditionStore) Delete(ctx context.Context, condName string, odate string) error {
	query := `DELETE FROM FJS_CONDITION WHERE COND_NAME = ? AND ODATE = ?`
	res, err := s.db.ExecContext(ctx, query, condName, odate)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return storage.ErrNotFound
	}
	return nil
}

func (s *SQLConditionStore) ListByDate(ctx context.Context, odate string) ([]*storage.Condition, error) {
	query := `SELECT COND_NAME, ODATE, CREATED_AT FROM FJS_CONDITION WHERE ODATE = ?`
	rows, err := s.db.QueryContext(ctx, query, odate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*storage.Condition
	for rows.Next() {
		var c storage.Condition
		if err := rows.Scan(&c.Name, &c.ODate, &c.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, &c)
	}
	return result, nil
}

// ----------------------------------------------------------------------------
// SQLAuditStore
// ----------------------------------------------------------------------------
type SQLAuditStore struct {
	db *sql.DB
}

func (s *SQLAuditStore) Record(ctx context.Context, operatorID, action, targetID, reason string) error {
	if operatorID == "" || action == "" || targetID == "" || reason == "" {
		return fmt.Errorf("%w: operator_id, action, target_id, and reason are required", storage.ErrInvalidInput)
	}

	auditID := storage.NewID("audit")
	query := `INSERT INTO FJS_AUDIT (AUDIT_ID, OPERATOR_ID, ACTION, TARGET_ID, REASON, CREATED_AT)
		VALUES (?, ?, ?, ?, ?, ?)`

	_, err := s.db.ExecContext(ctx, query, auditID, operatorID, action, targetID, reason, time.Now())
	return err
}

func (s *SQLAuditStore) ListByTarget(ctx context.Context, targetID string) ([]*storage.AuditRecord, error) {
	return s.query(ctx, `SELECT AUDIT_ID, OPERATOR_ID, ACTION, TARGET_ID, REASON, CREATED_AT
		FROM FJS_AUDIT WHERE TARGET_ID = ? ORDER BY CREATED_AT DESC`, targetID)
}

func (s *SQLAuditStore) ListRecent(ctx context.Context, limit int) ([]*storage.AuditRecord, error) {
	return s.query(ctx, `SELECT AUDIT_ID, OPERATOR_ID, ACTION, TARGET_ID, REASON, CREATED_AT
		FROM FJS_AUDIT ORDER BY CREATED_AT DESC, AUDIT_ID DESC LIMIT ?`, limit)
}

func (s *SQLAuditStore) query(ctx context.Context, query string, args ...any) ([]*storage.AuditRecord, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*storage.AuditRecord
	for rows.Next() {
		var rec storage.AuditRecord
		if err := rows.Scan(&rec.AuditID, &rec.OperatorID, &rec.Action, &rec.TargetID, &rec.Reason, &rec.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, &rec)
	}
	return result, nil
}

// ----------------------------------------------------------------------------
// Helper Scanners
// ----------------------------------------------------------------------------
type scannable interface {
	Scan(dest ...any) error
}

func scanJobDef(s scannable) (*storage.JobDef, error) {
	var def storage.JobDef
	var argsJSON, envJSON, labelsStr, inConds, outConds string
	var enabledInt int

	err := s.Scan(
		&def.ID, &def.Name, &def.Group, &def.CronExpr, &def.Command,
		&argsJSON, &envJSON, &labelsStr, &inConds, &outConds,
		&def.TimeoutSec, &enabledInt, &def.CreatedAt, &def.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}

	_ = json.Unmarshal([]byte(argsJSON), &def.Args)
	_ = json.Unmarshal([]byte(envJSON), &def.Env)
	_ = json.Unmarshal([]byte(inConds), &def.InConditions)
	_ = json.Unmarshal([]byte(outConds), &def.OutConditions)
	if labelsStr != "" {
		def.AgentLabels = strings.Split(labelsStr, ",")
	}
	def.Enabled = enabledInt == 1
	return &def, nil
}

func scanJobRun(s scannable) (*storage.JobRun, error) {
	var run storage.JobRun
	var stateStr, argsJSON, envJSON, errorMsg sql.NullString
	var startedAt, finishedAt sql.NullTime
	var agentID sql.NullString

	err := s.Scan(
		&run.RunID, &run.JobDefID, &run.JobName, &stateStr, &agentID, &run.Command,
		&argsJSON, &envJSON, &run.ExitCode, &run.ScheduledAt, &startedAt, &finishedAt,
		&run.CreatedDate, &errorMsg,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}

	run.State = storage.RunState(stateStr.String)
	if agentID.Valid {
		run.AgentID = agentID.String
	}
	if errorMsg.Valid {
		run.ErrorMessage = errorMsg.String
	}
	if startedAt.Valid {
		run.StartedAt = &startedAt.Time
	}
	if finishedAt.Valid {
		run.FinishedAt = &finishedAt.Time
	}
	if argsJSON.Valid {
		_ = json.Unmarshal([]byte(argsJSON.String), &run.Args)
	}
	if envJSON.Valid {
		_ = json.Unmarshal([]byte(envJSON.String), &run.Env)
	}
	return &run, nil
}

func scanJobRuns(rows *sql.Rows) ([]*storage.JobRun, error) {
	var result []*storage.JobRun
	for rows.Next() {
		run, err := scanJobRun(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, run)
	}
	return result, nil
}
