package webapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
	"github.com/ZeeingKajama/FreeJobScheduler/server/agenthub"
	"github.com/ZeeingKajama/FreeJobScheduler/server/dispatcher"
	"github.com/ZeeingKajama/FreeJobScheduler/server/engine/runs"
	"github.com/gorilla/websocket"
)

// APIHandler serves the web console API. Handlers only parse the request, call runs.Service (or read
// from the store) and write the response; run lifecycle rules live in the service.
type APIHandler struct {
	store    storage.Store
	hub      *agenthub.Hub
	disp     *dispatcher.Dispatcher
	runs     *runs.Service
	upgrader websocket.Upgrader
}

func NewAPIHandler(store storage.Store, hub *agenthub.Hub, disp *dispatcher.Dispatcher, svc *runs.Service) *APIHandler {
	return &APIHandler{
		store: store,
		hub:   hub,
		disp:  disp,
		runs:  svc,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (h *APIHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/jobs", h.listJobs)
	mux.HandleFunc("POST /api/v1/jobs", h.createJob)
	mux.HandleFunc("PUT /api/v1/jobs", h.updateJob)
	mux.HandleFunc("DELETE /api/v1/jobs", h.deleteJob)
	mux.HandleFunc("GET /api/v1/jobs/detail", h.getJob)
	mux.HandleFunc("POST /api/v1/jobs/trigger", h.triggerJob)
	mux.HandleFunc("GET /api/v1/runs", h.listRuns)
	mux.HandleFunc("GET /api/v1/runs/logs", h.getRunLogs)
	mux.HandleFunc("POST /api/v1/runs/action", h.runAction)
	mux.HandleFunc("POST /api/v1/runs/order", h.orderPlan)
	mux.HandleFunc("GET /api/v1/conditions", h.listConditions)
	mux.HandleFunc("POST /api/v1/conditions", h.addCondition)
	mux.HandleFunc("DELETE /api/v1/conditions", h.deleteCondition)
	mux.HandleFunc("GET /api/v1/audits", h.listAudits)
	mux.HandleFunc("GET /api/v1/agents", h.listAgents)
	mux.HandleFunc("GET /ws/logs", h.handleLogStream)
}

// writeJSON writes v as the JSON response body with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes err as a plain-text error response (the console reads error bodies as text).
func writeError(w http.ResponseWriter, status int, err error) {
	http.Error(w, err.Error(), status)
}

// odateOrToday returns odate, or today's date (YYYYMMDD) when it is empty.
func odateOrToday(odate string) string {
	if odate == "" {
		return time.Now().Format("20060102")
	}
	return odate
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

func (h *APIHandler) listJobs(w http.ResponseWriter, r *http.Request) {
	list, err := h.store.JobDef().List(r.Context(), r.URL.Query().Get("group"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (h *APIHandler) createJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req struct {
		storage.JobDef
		RunNow bool   `json:"run_now"`
		ODate  string `json:"odate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	def := req.JobDef
	if def.ID == "" {
		def.ID = storage.NewID("def")
	}
	if def.Group == "" {
		def.Group = "DEFAULT"
	}
	def.Enabled = true

	if err := h.store.JobDef().Create(ctx, &def); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	if req.RunNow {
		_, _ = h.runs.OrderRun(ctx, &def, odateOrToday(req.ODate))
	}

	writeJSON(w, http.StatusCreated, def)
}

func (h *APIHandler) updateJob(w http.ResponseWriter, r *http.Request) {
	var def storage.JobDef
	if err := json.NewDecoder(r.Body).Decode(&def); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if def.ID == "" {
		writeError(w, http.StatusBadRequest, errors.New("job id is required for update"))
		return
	}
	def.UpdatedAt = time.Now()
	if err := h.store.JobDef().Update(r.Context(), &def); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, def)
}

func (h *APIHandler) deleteJob(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, errors.New("id is required"))
		return
	}
	if err := h.store.JobDef().Delete(r.Context(), id); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "deleted_id": id})
}

func (h *APIHandler) getJob(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, errors.New("id is required"))
		return
	}

	def, err := h.store.JobDef().GetByID(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, def)
}

func (h *APIHandler) triggerJob(w http.ResponseWriter, r *http.Request) {
	var req struct {
		JobID string `json:"job_id"`
		ODate string `json:"odate"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.JobID == "" {
		req.JobID = r.URL.Query().Get("id")
	}
	if req.JobID == "" {
		writeError(w, http.StatusBadRequest, errors.New("job_id is required"))
		return
	}

	run, err := h.runs.Trigger(r.Context(), req.JobID, odateOrToday(req.ODate))
	switch {
	case errors.Is(err, storage.ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	case err != nil:
		writeError(w, http.StatusInternalServerError, err)
	default:
		writeJSON(w, http.StatusCreated, run)
	}
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

func (h *APIHandler) orderPlan(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ODate      string `json:"odate"`
		Group      string `json:"group"`
		OperatorID string `json:"operator_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.OperatorID == "" {
		req.OperatorID = "operator"
	}

	res, err := h.runs.OrderPlan(r.Context(), odateOrToday(req.ODate), req.Group, req.OperatorID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":       true,
		"odate":         res.ODate,
		"ordered_count": res.Ordered,
		"ready_count":   res.Ready,
		"wait_count":    res.Wait,
		"skipped_count": res.Skipped,
	})
}

func (h *APIHandler) listRuns(w http.ResponseWriter, r *http.Request) {
	odate := odateOrToday(r.URL.Query().Get("date"))

	list, err := h.store.JobRun().ListByDate(r.Context(), odate)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, list)
}

type RunActionRequest struct {
	Action     string `json:"action"` // "RERUN", "SET_OK", "BYPASS"
	RunID      string `json:"run_id"`
	OperatorID string `json:"operator_id"`
	Reason     string `json:"reason"`
}

func (h *APIHandler) runAction(w http.ResponseWriter, r *http.Request) {
	var req RunActionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	if req.OperatorID == "" || req.Reason == "" {
		writeError(w, http.StatusBadRequest, errors.New("operator_id and reason are required"))
		return
	}

	ctx := r.Context()

	switch strings.ToUpper(req.Action) {
	case "SET_OK":
		if err := h.runs.SetOK(ctx, req.RunID, req.OperatorID, req.Reason); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"success": true, "message": "Set OK applied successfully"})

	case "RERUN":
		newRun, err := h.runs.Rerun(ctx, req.RunID, req.OperatorID, req.Reason)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"success": true, "rerun": newRun})

	case "BYPASS":
		if err := h.runs.Bypass(ctx, req.RunID, req.OperatorID, req.Reason, true); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"success": true, "message": "Bypass applied successfully"})

	default:
		writeError(w, http.StatusBadRequest, errors.New("Unknown action"))
	}
}

func (h *APIHandler) getRunLogs(w http.ResponseWriter, r *http.Request) {
	runID := r.URL.Query().Get("run_id")
	if runID == "" {
		writeError(w, http.StatusBadRequest, errors.New("run_id required"))
		return
	}

	run, _ := h.store.JobRun().GetByID(r.Context(), runID)

	res := map[string]any{
		"run_id": runID,
		"logs":   h.disp.GetHistoricalLogs(runID),
	}
	if run != nil {
		res["job_name"] = run.JobName
		res["state"] = run.State
		res["exit_code"] = run.ExitCode
		res["command"] = run.Command
		res["args"] = run.Args
		res["error_message"] = run.ErrorMessage
		res["started_at"] = run.StartedAt
		res["finished_at"] = run.FinishedAt
	}
	writeJSON(w, http.StatusOK, res)
}

// ---------------------------------------------------------------------------
// Conditions
// ---------------------------------------------------------------------------

func (h *APIHandler) listConditions(w http.ResponseWriter, r *http.Request) {
	odate := odateOrToday(r.URL.Query().Get("date"))

	conds, err := h.store.Condition().ListByDate(r.Context(), odate)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, conds)
}

func (h *APIHandler) addCondition(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name  string `json:"name"`
		ODate string `json:"odate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, errors.New("name is required"))
		return
	}
	req.ODate = odateOrToday(req.ODate)

	if err := h.runs.EmitCondition(r.Context(), req.Name, req.ODate); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "name": req.Name, "odate": req.ODate})
}

func (h *APIHandler) deleteCondition(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		writeError(w, http.StatusBadRequest, errors.New("name is required"))
		return
	}
	odate := odateOrToday(r.URL.Query().Get("date"))

	if err := h.runs.DeleteCondition(r.Context(), name, odate); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

// ---------------------------------------------------------------------------
// Audits / agents
// ---------------------------------------------------------------------------

// auditListLimit bounds how many records GET /api/v1/audits returns when no target_id is given.
const auditListLimit = 500

// listAudits returns the records of target_id, or the most recent records of any target without it.
func (h *APIHandler) listAudits(w http.ResponseWriter, r *http.Request) {
	var (
		audits []*storage.AuditRecord
		err    error
	)
	if target := r.URL.Query().Get("target_id"); target != "" {
		audits, err = h.store.Audit().ListByTarget(r.Context(), target)
	} else {
		audits, err = h.store.Audit().ListRecent(r.Context(), auditListLimit)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, audits)
}

func (h *APIHandler) listAgents(w http.ResponseWriter, r *http.Request) {
	agents := h.hub.GetConnectedAgents()
	writeJSON(w, http.StatusOK, map[string]any{
		"active_agent_count": len(agents),
		"agents":             agents,
	})
}

// ---------------------------------------------------------------------------
// Log streaming
// ---------------------------------------------------------------------------

// handleLogStream provides a WebSocket connection for real-time terminal output streaming.
func (h *APIHandler) handleLogStream(w http.ResponseWriter, r *http.Request) {
	runID := r.URL.Query().Get("run_id")
	if runID == "" {
		http.Error(w, "run_id required", http.StatusBadRequest)
		return
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	// Subscribe before reading history and run state so no chunk or termination is missed in between.
	logCh, unsubscribe := h.disp.Subscribe(runID)
	defer unsubscribe()

	// Detect the browser going away: the reader loop ends when the connection breaks.
	gone := make(chan struct{})
	go func() {
		defer close(gone)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	send := func(chunk protocol.TaskLogChunkPayload) bool {
		bytes, err := json.Marshal(chunk)
		if err != nil {
			return false
		}
		return conn.WriteMessage(websocket.TextMessage, bytes) == nil
	}

	// 1. Replay historical logs first
	seen := make(map[int64]bool)
	for _, chunk := range h.disp.GetHistoricalLogs(runID) {
		seen[chunk.Sequence] = true
		if !send(chunk) {
			return
		}
	}

	// 2. Check if the task is already terminal
	if h.sendTerminationIfDone(r.Context(), send, runID) {
		return
	}

	// 3. Listen for live streaming logs until the task ends (channel closed) or the client leaves
	for {
		select {
		case chunk, ok := <-logCh:
			if !ok {
				h.sendTerminationIfDone(r.Context(), send, runID)
				return
			}
			if seen[chunk.Sequence] { // already replayed from history
				continue
			}
			if !send(chunk) {
				return
			}
		case <-gone:
			return
		}
	}
}

// sendTerminationIfDone sends the synthetic "EXECUTION TERMINATED" line when the run has finished.
func (h *APIHandler) sendTerminationIfDone(ctx context.Context, send func(protocol.TaskLogChunkPayload) bool, runID string) bool {
	run, _ := h.store.JobRun().GetByID(ctx, runID)
	if run == nil || (run.State != storage.StateSuccess && run.State != storage.StateFailed && run.State != storage.StateBypass) {
		return false
	}
	send(protocol.TaskLogChunkPayload{
		TaskID:    runID,
		Stream:    protocol.StreamStdout,
		Content:   fmt.Sprintf("\n--- [FreeJobScheduler] EXECUTION TERMINATED (%s, ExitCode: %d) ---", run.State, run.ExitCode),
		Timestamp: time.Now().UnixMilli(),
	})
	return true
}
