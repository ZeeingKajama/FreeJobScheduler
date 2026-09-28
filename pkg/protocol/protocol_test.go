package protocol

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEnvelope_SerializationAndDecoding(t *testing.T) {
	// Given a TaskDispatchPayload
	dispatch := TaskDispatchPayload{
		TaskID:         "task-12345",
		JobName:        "DAILY_SETTLE_01",
		Command:        "/opt/batch/run.sh",
		Args:           []string{"--date", "20260916", "--dry-run=false"},
		Env:            map[string]string{"ODATE": "20260916", "ENV": "PRD"},
		WorkingDir:     "/opt/batch",
		RunAsUser:      "batchuser",
		TimeoutSeconds: 600,
	}

	// When wrapping in an Envelope
	env, err := NewEnvelope(TypeTaskDispatch, "msg-001", dispatch)
	if err != nil {
		t.Fatalf("failed to create envelope: %v", err)
	}

	if env.Type != TypeTaskDispatch {
		t.Errorf("expected type %s, got %s", TypeTaskDispatch, env.Type)
	}
	if env.ID != "msg-001" {
		t.Errorf("expected id msg-001, got %s", env.ID)
	}
	if env.Timestamp == 0 {
		t.Errorf("timestamp should be populated")
	}

	// When serializing to JSON wire format
	wireBytes, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("failed to marshal wire bytes: %v", err)
	}

	// Then deserializing wire format back into Envelope
	var receivedEnv Envelope
	if err := json.Unmarshal(wireBytes, &receivedEnv); err != nil {
		t.Fatalf("failed to unmarshal wire envelope: %v", err)
	}

	// And decoding payload into typed struct
	var decodedDispatch TaskDispatchPayload
	if err := receivedEnv.DecodePayload(&decodedDispatch); err != nil {
		t.Fatalf("failed to decode payload: %v", err)
	}

	// Verify all fields are preserved
	if decodedDispatch.TaskID != dispatch.TaskID {
		t.Errorf("expected task_id %s, got %s", dispatch.TaskID, decodedDispatch.TaskID)
	}
	if decodedDispatch.JobName != dispatch.JobName {
		t.Errorf("expected job_name %s, got %s", dispatch.JobName, decodedDispatch.JobName)
	}
	if len(decodedDispatch.Args) != 3 || decodedDispatch.Args[0] != "--date" {
		t.Errorf("unexpected args: %v", decodedDispatch.Args)
	}
	if decodedDispatch.Env["ODATE"] != "20260916" {
		t.Errorf("expected env ODATE=20260916, got %s", decodedDispatch.Env["ODATE"])
	}
	if decodedDispatch.TimeoutSeconds != 600 {
		t.Errorf("expected timeout 600, got %d", decodedDispatch.TimeoutSeconds)
	}
}

func TestEnvelope_ValidationFailures(t *testing.T) {
	// Empty message ID
	_, err := NewEnvelope(TypeTaskAck, "", TaskAckPayload{TaskID: "t"})
	if err != ErrMissingMessageID {
		t.Errorf("expected ErrMissingMessageID, got %v", err)
	}

	// Empty message Type
	_, err = NewEnvelope("", "id-1", TaskAckPayload{TaskID: "t"})
	if err != ErrMissingMessageType {
		t.Errorf("expected ErrMissingMessageType, got %v", err)
	}

	// Decode empty payload
	emptyEnv := Envelope{Type: TypeTaskAck, ID: "id-1"}
	var target TaskAckPayload
	if err := emptyEnv.DecodePayload(&target); err != ErrEmptyPayload {
		t.Errorf("expected ErrEmptyPayload, got %v", err)
	}

	// Decode malformed payload
	malformedEnv := Envelope{
		Type:    TypeTaskDispatch,
		ID:      "id-2",
		Payload: []byte(`{"timeout_seconds": "not_an_int"}`),
	}
	var dispatch TaskDispatchPayload
	if err := malformedEnv.DecodePayload(&dispatch); err == nil {
		t.Errorf("expected unmarshal error on malformed payload, got nil")
	}
}

func TestEnvelope_LogChunkAndStatusPayload(t *testing.T) {
	now := time.Now().UnixMilli()
	logChunk := TaskLogChunkPayload{
		TaskID:    "task-999",
		Sequence:  42,
		Stream:    StreamStdout,
		Content:   "Database migration completed: 142 tables updated\n",
		Timestamp: now,
	}

	env, err := NewEnvelope(TypeTaskLogChunk, "msg-002", logChunk)
	if err != nil {
		t.Fatalf("failed to create log envelope: %v", err)
	}

	var decodedLog TaskLogChunkPayload
	if err := env.DecodePayload(&decodedLog); err != nil {
		t.Fatalf("failed to decode log payload: %v", err)
	}

	if decodedLog.Sequence != 42 || decodedLog.Stream != StreamStdout {
		t.Errorf("mismatch in decoded log chunk: %+v", decodedLog)
	}

	// Status update payload
	finishedAt := time.Now().UnixMilli()
	statusPayload := TaskStatusUpdatePayload{
		TaskID:     "task-999",
		State:      "SUCCESS",
		ExitCode:   0,
		FinishedAt: &finishedAt,
	}

	statusEnv, err := NewEnvelope(TypeTaskStatusUpdate, "msg-003", statusPayload)
	if err != nil {
		t.Fatalf("failed to create status envelope: %v", err)
	}

	var decodedStatus TaskStatusUpdatePayload
	if err := statusEnv.DecodePayload(&decodedStatus); err != nil {
		t.Fatalf("failed to decode status payload: %v", err)
	}

	if decodedStatus.State != "SUCCESS" || decodedStatus.ExitCode != 0 {
		t.Errorf("mismatch in decoded status update: %+v", decodedStatus)
	}
}
