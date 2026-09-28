package protocol

import (
	"encoding/json"
	"fmt"
	"time"
)

// MessageType identifies the intent of a protocol message.
type MessageType string

const (
	// Agent lifecycle
	TypeAgentRegister    MessageType = "AGENT_REGISTER"
	TypeAgentRegisterAck MessageType = "AGENT_REGISTER_ACK"

	// Task control (Server -> Agent)
	TypeTaskDispatch MessageType = "TASK_DISPATCH"
	TypeTaskCancel   MessageType = "TASK_CANCEL"

	// Task feedback (Agent -> Server)
	TypeTaskAck          MessageType = "TASK_ACK"
	TypeTaskStatusUpdate MessageType = "TASK_STATUS_UPDATE"
	TypeTaskLogChunk     MessageType = "TASK_LOG_CHUNK"
)

// Envelope wraps all WebSocket payloads with a common routing header.
type Envelope struct {
	Type      MessageType     `json:"type"`
	ID        string          `json:"id"`
	Timestamp int64           `json:"timestamp"` // Unix Epoch Milliseconds
	Payload   json.RawMessage `json:"payload"`
}

// NewEnvelope creates a new serialized Envelope wrapping the provided payload.
func NewEnvelope(msgType MessageType, id string, payload any) (*Envelope, error) {
	if id == "" {
		return nil, ErrMissingMessageID
	}
	if msgType == "" {
		return nil, ErrMissingMessageType
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPayloadMarshalFailed, err)
	}

	return &Envelope{
		Type:      msgType,
		ID:        id,
		Timestamp: time.Now().UnixMilli(),
		Payload:   raw,
	}, nil
}

// DecodePayload unmarshals the internal JSON payload into target.
func (e *Envelope) DecodePayload(target any) error {
	if len(e.Payload) == 0 {
		return ErrEmptyPayload
	}
	if err := json.Unmarshal(e.Payload, target); err != nil {
		return fmt.Errorf("%w: %v", ErrPayloadUnmarshalFailed, err)
	}
	return nil
}
