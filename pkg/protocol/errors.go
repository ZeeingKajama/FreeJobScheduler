package protocol

import "errors"

var (
	ErrMissingMessageID       = errors.New("protocol: message id cannot be empty")
	ErrMissingMessageType     = errors.New("protocol: message type cannot be empty")
	ErrEmptyPayload           = errors.New("protocol: payload cannot be empty")
	ErrPayloadMarshalFailed   = errors.New("protocol: failed to marshal payload")
	ErrPayloadUnmarshalFailed = errors.New("protocol: failed to unmarshal payload")
)
