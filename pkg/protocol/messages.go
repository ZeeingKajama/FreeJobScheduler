package protocol

// AgentRegisterPayload is sent by the Agent upon establishing WebSocket connection.
type AgentRegisterPayload struct {
	AgentID        string   `json:"agent_id"`
	Hostname       string   `json:"hostname"`
	OS             string   `json:"os"`   // "linux", "windows"
	Arch           string   `json:"arch"` // "amd64", "arm64"
	Version        string   `json:"version"`
	Labels         []string `json:"labels"`
	MaxConcurrency int32    `json:"max_concurrency,omitempty"`
	AuthToken      string   `json:"auth_token"`
}

// AgentRegisterAckPayload is returned by the Server confirming registration.
type AgentRegisterAckPayload struct {
	Success      bool   `json:"success"`
	AssignedID   string `json:"assigned_id"`
	ServerTime   int64  `json:"server_time"`
	ErrorMessage string `json:"error_message,omitempty"`
}

// TaskDispatchPayload is sent by Server to Agent to execute a workload.
type TaskDispatchPayload struct {
	TaskID         string            `json:"task_id"`
	JobName        string            `json:"job_name"`
	Command        string            `json:"command"`
	Args           []string          `json:"args"`
	Env            map[string]string `json:"env"`
	WorkingDir     string            `json:"working_dir"`
	RunAsUser      string            `json:"run_as_user,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds"`
}

// TaskCancelPayload requests immediate termination of a running task.
type TaskCancelPayload struct {
	TaskID string `json:"task_id"`
	Signal string `json:"signal"` // "SIGTERM", "SIGKILL"
	Reason string `json:"reason"`
}

// TaskAckPayload acknowledges receipt of TaskDispatch.
type TaskAckPayload struct {
	TaskID   string `json:"task_id"`
	Accepted bool   `json:"accepted"`
	Reason   string `json:"reason,omitempty"`
}

// StreamType defines standard output vs standard error.
type StreamType string

const (
	StreamStdout StreamType = "STDOUT"
	StreamStderr StreamType = "STDERR"
)

// TaskLogChunkPayload carries streaming log fragments from Agent to Server.
type TaskLogChunkPayload struct {
	TaskID    string     `json:"task_id"`
	Sequence  int64      `json:"sequence"`
	Stream    StreamType `json:"stream"`
	Content   string     `json:"content"`
	Timestamp int64      `json:"timestamp"`
}

// TaskState is the execution state an Agent reports for a task.
type TaskState string

const (
	TaskRunning TaskState = "RUNNING"
	TaskSuccess TaskState = "SUCCESS"
	TaskFailed  TaskState = "FAILED"
)

// TaskStatusUpdatePayload reports execution progress or terminal status.
type TaskStatusUpdatePayload struct {
	TaskID     string    `json:"task_id"`
	State      TaskState `json:"state"`
	ExitCode   int       `json:"exit_code"`
	ErrorMsg   string    `json:"error_msg,omitempty"`
	FinishedAt *int64    `json:"finished_at,omitempty"`
}
