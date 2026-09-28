package dispatcher

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
)

// RingBufferLog maintains a bounded in-memory circular buffer of log chunks per task.
// It prevents unbounded memory growth (OOM) when large batch workloads emit millions of lines.
type RingBufferLog struct {
	mu       sync.RWMutex
	capacity int
	chunks   []protocol.TaskLogChunkPayload
	start    int
	count    int
}

// NewRingBufferLog initializes a fixed-capacity ring buffer.
func NewRingBufferLog(capacity int) *RingBufferLog {
	if capacity <= 0 {
		capacity = 1000 // Default 1,000 chunks for live streaming UI view
	}
	return &RingBufferLog{
		capacity: capacity,
		chunks:   make([]protocol.TaskLogChunkPayload, capacity),
	}
}

// Append inserts a chunk into the circular buffer, dropping the oldest chunk if full.
func (rb *RingBufferLog) Append(chunk protocol.TaskLogChunkPayload) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	idx := (rb.start + rb.count) % rb.capacity
	if rb.count < rb.capacity {
		rb.chunks[idx] = chunk
		rb.count++
	} else {
		// Overwrite the oldest element and advance start
		rb.chunks[idx] = chunk
		rb.start = (rb.start + 1) % rb.capacity
	}
}

// GetAll returns a snapshot copy of all retained log chunks in chronological order.
func (rb *RingBufferLog) GetAll() []protocol.TaskLogChunkPayload {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	result := make([]protocol.TaskLogChunkPayload, rb.count)
	for i := 0; i < rb.count; i++ {
		result[i] = rb.chunks[(rb.start+i)%rb.capacity]
	}
	return result
}

// Count returns the number of buffered chunks.
func (rb *RingBufferLog) Count() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.count
}

// LogSpooler streams and persists all stdout/stderr log output directly to disk files.
// This guarantees zero log loss for compliance and audit requirements without loading entire logs in memory.
type LogSpooler struct {
	baseDir string
	mu      sync.Mutex
	writers map[string]*bufio.Writer
	files   map[string]*os.File
}

// NewLogSpooler creates a spooler storing log files under baseDir.
func NewLogSpooler(baseDir string) (*LogSpooler, error) {
	if baseDir == "" {
		baseDir = filepath.Join("logs", "runs")
	}
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	return &LogSpooler{
		baseDir: baseDir,
		writers: make(map[string]*bufio.Writer),
		files:   make(map[string]*os.File),
	}, nil
}

// WriteChunk buffers and writes one log line to the task's log file using a 64KB I/O buffer.
// Agents send lines without their trailing newline, so it is appended here.
func (ls *LogSpooler) WriteChunk(taskID string, content string) error {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	w, ok := ls.writers[taskID]
	if !ok {
		filePath := filepath.Join(ls.baseDir, fmt.Sprintf("%s.log", taskID))
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		w = bufio.NewWriterSize(f, 64*1024)
		ls.files[taskID] = f
		ls.writers[taskID] = w
	}

	if _, err := w.WriteString(content); err != nil {
		return err
	}
	return w.WriteByte('\n')
}

// CloseTask flushes and closes file handles for taskID when execution finishes.
func (ls *LogSpooler) CloseTask(taskID string) {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	if w, ok := ls.writers[taskID]; ok {
		_ = w.Flush()
		delete(ls.writers, taskID)
	}
	if f, ok := ls.files[taskID]; ok {
		_ = f.Close()
		delete(ls.files, taskID)
	}
}

// ReadFullLog reads the entire spool file from disk for post-mortem analysis or download.
func (ls *LogSpooler) ReadFullLog(taskID string) (string, error) {
	ls.mu.Lock()
	if w, ok := ls.writers[taskID]; ok {
		_ = w.Flush()
	}
	ls.mu.Unlock()

	filePath := filepath.Join(ls.baseDir, fmt.Sprintf("%s.log", taskID))
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	bytes, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// ReadTail returns the last n lines of the task's spool file and the 1-based line number of the first
// returned line, streaming the file so long logs are never loaded whole.
func (ls *LogSpooler) ReadTail(taskID string, n int) ([]string, int, error) {
	ls.mu.Lock()
	if w, ok := ls.writers[taskID]; ok {
		_ = w.Flush()
	}
	ls.mu.Unlock()

	f, err := os.Open(filepath.Join(ls.baseDir, fmt.Sprintf("%s.log", taskID)))
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024) // agents cap lines at 1MB
	tail := make([]string, 0, n)
	total := 0
	for scanner.Scan() {
		total++
		if len(tail) == n {
			copy(tail, tail[1:])
			tail = tail[:n-1]
		}
		tail = append(tail, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, err
	}
	return tail, total - len(tail) + 1, nil
}

// CloseAll safely closes all active file handles (e.g. during server shutdown).
func (ls *LogSpooler) CloseAll() {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	for taskID, w := range ls.writers {
		_ = w.Flush()
		delete(ls.writers, taskID)
	}
	for taskID, f := range ls.files {
		_ = f.Close()
		delete(ls.files, taskID)
	}
}
