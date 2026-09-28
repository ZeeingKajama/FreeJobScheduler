package client

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/agent/executor"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/protocol"
	"github.com/gorilla/websocket"
)

// AgentConfig holds agent startup parameters.
type AgentConfig struct {
	ServerURL      string
	AgentID        string
	Hostname       string
	OS             string
	Arch           string
	Version        string
	Labels         []string
	MaxConcurrency int32
	AuthToken      string
}

// Client represents the Agent's persistent background connection to the Master Server.
type Client struct {
	cfg      AgentConfig
	exec     executor.ProcessExecutor
	conn     *websocket.Conn
	writeMu  sync.Mutex
	stopChan chan struct{}
	wg       sync.WaitGroup
}

func NewClient(cfg AgentConfig, exec executor.ProcessExecutor) *Client {
	return &Client{
		cfg:      cfg,
		exec:     exec,
		stopChan: make(chan struct{}),
	}
}

// Start initiates the persistent connection and auto-reconnect loop.
func (c *Client) Start(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		backoff := 1 * time.Second
		const maxBackoff = 3 * time.Second

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopChan:
				return
			default:
			}

			err := c.connectAndServe(ctx)
			if err != nil {
				log.Printf("[FreeJobScheduler Agent] Disconnected or dial failed: %v. Retrying in %v...", err, backoff)
				// Reconnect backoff
				select {
				case <-ctx.Done():
					return
				case <-c.stopChan:
					return
				case <-time.After(backoff):
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
			} else {
				backoff = 1 * time.Second
			}
		}
	}()
}

// Stop shuts down the client.
func (c *Client) Stop() {
	close(c.stopChan)
	c.writeMu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.writeMu.Unlock()
	c.wg.Wait()
}

func (c *Client) connectAndServe(ctx context.Context) error {
	u, err := url.Parse(c.cfg.ServerURL)
	if err != nil {
		return fmt.Errorf("invalid server URL: %w", err)
	}

	dialer := websocket.DefaultDialer
	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return fmt.Errorf("dial failed: %w", err)
	}

	c.writeMu.Lock()
	c.conn = conn
	c.writeMu.Unlock()

	defer func() {
		c.writeMu.Lock()
		_ = conn.Close()
		c.conn = nil
		c.writeMu.Unlock()
	}()

	// 1. Send AGENT_REGISTER
	regPayload := protocol.AgentRegisterPayload{
		AgentID:        c.cfg.AgentID,
		Hostname:       c.cfg.Hostname,
		OS:             c.cfg.OS,
		Arch:           c.cfg.Arch,
		Version:        c.cfg.Version,
		Labels:         c.cfg.Labels,
		MaxConcurrency: c.cfg.MaxConcurrency,
		AuthToken:      c.cfg.AuthToken,
	}

	regEnv, err := protocol.NewEnvelope(protocol.TypeAgentRegister, fmt.Sprintf("reg-%d", time.Now().UnixMilli()), regPayload)
	if err != nil {
		return err
	}

	if err := c.send(regEnv); err != nil {
		return err
	}

	// 2. Read loop
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.stopChan:
			return nil
		default:
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			return err
		}

		var env protocol.Envelope
		if err := json.Unmarshal(message, &env); err != nil {
			continue
		}

		switch env.Type {
		case protocol.TypeAgentRegisterAck:
			// Registration confirmed

		case protocol.TypeTaskDispatch:
			var dispatch protocol.TaskDispatchPayload
			if err := env.DecodePayload(&dispatch); err == nil {
				go c.handleTaskDispatch(ctx, dispatch)
			}

		case protocol.TypeTaskCancel:
			var cancel protocol.TaskCancelPayload
			if err := env.DecodePayload(&cancel); err == nil {
				_ = c.exec.Cancel(cancel.TaskID, cancel.Signal)
			}
		}
	}
}

func (c *Client) handleTaskDispatch(ctx context.Context, d protocol.TaskDispatchPayload) {
	// Acknowledge task receipt
	ackEnv, _ := protocol.NewEnvelope(protocol.TypeTaskAck, fmt.Sprintf("ack-%s", d.TaskID), protocol.TaskAckPayload{
		TaskID:   d.TaskID,
		Accepted: true,
	})
	_ = c.send(ackEnv)

	// Report RUNNING state
	runEnv, _ := protocol.NewEnvelope(protocol.TypeTaskStatusUpdate, fmt.Sprintf("stat-%s-run", d.TaskID), protocol.TaskStatusUpdatePayload{
		TaskID: d.TaskID,
		State:  protocol.TaskRunning,
	})
	_ = c.send(runEnv)

	// Setup log streaming channel
	logChan := make(chan protocol.TaskLogChunkPayload, 50)
	logsDone := make(chan struct{})
	go func() {
		defer close(logsDone)
		for chunk := range logChan {
			logEnv, err := protocol.NewEnvelope(protocol.TypeTaskLogChunk, fmt.Sprintf("log-%s-%d", chunk.TaskID, chunk.Sequence), chunk)
			if err == nil {
				_ = c.send(logEnv)
			}
		}
	}()

	spec := executor.TaskSpec{
		TaskID:         d.TaskID,
		Command:        d.Command,
		Args:           d.Args,
		Env:            d.Env,
		WorkingDir:     d.WorkingDir,
		RunAsUser:      d.RunAsUser,
		TimeoutSeconds: d.TimeoutSeconds,
	}

	result, err := c.exec.Execute(ctx, spec, logChan)
	close(logChan)
	<-logsDone // flush every log chunk before the terminal status so the server never sees logs after it

	// Send terminal status
	finishedAt := time.Now().UnixMilli()
	statusPayload := protocol.TaskStatusUpdatePayload{
		TaskID:     d.TaskID,
		FinishedAt: &finishedAt,
	}

	if err != nil || result == nil || result.ExitCode != 0 {
		statusPayload.State = protocol.TaskFailed
		if result != nil {
			statusPayload.ExitCode = result.ExitCode
		} else {
			statusPayload.ExitCode = 1
		}
		// err means the process could not run at all; result.Error explains an abnormal end
		// (timeout, cancel, non-zero exit status).
		if err != nil {
			statusPayload.ErrorMsg = err.Error()
		} else if result != nil && result.Error != nil {
			statusPayload.ErrorMsg = result.Error.Error()
		}
	} else {
		statusPayload.State = protocol.TaskSuccess
		statusPayload.ExitCode = 0
	}

	finEnv, _ := protocol.NewEnvelope(protocol.TypeTaskStatusUpdate, fmt.Sprintf("stat-%s-fin", d.TaskID), statusPayload)
	_ = c.send(finEnv)
}

func (c *Client) send(env *protocol.Envelope) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("connection is offline")
	}

	bytes, err := json.Marshal(env)
	if err != nil {
		return err
	}

	_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return c.conn.WriteMessage(websocket.TextMessage, bytes)
}
