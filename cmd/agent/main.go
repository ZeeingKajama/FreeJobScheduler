package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/ZeeingKajama/FreeJobScheduler/agent/client"
	"github.com/ZeeingKajama/FreeJobScheduler/agent/executor"
)

func main() {
	serverURL := flag.String("server", "ws://localhost:8080/ws/agent", "Master Server WebSocket URL")
	agentID := flag.String("id", "", "Unique Agent ID (default: hostname)")
	labelsFlag := flag.String("labels", "linux,batch", "Comma-separated matching labels")
	authToken := flag.String("token", "fjs-secret-token", "Authentication token")
	maxConcurrency := flag.Int("max-concurrency", 0, "Maximum simultaneous tasks on this agent (0 = server default)")
	flag.Parse()

	hostname, _ := os.Hostname()
	if *agentID == "" {
		*agentID = "agent-" + hostname
	}

	labels := strings.Split(*labelsFlag, ",")
	for i := range labels {
		labels[i] = strings.TrimSpace(labels[i])
	}

	log.Printf("[FreeJobScheduler Agent] Starting agent ID: %s (OS: %s, Arch: %s)...", *agentID, runtime.GOOS, runtime.GOARCH)
	log.Printf("[FreeJobScheduler Agent] Connecting to server at: %s", *serverURL)

	cfg := client.AgentConfig{
		ServerURL:      *serverURL,
		AgentID:        *agentID,
		Hostname:       hostname,
		OS:             runtime.GOOS,
		Arch:           runtime.GOARCH,
		Version:        "v1.0.0",
		Labels:         labels,
		AuthToken:      *authToken,
		MaxConcurrency: int32(*maxConcurrency),
	}

	exec := executor.NewOSExecutor()
	agentClient := client.NewClient(cfg, exec)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	agentClient.Start(ctx)

	// Wait for interrupt
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("[FreeJobScheduler Agent] Stopping...")
	agentClient.Stop()
	log.Println("[FreeJobScheduler Agent] Stopped cleanly.")
}
