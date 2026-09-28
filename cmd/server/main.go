package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage/sqlstore"
	"github.com/ZeeingKajama/FreeJobScheduler/server/agenthub"
	"github.com/ZeeingKajama/FreeJobScheduler/server/dispatcher"
	"github.com/ZeeingKajama/FreeJobScheduler/server/engine/runs"
	"github.com/ZeeingKajama/FreeJobScheduler/server/webapi"
	"github.com/ZeeingKajama/FreeJobScheduler/web"
	_ "modernc.org/sqlite"
)

func main() {
	port := flag.Int("port", 8080, "Master Server HTTP/WebSocket port")
	dbPath := flag.String("db", "fjs.db", "SQLite / ODBC database path")
	authToken := flag.String("token", "fjs-secret-token", "Pre-shared agent authentication token")
	logDir := flag.String("logdir", dispatcher.DefaultLogDir, "Directory for per-run log files")
	flag.Parse()

	log.Printf("[Master Server] Starting FreeJobScheduler Server on port :%d...", *port)

	// 1. Initialize Storage Layer (ANSI-SQL compatible)
	db, err := sql.Open("sqlite", fmt.Sprintf("%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)", *dbPath))
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	store := sqlstore.NewSQLStore(db)

	if err := store.InitializeSchema(context.Background(), sqlstore.SchemaDDL); err != nil {
		log.Fatalf("failed to initialize database schema: %v", err)
	}

	// 2. Initialize the run service, AgentHub and Dispatcher
	svc := runs.NewService(store)
	disp := dispatcher.NewDispatcher(store, svc, nil, dispatcher.WithLogDir(*logDir))
	hub := agenthub.NewHub(*authToken, disp)
	disp.SetHub(hub)

	// 3. Restore WAIT runs into the condition index, then start the Dispatcher polling loop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := svc.Bootstrap(ctx); err != nil {
		log.Printf("[Master Server] Warning: could not restore waiting runs: %v", err)
	}
	disp.Start(ctx, 1*time.Second)
	go pruneConditionCache(ctx, svc)

	// 4. Setup HTTP Router & Web API
	mux := http.NewServeMux()

	// Agent WebSocket endpoint
	mux.Handle("/ws/agent", hub)

	// Web Console API
	apiHandler := webapi.NewAPIHandler(store, hub, disp, svc)
	apiHandler.RegisterRoutes(mux)

	// Serve Frontend Static Assets
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServerFS(web.Assets)))
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, web.Assets, "index.html")
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: mux,
	}

	// Graceful Shutdown Handler
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		<-sigCh
		log.Println("[Master Server] Shutting down...")
		disp.Stop()
		ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelShutdown()
		_ = server.Shutdown(ctxShutdown)
	}()

	log.Printf("[Master Server] Console Web UI available at: http://localhost:%d", *port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}

// conditionCacheRetentionDays is how many days of ODates the in-memory condition cache keeps.
// Older entries are dropped; the database stays the source of truth and is consulted on demand.
const conditionCacheRetentionDays = 7

// pruneConditionCache periodically drops cached conditions of old ODates so the cache cannot grow forever.
func pruneConditionCache(ctx context.Context, svc *runs.Service) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			svc.PruneConditionCache(time.Now().AddDate(0, 0, -conditionCacheRetentionDays).Format("20060102"))
		}
	}
}
