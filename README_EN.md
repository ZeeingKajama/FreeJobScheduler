# FreeJobScheduler

**A lightweight open-source job scheduler inspired by Control-M**

FreeJobScheduler is a tool designed to orchestrate and run batch scripts scattered across multiple servers in a defined sequence from a single central point, while monitoring execution flows and logs through a web console.
Configure dependency flows such as *"Run B when A finishes, and run D when both B and C finish"*, and subsequent jobs will trigger automatically the instant upstream tasks succeed.

> *A lightweight, open-source batch job scheduler inspired by enterprise workload automation tools.  
> One Go server, one Go agent per host, SQLite storage, condition-based job chaining, and a built-in web console.*

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Go](https://img.shields.io/badge/Go-1.27%2B-00ADD8?logo=go)

---

## Key Features

- **Simple Deployment** — A single server binary and a single agent binary. Web console assets and DB schemas are embedded directly into the binary; storage requires only a single SQLite database file.
- **Condition-Based Chaining** — When a job finishes successfully, it creates *out-conditions*. Any job requiring those as *in-conditions* is immediately unlocked and queued for execution. Chaining works seamlessly across different job groups.
- **Order Date (ODATE)** — Order, execute, and track identical job definitions separately for each business date. Conditions are isolated and managed per ODATE.
- **Reverse Agent Connections** — Agents initiate outbound WebSocket connections to the server, eliminating the need to open inbound firewall rules into target worker servers.
- **Label-Based Assignment & Concurrency Limits** — Tag-based job placement (e.g., *"run this job only on accounting servers"*) and per-agent execution slot limits.
- **Execution Timeouts** — When an execution exceeds its defined timeout limit, its entire process group is terminated and marked as failed.
- **Real-Time Logs** — View live streaming job output directly in the web console, with logs archived to disk upon job completion.
- **Operator Actions & Audit Trail** — Support for manual interventions: Rerun, Set OK, and Bypass. Tracks who performed the action, when, and why.
- **Workflow Graph (DAG)** — Interactive DAG visualization illustrating job dependencies and real-time execution states.
- **REST API** — All console operations can be performed via REST APIs, making it simple to integrate with cron scripts or CI/CD pipelines.

## Architecture

```
 ┌──────────── Browser (Web Console) ────────────┐
 └───────────────────────┬───────────────────────┘
                         │ HTTP (Default: 8080)
 ┌───────────────────────▼───────────────────────┐
 │  fjs-server                                   │
 │   REST API · Web Console · Scheduling · SQLite│
 └───────────┬───────────────────────┬───────────┘
             │ WebSocket (/ws/agent) │
 ┌───────────▼────┐         ┌────────▼───────┐
 │ fjs-agent      │         │ fjs-agent      │   ← One per host executing jobs
 │ labels: etl    │         │ labels: fin    │
 └────────────────┘         └────────────────┘
```

## Quick Start

Go 1.27.1 or higher is required (for building only).

```bash
git clone https://github.com/ZeeingKajama/FreeJobScheduler.git
cd FreeJobScheduler

go build -o bin/fjs-server ./cmd/server
go build -o bin/fjs-agent  ./cmd/agent

./bin/fjs-server &        # http://localhost:8080 , DB: ./fjs.db
./bin/fjs-agent  &        # Connects to server with labels 'linux,batch'
```

Open `http://localhost:8080` in your browser to access the web console.

To populate the console with 12 sample jobs (after launching the server once to generate the DB schema):

```bash
go run ./cmd/seed fjs.db   # ⚠ Clears all existing DB data. DO NOT run against production DBs!
```

### Job Registration & Ordering Example

```bash
# Register two jobs chained by conditions
curl -fsS -X POST http://localhost:8080/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "id": "EXTRACT", "name": "Extract", "group": "DAILY",
  "command": "/opt/batch/extract.sh",
  "out_conditions": ["EXTRACT-OK"], "timeout_sec": 3600, "enabled": true }'

curl -fsS -X POST http://localhost:8080/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "id": "LOAD", "name": "Load", "group": "DAILY",
  "command": "/opt/batch/load.sh",
  "in_conditions": ["EXTRACT-OK"], "timeout_sec": 3600, "enabled": true }'

# Order the DAILY group for ODATE 2026-09-30 -> LOAD automatically starts once EXTRACT succeeds
curl -fsS -X POST http://localhost:8080/api/v1/runs/order -H 'Content-Type: application/json' \
  -d '{"odate":"20260930","group":"DAILY","operator_id":"admin"}'
```

## CLI Flags & Options

| Server (`fjs-server`) | Default | Description |
|---|---|---|
| `-port` | `8080` | HTTP / WebSocket port |
| `-db` | `fjs.db` | SQLite file path (created if not found) |
| `-logdir` | `logs/runs` | Directory for job execution log files |
| `-token` | `fjs-secret-token` | Agent authentication token. **Must be changed in production** |

| Agent (`fjs-agent`) | Default | Description |
|---|---|---|
| `-server` | `ws://localhost:8080/ws/agent` | Server WebSocket address |
| `-id` | `agent-<hostname>` | Agent identifier |
| `-labels` | `linux,batch` | Comma-separated agent labels |
| `-token` | `fjs-secret-token` | Must match the server's `-token` |
| `-max-concurrency` | `0` (Server default: 10) | Maximum concurrent jobs on this agent |

## API Summary

| Method | Path | Description |
|---|---|---|
| `GET` / `POST` / `PUT` / `DELETE` | `/api/v1/jobs` | Query, create, update, or delete job definitions |
| `GET` | `/api/v1/jobs/detail?id=` | Retrieve a single job definition |
| `POST` | `/api/v1/jobs/trigger` | Trigger an immediate one-off execution of a job |
| `POST` | `/api/v1/runs/order` | Batch order jobs by ODATE and optional group |
| `GET` | `/api/v1/runs?date=` | List job runs for a specific ODATE |
| `GET` | `/api/v1/runs/logs?run_id=` | Retrieve logs for a specific run |
| `POST` | `/api/v1/runs/action` | Perform operator actions: `RERUN` / `SET_OK` / `BYPASS` |
| `GET` / `POST` / `DELETE` | `/api/v1/conditions` | Query, manually create, or delete conditions |
| `GET` | `/api/v1/audits` | Query audit trail records |
| `GET` | `/api/v1/agents` | List active connected agents and slot usage |
| `GET` | `/ws/logs` | Real-time log streaming over WebSocket |

For complete request/response schemas, refer to [User Manual Section 11 (API Reference)](USER_MANUAL_EN.md#11-api-reference).

## Current Limitations

The current version prioritizes simplicity and lightweight architecture. Please review the following before production deployment:

- **No Authentication/Authorization:** The web console and REST APIs do not require credentials. Restrict access strictly to trusted internal networks.
- **No Native Time-Based Scheduling (Cron):** The "schedule" field in job definitions is informational only. Automate ordering by calling the ordering API via OS `crontab`.
- **No Built-in Holiday Calendar:** Manage business calendars using a holiday list file combined with an ordering script as detailed in the manual.
- In-conditions support AND logic only, and conditions can only be resolved within the same ODATE.
- Job execution is supported only on Linux/Unix-based agents.
- Only SQLite storage is supported out of the box.

For a full list of limitations and workarounds, refer to [User Manual Section 14 (Limitations and Known Issues)](USER_MANUAL_EN.md#14-limitations-and-known-issues).

## Documentation

- [USER_MANUAL_EN.md](USER_MANUAL_EN.md) — Comprehensive user manual covering installation, operation, calendars, troubleshooting, API reference, and source code architecture.
- [Korean Documentation: README.md](README.md) / [USER_MANUAL.md](USER_MANUAL.md) — 원문 한국어 문서
- [docs/](docs/) — Architecture and design notes (may contain historical or planning drafts).

## Development

```bash
go build ./...
go test ./...
```

| Path | Description |
|---|---|
| `cmd/server`, `cmd/agent`, `cmd/seed` | Entrypoints for server, agent, and seed data generator |
| `server/engine/runs` | Job run lifecycle (ordering, condition checking, operator actions) |
| `server/engine/condition` | In-memory condition indexing and caching |
| `server/dispatcher` | Agent assignment, slot dispatching, log storage & streaming |
| `server/agenthub` | Agent WebSocket connection and slot management |
| `server/webapi` | REST API handlers |
| `agent/` | Agent connection handling and process execution |
| `pkg/storage` | Domain models, state transition rules, and SQLite storage |
| `web/` | Web console frontend (embedded into binary) |

## Reuse & Modification

Feel free to adapt and customize the codebase for your own infrastructure needs.

## License

Released under the [MIT License](LICENSE). You are free to use, modify, and distribute it for both commercial and non-commercial purposes.

## Trademark Notice

FreeJobScheduler is an independent open-source project and is not affiliated with, sponsored by, or endorsed by BMC Software, Inc.  
Control-M is a trademark or registered trademark of BMC Software, Inc., referenced in this documentation solely to describe the design background.  
FreeJobScheduler contains no source code, documentation, or UI designs from Control-M, and does not guarantee compatibility with Control-M.
