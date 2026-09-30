# FreeJobScheduler User Manual

> Baseline: 2026-09-28 Source Code (`master` branch)  
> Target Audience: Operators defining and running batch jobs, administrators deploying servers, and developers extending the codebase.

This document is designed to give you a complete, unambiguous understanding of what FreeJobScheduler can and cannot do, how to install and configure it, and how to operate it daily.
Every behavior described here has been verified against the source code and actual test runs. Features not present in the code are not listed, and missing features are explicitly documented as "not supported."

---

## Table of Contents

1. [What is FreeJobScheduler](#1-what-is-freejobscheduler)
2. [Core Concepts](#2-core-concepts)
   - [2.1 Job Definitions and Runs](#21-job-definitions-and-runs)
   - [2.2 Order Date (ODATE)](#22-order-date-odate)
   - [2.3 Conditions](#23-conditions)
   - [2.4 Agents, Labels, and Slots](#24-agents-labels-and-slots)
   - [2.5 Run States](#25-run-states)
3. [Installation and Setup](#3-installation-and-setup)
   - [3.1 Prerequisites](#31-prerequisites)
   - [3.2 Building from Source](#32-building-from-source)
   - [3.3 Server CLI Options](#33-server-cli-options)
   - [3.4 Agent CLI Options](#34-agent-cli-options)
   - [3.5 Production Deployment Example](#35-production-deployment-example)
   - [3.6 Local Quick Start](#36-local-quick-start)
   - [3.7 File Locations](#37-file-locations)
4. [Web Console Guide](#4-web-console-guide)
   - [4.1 Top Navigation Bar](#41-top-navigation-bar)
   - [4.2 Status Bar](#42-status-bar)
   - [4.3 DAG Map (Workflow Graph)](#43-dag-map-workflow-graph)
   - [4.4 Job Detail Drawer](#44-job-detail-drawer)
   - [4.5 The Five Tabs](#45-the-five-tabs)
   - [4.6 Log Modal](#46-log-modal)
   - [4.7 Keyboard Shortcuts](#47-keyboard-shortcuts)
5. [Creating Jobs](#5-creating-jobs)
   - [5.1 Registration via Web Console](#51-registration-via-web-console)
   - [5.2 Registration via API and JSON Files (Recommended for Production)](#52-registration-via-api-and-json-files-recommended-for-production)
   - [5.3 How Commands are Executed](#53-how-commands-are-executed)
   - [5.4 Dates are Not Passed to Scripts](#54-dates-are-not-passed-to-scripts)
   - [5.5 Disabling and Deleting Jobs](#55-disabling-and-deleting-jobs)
6. [Chaining Jobs (In-Conditions and Out-Conditions)](#6-chaining-jobs-in-conditions-and-out-conditions)
   - [6.1 Basic Chaining](#61-basic-chaining)
   - [6.2 Cross-Group Chaining](#62-cross-group-chaining)
   - [6.3 Creating Conditions via Human Operators or External Systems](#63-creating-conditions-via-human-operators-or-external-systems)
   - [6.4 Deleting Conditions](#64-deleting-conditions)
   - [6.5 Important Considerations and Caveats](#65-important-considerations-and-caveats)
7. [Executing Jobs (Ordering)](#7-executing-jobs-ordering)
   - [7.1 Batch Ordering](#71-batch-ordering)
   - [7.2 Immediate Single Execution (Trigger)](#72-immediate-single-execution-trigger)
   - [7.3 Executing Immediately Upon Registration](#73-executing-immediately-upon-registration)
   - [7.4 Automating Daily Ordering](#74-automating-daily-ordering)
8. [Business Day Calendar Management](#8-business-day-calendar-management)
   - [8.1 Using Groups as Calendars](#81-using-groups-as-calendars)
   - [8.2 Holiday List File](#82-holiday-list-file)
   - [8.3 Ordering Script](#83-ordering-script)
   - [8.4 Verification via Dry Run](#84-verification-via-dry-run)
   - [8.5 Calendar Operational Scenarios](#85-calendar-operational-scenarios)
9. [Real-World Case Study: E-Commerce Daily Sales Reconciliation](#9-real-world-case-study-e-commerce-daily-sales-reconciliation)
   - [9.1 Business Workflow](#91-business-workflow)
   - [9.2 Job Design](#92-job-design)
   - [9.3 Creating Job Definition Files](#93-creating-job-definition-files)
   - [9.4 Initial Test Run](#94-initial-test-run)
   - [9.5 A Typical Day in Operation (September 30, 2026, Month-End)](#95-a-typical-day-in-operation-september-30-2026-month-end)
   - [9.6 Incident Response & Operational Scenarios](#96-incident-response--operational-scenarios)
10. [Troubleshooting and Incident Handling](#10-troubleshooting-and-incident-handling)
   - [10.1 Three Operator Actions](#101-three-operator-actions)
   - [10.2 Diagnosis by Symptom](#102-diagnosis-by-symptom)
   - [10.3 Stopping a Running Job](#103-stopping-a-running-job)
   - [10.4 When an Agent Host Crashes](#104-when-an-agent-host-crashes)
   - [10.5 When the Server Restarts](#105-when-the-server-restarts)
11. [API Reference](#11-api-reference)
   - [11.1 Endpoint List](#111-endpoint-list)
   - [11.2 Common API Examples](#112-common-api-examples)
12. [Operational Checklist](#12-operational-checklist)
   - [12.1 Initial Deployment](#121-initial-deployment)
   - [12.2 Backup and Maintenance](#122-backup-and-maintenance)
   - [12.3 Failure Alerting Script](#123-failure-alerting-script)
   - [12.4 Daily Morning Inspection](#124-daily-morning-inspection)
13. [Developer Guide](#13-developer-guide)
   - [13.1 Codebase Architecture](#131-codebase-architecture)
   - [13.2 Architecture & Flow at a Glance](#132-architecture--flow-at-a-glance)
   - [13.3 Building and Testing](#133-building-and-testing)
14. [Current Limitations and Known Issues](#14-current-limitations-and-known-issues)
   - [14.1 Missing Features](#141-missing-features)
   - [14.2 Operational Behaviors to Note](#142-operational-behaviors-to-note)
   - [14.3 Known Web Console Issues](#143-known-web-console-issues)

---

## 1. What is FreeJobScheduler

FreeJobScheduler is a tool designed to **orchestrate, run, and monitor batch scripts distributed across multiple servers in a defined execution order from a central control point**.
By registering execution dependencies such as *"Run B when A finishes, and run D when both B and C finish"*, downstream tasks trigger automatically the instant upstream tasks complete successfully.

### Architecture

```
 ┌──────────── Browser (Web Console) ────────────┐
 │ Job Management · Ordering · DAG · Logs · Actions│
 └───────────────────────┬───────────────────────┘
                         │ HTTP (Default Port 8080)
 ┌───────────────────────▼───────────────────────┐
 │  FreeJobScheduler Server (fjs-server)         │
 │   - Web console and REST API provider         │
 │   - SQLite store: job definitions, runs, conds│
 │   - Dispatches ready runs to matching agents  │
 │   - Stores execution logs to disk             │
 └───────────┬───────────────────────┬───────────┘
             │ WebSocket (/ws/agent) │
 ┌───────────▼────┐         ┌────────▼───────┐
 │ Agent etl01    │         │ Agent fin01    │   ← One per host executing commands
 │ labels: linux,etl│       │ labels: linux,fin│
 └────────────────┘         └────────────────┘
```

- **Server:** Run exactly one instance. All operational data is stored in a single SQLite database file co-located with the server binary.
- **Agent:** Run one instance on each server where commands actually execute. Agents initiate the outbound WebSocket connection to the server, meaning no inbound firewall ports need to be opened into agent worker hosts.
- **Execution Context:** The agent executes commands assigned by the server **on its local machine, under the OS user account running the agent process, inside the agent's current working directory**.

### Capabilities

| Feature | Description |
|---|---|
| Job Registration / Modification / Deletion | Supported via the web console or REST API. |
| In- / Out-Condition Chaining | "Execute only when all these conditions exist" / "Generate this condition upon success". |
| Per-ODATE Execution | Run the same job definition separately for each business date, maintaining independent histories. |
| Batch Ordering | Order all active jobs (or a specific group) for a chosen date into the execution queue at once. |
| Immediate Single Trigger | Immediately trigger a single job run. |
| Target Host Assignment via Labels | Route jobs via label requirements (e.g., *"run this job only on accounting servers"*). |
| Concurrency Throttling | Limit the maximum number of concurrent tasks per agent via slot capacity. |
| Execution Timeouts | Terminate the process group and fail the run if execution exceeds the timeout limit. |
| Real-Time Streaming Logs | Stream live stdout/stderr to the console; persist full logs to disk upon completion. |
| Operator Interventions | Rerun, Set OK, and Bypass with mandatory operator ID and reason logging. |
| DAG Workflow Visualization | Visual representation of job dependencies and real-time execution states. |

### Unsupported Capabilities (Please Read First)

| Unsupported Feature | Recommended Workaround |
|---|---|
| **Built-in Time-Based Scheduler (Cron)** | The "Schedule" field in job definitions is informational only. Automate execution by calling the ordering API via the server's OS `crontab` → [Section 8](#8-business-day-calendar-management). |
| **Built-in Holiday Calendar** | Manage business calendars using a holiday list file and an ordering shell script → [Section 8](#8-business-day-calendar-management). |
| **Date Variable Interpolation (`%%$ODATE`, etc.)** | Unsubstituted strings are passed verbatim. Scripts should calculate their own dates internally → [Section 5.4](#54-dates-are-not-passed-to-scripts). |
| Web Console Kill Button for Running Jobs | Terminate the process group directly on the agent host via `kill` → [Section 10.3](#103-stopping-a-running-job). |
| Hold / Release Run States | Not supported. Deactivate the job definition prior to ordering if it should not run. |
| OR Conditions & Prior-Date Conditions | In-conditions support AND logic only, and conditions are evaluated strictly within the same ODATE. |
| Alerting (Email, Slack, SMS) | Use an external watchdog script polling the REST API → [Section 12.3](#123-failure-alerting-script). |
| Authentication & Role-Based Access Control | Neither the web console nor REST API requires credentials. Restrict network access to trusted private corporate networks. |
| Windows Agents | Job process execution is supported only on Linux/Unix-based environments. |
| Databases Other Than SQLite | While the schema is standard SQL, the server binary embeds only the SQLite driver. |

---

## 2. Core Concepts

### 2.1 Job Definitions and Runs

- **Job Definition:** The blueprint specifying *"what command to run, on which servers, and after what preconditions are satisfied"*. Once registered, it is reused continuously.
- **Run (Execution Instance):** An instance created to execute a job definition for a specific business date. Execution state, exit code, and logs are attached to this run.

A single job definition spawns one run per business date under normal batch ordering. Rerunning a job creates an additional run for the same business date.

When a run is instantiated, the job definition's **command, arguments, and environment variables are copied into the run record**. Consequently, subsequent edits to the job definition will not affect existing runs or their reruns. (Agent labels and timeout limits, however, are re-read from the definition when the run is actually dispatched to an agent.)

Run IDs follow the format `run-<job_id>-<8-char-hex>`, while reruns are formatted as `<original_run_id>-rerun-<8-char-hex>`.

### 2.2 Order Date (ODATE)

An `YYYYMMDD` date tag attached to every run. It represents the business date that the execution belongs to and is independent of actual wall-clock execution time.
For example, if sales data for September 30 is processed at 02:00 AM on October 1, the ODATE remains `20260930`.

- The web console view, run monitor, and condition list are filtered by the selected ODATE.
- In-conditions and out-conditions are scoped strictly per ODATE. A condition generated on September 29 will never satisfy a precondition for a September 30 run.

### 2.3 Conditions

A condition is a named flag that links jobs together. A single condition is defined uniquely by the tuple **(Name + ODATE)**.

- If Job A has an **out-condition** named `SAL-010-OK`, completing Job A successfully creates `SAL-010-OK` for that run's ODATE.
- If Job B has an **in-condition** named `SAL-010-OK`, Job B remains queued until `SAL-010-OK` exists for that ODATE.
- Conditions can also be created manually via the console or REST API. This is used to signal external events outside FreeJobScheduler, such as file arrival or business stakeholder approval.

### 2.4 Agents, Labels, and Slots

- **Labels:** Tags assigned to an agent process upon startup (e.g., `linux,etl`). When labels are configured on a job definition, the job will **only be dispatched to agents possessing all specified labels**. If a job has no labels specified, it may run on any available agent.
- **Slots:** The maximum number of concurrent tasks an agent can execute simultaneously. Defaults to 10 and can be adjusted at agent startup. When an agent's slots are fully saturated, incoming runs remain in the `READY` state.
- Runs with satisfied conditions are dispatched to agents with available slots **in first-in, first-out (FIFO) order**. If no matching agent is currently available for a run, the dispatcher skips it and inspects subsequent ready runs, ensuring one blocked job does not stall the entire queue.

### 2.5 Run States

| State | Description |
|---|---|
| `WAIT` | Waiting for one or more in-conditions to be satisfied. |
| `READY` | All preconditions are satisfied; waiting for an available slot on a matching agent. |
| `ASSIGNED` | Dispatched to an agent; awaiting the agent's start acknowledgment (usually transitions instantaneously). |
| `RUNNING` | Currently executing on the agent host. |
| `SUCCESS` | Terminated with exit code 0, or manually forced by an operator via Set OK. |
| `FAILED` | Terminated with a non-zero exit code. Timeouts produce exit code `124`; external process termination produces `-1`. |
| `BYPASS` | Marked as bypassed by an operator. |

```
 Order ──▶ WAIT ──(All In-Conditions Met)──▶ READY ──▶ ASSIGNED ──▶ RUNNING ──▶ SUCCESS ─▶ Out-Conditions Created
                                              ▲           │                      └─▶ FAILED
                                              └───────────┘
                       (If agent disconnects, reverts to READY to re-dispatch to another agent)
```

- `SUCCESS` and `BYPASS` are terminal states and cannot be modified further.
- `FAILED` runs can only transition via Set OK or Bypass. To re-execute a failed task, trigger a Rerun to generate a new run record.

---

## 3. Installation and Setup

### 3.1 Prerequisites

- Go 1.27.1 or higher (required only for building from source; the compiled binaries run standalone without Go installed).
- Linux servers for running agents (Windows hosts are not supported for job execution).
- Network connectivity from agent hosts and operator workstations to the server's HTTP port (default: 8080).

### 3.2 Building from Source

```bash
git clone https://github.com/ZeeingKajama/FreeJobScheduler.git && cd FreeJobScheduler
go build -o bin/fjs-server ./cmd/server
go build -o bin/fjs-agent  ./cmd/agent
```

The web console assets (`web/`) and DB schema migrations are embedded directly into the server binary. You only need to deploy the two compiled binaries under `bin/`.
If your agent hosts use different CPU architectures, cross-compile accordingly (e.g., `GOOS=linux GOARCH=arm64 go build ...`).

### 3.3 Server CLI Options

| Flag | Default | Description |
|---|---|---|
| `-port` | `8080` | Port for web console, REST API, and agent WebSocket connections. |
| `-db` | `fjs.db` | File path to SQLite database. Created and initialized automatically if not present. |
| `-token` | `fjs-secret-token` | Secret authentication token for agent connections. **Must be changed in production.** |
| `-logdir` | `logs/runs` | Directory where job execution log files are stored. |

Note that default values for `-db` and `-logdir` are relative paths. Their location depends on the working directory from which the binary is invoked. **Always specify absolute paths in production environments.**

### 3.4 Agent CLI Options

| Flag | Default | Description |
|---|---|---|
| `-server` | `ws://localhost:8080/ws/agent` | WebSocket address of the server. |
| `-id` | `agent-<hostname>` | Unique agent identifier. **Must be globally unique across all agents.** If an agent connects with a duplicate ID, the existing connection will be dropped. |
| `-labels` | `linux,batch` | Comma-separated list of agent labels. |
| `-token` | `fjs-secret-token` | Agent authentication token. Must match the server's `-token`. |
| `-max-concurrency` | `0` (Server default: 10) | Maximum number of concurrent tasks this agent can execute. |

Agents automatically retry connections at 1 to 3 second intervals if the server is unreachable. The startup order between server and agents does not matter.

### 3.5 Production Deployment Example

The following example configuration reflects the setup used in the real-world case study in [Section 9](#9-real-world-case-study-e-commerce-daily-sales-reconciliation).

| Server Host | Role | Service |
|---|---|---|
| `batch-mgr01` | Management Server | `fjs-server` |
| `etl01` | Data Extraction & Loading | `fjs-agent -id etl01 -labels linux,etl` |
| `fin01` | Financial Accounting System | `fjs-agent -id fin01 -labels linux,fin -max-concurrency 4` |

#### Management Server Setup

Create directories and configure a systemd service:

```bash
sudo useradd -r -m -d /opt/fjs fjs
sudo mkdir -p /opt/fjs/{bin,data,logs/runs,etc,calendar,jobs}
sudo cp bin/fjs-server /opt/fjs/bin/
echo 'FJS_TOKEN=replace-with-a-sufficiently-long-random-secret' | sudo tee /opt/fjs/etc/fjs.env
sudo chmod 600 /opt/fjs/etc/fjs.env
sudo chown -R fjs: /opt/fjs
```

Create `/etc/systemd/system/fjs-server.service`:

```ini
[Unit]
Description=FreeJobScheduler Server
After=network.target

[Service]
User=fjs
WorkingDirectory=/opt/fjs
EnvironmentFile=/opt/fjs/etc/fjs.env
ExecStart=/opt/fjs/bin/fjs-server -port 8080 -db /opt/fjs/data/fjs.db -logdir /opt/fjs/logs/runs -token ${FJS_TOKEN}
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

#### Agent Server Setup

Run the agent under the dedicated OS account intended to execute the batch scripts. Jobs will execute with the exact permissions and environment variables of this account.

Create `/etc/systemd/system/fjs-agent.service` (example for host `etl01`):

```ini
[Unit]
Description=FreeJobScheduler Agent
After=network-online.target

[Service]
User=batch
WorkingDirectory=/home/batch
EnvironmentFile=/etc/fjs-agent.env
Environment=PATH=/usr/local/bin:/usr/bin:/bin:/opt/batch/bin
Environment=LANG=en_US.UTF-8
ExecStart=/usr/local/bin/fjs-agent -server ws://batch-mgr01:8080/ws/agent -id etl01 -labels linux,etl -token ${FJS_TOKEN}
Restart=always

[Install]
WantedBy=multi-user.target
```

- Create `/etc/fjs-agent.env` containing `FJS_TOKEN=…` matching the server's token, and run `chmod 600 /etc/fjs-agent.env`.
- `WorkingDirectory` is the directory where jobs will run. It is best practice to use absolute paths in all job scripts.
- Jobs inherit the agent process environment. Common settings such as `PATH`, `LANG`, and database credentials should be configured here.

Enable and start services:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now fjs-server   # On batch-mgr01
sudo systemctl enable --now fjs-agent    # On each agent host
```

Open `http://batch-mgr01:8080` in your browser. When `Agents: 2 Active` appears in the upper right header, setup is complete.

> **Caution Regarding Restarts**: If an agent process is stopped while jobs are running, the server will not receive execution results, leaving runs permanently stranded in the `RUNNING` state (see [Section 10.4](#104-when-an-agent-host-crashes)). Similarly, if the server restarts while jobs finish on agents, completion reports may be lost. **Always check the Agents tab and verify that active job count is 0 before restarting server or agent processes.**

### 3.6 Local Quick Start

To test locally on a developer workstation:

```bash
go run ./cmd/server &          # Listens on http://localhost:8080
go run ./cmd/agent &           # Connects with labels linux,batch
```

The script `runall.sh` executes these two commands.

To populate the database with sample data:

```bash
go run ./cmd/seed fjs.db
```

> **Warning**: `cmd/seed` **completely wipes all existing job definitions, run histories, conditions, and audit logs** from the specified database and replaces them with 12 sample jobs. Never execute this against a production database.

### 3.7 File Locations

| Asset | Location |
|---|---|
| Database | File specified by `-db`. Associated `-wal` and `-shm` files are generated side-by-side (the three files form a single SQLite dataset). |
| Job Logs | Files located under `-logdir` named `<run_id>.log`. Each line corresponds to one line of output. Standard output and standard error are interleaved. |
| Server / Agent System Logs | Emitted to standard output. When managed by systemd, view with `journalctl -u fjs-server` or `journalctl -u fjs-agent`. |

---

## 4. Web Console Guide

Open `http://<server-ip>:8080` in a web browser. No authentication prompt is displayed.
The console auto-refreshes every 3 seconds. Refreshing is paused while modals (Job Registration, Logs, Actions, etc.) are open.

### 4.1 Top Navigation Bar

| Element | Description |
|---|---|
| Search Input | Filter the DAG and table views by partial match on job name, job ID, or run ID. |
| **ODATE** | The active business date. Pressing Enter or unfocusing the field updates the view. Ordering, manual triggering, and condition creation apply to this date. |
| `Agents: N Active` | Number of currently connected active agents. |
| `⛶ DAG Map` / `◫ Split` / `☰ Table` | Toggle layout: DAG only, split view, or table only. |
| `⟳` | Refresh the currently selected tab immediately. |
| `📅 Order Daily Plan` | Batch-orders all active jobs across **all groups** for the date specified in the ODATE input ([Section 7](#7-executing-jobs-ordering)). |
| `+ New Batch Job` | Opens the job creation modal ([Section 5](#5-creating-jobs)). |

> **Always verify the ODATE input.** When first loaded, this field defaults to the current calendar date of the client workstation. If you order batch runs after midnight or while reviewing yesterday's view, you could accidentally trigger jobs against the wrong business date. Always verify the date before ordering, triggering, or adding conditions.

### 4.2 Status Bar

Displays real-time counts: `TOTAL / SUCCESS / RUNNING / READY / WAIT / FAILED / BYPASS`.
Clicking a status badge filters the view to that state; clicking `×` clears the filter.
The `READY` count includes runs currently in `ASSIGNED`.

### 4.3 DAG Map (Workflow Graph)

- Each node box represents **one job definition**. For the selected ODATE, the box displays the status of the **most recent run**. If no run exists for that date, it displays `UNSCHEDULED`. If disabled, it displays `DISABLED` (greyed out).
- Directed arrows connect upstream out-conditions to downstream in-conditions. **Green arrows** indicate conditions that have already been generated; **orange arrows** indicate pending conditions.
- The footer of each box displays indicators such as `In: 1/2` (1 of 2 preconditions met), `Root Job` (no preconditions), or `Leaf` (no postconditions).
- Hovering over a job highlights its upstream lineage in orange and downstream dependents in blue, making failure blast radius immediately obvious.
- Pan by dragging the canvas; zoom using the mouse wheel. Click `⛶ Fit` to fit the graph to the viewport.
- Filter the graph using the top **Group** and **State** dropdown selectors.
- **Single click** opens the Job Detail Drawer on the right; **double click** opens the Job Edit Modal.

### 4.4 Job Detail Drawer

Opens upon selecting a job in the DAG map:

- **Run History:** If multiple runs exist for that ODATE (e.g., after reruns), select between them. Action buttons apply to the selected run.
- **Required In-Conditions:** Lists each precondition with `✓ Satisfied` or `⏳ Pending`. Clicking `+ Manual Satisfy` next to a pending condition creates that condition immediately for the current ODATE.
- **Post-Execution Out-Conditions:** Conditions emitted upon job success, along with their current status.
- **Impacted Downstream Jobs:** Lists dependent downstream tasks. Clicking a job navigates to it.
- **Action Buttons:** `⚡ Trigger Now`, `Rerun`, `Set OK`, `Bypass`, `Edit Definition`, and `Sysout Log`.

### 4.5 The Five Tabs

| Tab | Contents |
|---|---|
| **Run Status** | List of runs for the selected ODATE (newest first). Each row provides `Log`, `Edit`, and `Rerun` buttons; non-success rows provide `Set OK`; `WAIT` and `READY` rows provide `Bypass`. |
| **Job Definitions** | List of all registered job definitions. Actions: `Edit`, `⚡ Trigger`, `Delete`. |
| **Conditions** | Conditions created for the selected ODATE. Add new conditions with `+ Manually Create Condition` or delete existing ones with `Delete`. Manual entries are **automatically converted to uppercase**. |
| **Audit Trail** | Audit logs for operator actions (Rerun, Set OK, Bypass) and batch ordering (`ORDER_PLAN`). Displays the last 500 records across all dates. To view logs for a specific run, use the REST API ([Section 11](#11-api-reference)). |
| **Agents** | Active connected agents, labels, and slot usage (Active / Max Slots). |

### 4.6 Log Modal

Opens upon clicking `Log` or `Sysout Log`.
For running jobs, log lines stream in real time. For completed jobs, the last 1,000 lines are displayed.
The header displays the state and exit code. Below it, the `Reason:` line displays failure descriptions (timeout, exit code) or the operator justification entered during Set OK / Bypass.
During active execution, `STDERR` output is highlighted in a distinct color. For completed runs, logs are loaded from disk and displayed under a uniform format. For logs exceeding 1,000 lines, inspect the log file directly in the server's log directory.

### 4.7 Keyboard Shortcuts

Available when cursor focus is outside input fields:

| Key | Action |
|---|---|
| `/` | Focus search input |
| `F` | Toggle DAG only ↔ Split view |
| `T` | Toggle Table only ↔ Split view |
| `R` | Refresh active tab |
| `Esc` | Close active modal/drawer; clear search field if focused |

---

## 5. Creating Jobs

### 5.1 Registration via Web Console

Click `+ New Batch Job`. Fields open blank with placeholder examples:

| Field | Required | Description |
|---|---|---|
| Job Name | Yes | Display name. Duplicate names are not blocked by the system; ensure names are unique within your organization. |
| Job Group | No | Logical categorization used for grouped ordering and UI filtering. Defaults to `DEFAULT` if blank. |
| Command | Yes | The command to execute ([Section 5.3](#53-how-commands-are-executed)). |
| Command Arguments | No | Optional arguments. It is recommended to leave this blank ([Section 5.3](#53-how-commands-are-executed)). |
| Schedule | No | **Informational only.** Does not trigger automated execution. |
| Agent Labels | No | Comma-separated labels. If omitted, the job can run on any agent. |
| In-Conditions | No | Comma-separated preconditions. All must exist before execution starts. |
| Out-Conditions | No | Comma-separated conditions emitted upon successful completion. |
| Run Once Immediately Upon Creation | No | Disabled by default. If enabled, orders a run for the active ODATE immediately upon registration. |

Job IDs are auto-generated in the format `def-1a2b3c4d`. To assign meaningful semantic IDs (such as `SAL-030`), register jobs via the REST API or JSON files.

**The web console creation modal does not provide fields for timeout limits or environment variables.** Jobs created via the web console receive the default timeout of **300 seconds (5 minutes)**. Jobs running longer than 5 minutes must be registered or updated via the API with `timeout_sec`.

### 5.2 Registration via API and JSON Files (Recommended for Production)

For production systems, it is strongly recommended to **maintain job definitions as JSON files in a Git repository and apply them to the server via scripts**. This enables custom semantic Job IDs, precise timeouts, custom environment variables, and change auditing via version control.

Example `/opt/fjs/jobs/SAL-030.json` (filename equals job ID):

```json
{
  "id": "SAL-030",
  "name": "Load_Sales",
  "group": "DAILY",
  "command": "/opt/batch/sales/load_sales.sh",
  "env": { "DB_HOST": "salesdb01", "LOAD_MODE": "full" },
  "agent_labels": ["etl"],
  "in_conditions": ["SAL-010-OK", "SAL-020-OK"],
  "out_conditions": ["SAL-030-OK"],
  "timeout_sec": 7200,
  "enabled": true
}
```

| Field | Description |
|---|---|
| `id` | Unique Job ID. If omitted, auto-generated as `def-xxxxxxxx`. |
| `name` | Human-readable job name. |
| `group` | Job group. Defaults to `DEFAULT` if omitted. |
| `command`, `args` | Command and arguments to execute ([Section 5.3](#53-how-commands-are-executed)). |
| `env` | Custom environment variables injected into this specific job. |
| `agent_labels` | Required agent labels (array of strings). |
| `in_conditions` / `out_conditions` | Required in-conditions / produced out-conditions. |
| `timeout_sec` | Execution timeout in seconds. Defaults to 300 if 0 or omitted. |
| `enabled` | Whether the job is active. When updating via PUT, omitting this sets it to `false` (disabled). |
| `cron_expr` | Informational cron expression string. |

Registration script `/opt/fjs/bin/register-jobs.sh`:

```bash
#!/usr/bin/env bash
# Synchronize job definition JSON files with the server.
# The filename without extension acts as the Job ID (e.g., SAL-030.json -> SAL-030).
set -euo pipefail
FJS=${FJS:-http://localhost:8080}
JOB_DIR=${1:-/opt/fjs/jobs}

for f in "$JOB_DIR"/*.json; do
  id=$(basename "$f" .json)
  if curl -fs -o /dev/null "$FJS/api/v1/jobs/detail?id=$id"; then
    method=PUT
  else
    method=POST
  fi
  curl -fsS -o /dev/null -X "$method" "$FJS/api/v1/jobs" \
    -H 'Content-Type: application/json' -d @"$f"
  echo "$method $id"
done
```

```
$ /opt/fjs/bin/register-jobs.sh
POST SAL-010
POST SAL-030
```

Updates via `PUT` **replace the job definition entirely with the provided payload**. Any omitted fields will be reset to empty defaults. Keeping all fields specified in your JSON files prevents accidental overwrites.

When editing a job via the console modal, non-modal fields like custom environment variables and timeouts are preserved. However, applying `register-jobs.sh` will subsequently overwrite console modifications with the file contents. If you manage jobs as code, make all changes in your JSON files.

### 5.3 How Commands are Executed

| Case | Execution Method | Example |
|---|---|---|
| **No arguments** provided and command contains **whitespace** | Invoked via `/bin/sh -c "<command>"` | `cd /data && ./run.sh > out.txt`<br>All shell operators (`;`, `&&`, `|`, `>`) are fully supported. |
| **No arguments** provided and command contains **no whitespace** | Executed directly as a binary/script | `/opt/batch/sales/load_sales.sh`<br>Requires executable permissions and a valid shebang (e.g. `#!/bin/bash`). |
| **Arguments are provided** | Executed directly without a shell, passing arguments to the command binary | `python3` with arguments `["/opt/app/main.py", "--full"]` |

**Recommendation:** Leave the Arguments field blank and place the entire command line into the Command field. This provides full shell capabilities without quoting ambiguities.

In the web console, the arguments input splits tokens by whitespace, treating double-quoted segments as single arguments (e.g., `-c "echo a; exit 0"` becomes `-c` and `echo a; exit 0`). If arguments must contain nested double quotes, configure them via JSON files over the REST API.

**Success and failure are determined solely by process exit code.** Exit code 0 denotes `SUCCESS`; any non-zero exit code denotes `FAILED`. Even if text matching "ERROR" appears in the logs, an exit code of 0 is treated as a success. Write scripts with strict error handling:

```bash
#!/usr/bin/env bash
set -euo pipefail          # Immediately exit with non-zero on any error

python3 /opt/app/transform.py "$@"     # If Python fails (sys.exit(1)), script stops here
row=$(psql -tAc "select count(*) from sales_daily")
if [ "$row" -eq 0 ]; then
  echo "Loaded row count is 0" >&2
  exit 2                   # Explicitly fail the job
fi
echo "Success: ${row} rows processed"
```

### 5.4 Dates are Not Passed to Scripts

The Order Date (ODATE) is **not injected into the script execution environment automatically**. Control-M-style macros such as `%%$ODATE` are not substituted and are passed as raw text.

Job scripts must resolve their target processing dates independently. It is best practice to allow overriding the target date via environment variables for manual runs and reruns:

```bash
# Default to processing "yesterday". Override manually via BASE_DATE=20260927 ./load_sales.sh
BASE_DATE=${BASE_DATE:-$(date -d yesterday +%Y%m%d)}
```

If past data must be reprocessed after the calendar date has advanced, triggering a standard Rerun will not change the target date inside the script. In this scenario, execute the script manually on the agent host with the explicit `BASE_DATE`, then use `Set OK` on the failed run in the web console.

### 5.5 Disabling and Deleting Jobs

- **Disable (`enabled: false`):** Excludes the job from batch ordering. It can still be executed on demand via `⚡ Trigger Now`, making this ideal for manual on-demand batch procedures.
- **Delete:** Deletes the job definition. Existing run records and historical log files remain intact. The job node is removed from the DAG map.
  - If you delete the job definition of a currently executing run, the run will **not generate out-conditions upon completion**. Only delete job definitions when all associated runs have finished.

---

## 6. Chaining Jobs (In-Conditions and Out-Conditions)

### 6.1 Basic Chaining

```
SAL-010 (Out: SAL-010-OK) ─┐
                           ├─▶ SAL-030 (In: SAL-010-OK, SAL-020-OK)
SAL-020 (Out: SAL-020-OK) ─┘
```

1. When `SAL-010` succeeds, `SAL-010-OK` is generated for the current ODATE. `SAL-030` remains in `WAIT` because `SAL-020-OK` is still missing.
2. The instant `SAL-020` completes and generates `SAL-020-OK`, `SAL-030` transitions to `READY` and begins execution immediately.

Condition names are **case-sensitive and sensitive to whitespace**. Follow these conventions:
- Use uppercase for all condition names. (The console's Conditions tab automatically converts input to uppercase; the drawer and API preserve casing as entered.)
- Name job completion conditions uniformly as `<JOB_ID>-OK`. This makes dependencies immediately recognizable when inspecting job definitions.
- Name conditions generated by manual operators or external systems descriptively (e.g., `FIN_APPROVE`, `PG_FILE_ARRIVED`).
- **Never enter an upstream Job Name in an in-condition field.** While an arrow may appear on the DAG canvas, unless a condition with that exact name is generated, downstream jobs will remain in `WAIT` indefinitely.

### 6.2 Cross-Group Chaining

Conditions are evaluated strictly by condition name and ODATE, regardless of job groups. A daily job in the `DAILY` group can seamlessly trigger a month-end job in the `MONTHEND` group (e.g., `SAL-900` in Section 9).

### 6.3 Creating Conditions via Human Operators or External Systems

**Via Web Console:**
- From the Conditions tab: Enter the condition name and click `+ Manually Create Condition`.
- From the DAG map: Select the waiting job in the drawer and click `+ Manual Satisfy` next to the missing condition.
- Both actions create the condition under the currently active **ODATE**.

**Via External Systems:**
Call the REST API at the end of an external pipeline (e.g., when a file transfer finishes):

```bash
curl -fsS -X POST http://batch-mgr01:8080/api/v1/conditions \
  -H 'Content-Type: application/json' \
  -d '{"name":"PG_FILE_ARRIVED","odate":"20260930"}'
```

If `odate` is omitted, the server's current calendar date is used. Posting the same condition multiple times is safe and idempotent.

### 6.4 Deleting Conditions

Deleting a condition **does not roll back or cancel runs that have already been released by it**. Deleting a condition only affects subsequent runs evaluated after the deletion. Condition deletion is intended for correcting misconfigured flags, not for stopping active jobs.

### 6.5 Important Considerations and Caveats

- **No Circular Dependency Detection at Runtime:** If Job A waits on B and Job B waits on A, both will remain in `WAIT` forever.
- **No OR Logic:** In-conditions are strictly evaluated as AND logic. "Run if either A or B finishes" cannot be expressed.
- **No Cross-ODATE Conditions:** Conditions cannot satisfy preconditions across different business dates. If today's job requires yesterday's close as a precondition, have the final job of the prior run explicitly create the next day's condition via the REST API.

---

## 7. Executing Jobs (Ordering)

Job definitions do not run on their own. They must be **ordered** to create executable run records. There are three ways to order jobs:

### 7.1 Batch Ordering

Batch ordering evaluates all active jobs for a given ODATE and instantiates runs for those **that have not yet been ordered for that date**.

- Jobs with no preconditions (or whose preconditions are already met) transition immediately to `READY` -> `RUNNING`.
- Jobs with unmet preconditions enter `WAIT` and execute progressively as upstream conditions are satisfied.
- Jobs that already have a run for that ODATE are skipped. **Triggering batch ordering multiple times will not duplicate existing runs.**
- Existing `WAIT` runs for that date are re-evaluated and released if conditions have since become available.
- Batch ordering events are logged in the audit trail under action `ORDER_PLAN` (with the ODATE as the target ID).

**Web Console:** Verify the ODATE input and click `📅 Order Daily Plan`. This orders all active jobs **across all groups without distinction** and reports the count of ordered runs.

**REST API:** Supports ordering a specific group. In production operations, always use the API:

```bash
curl -fsS -X POST http://batch-mgr01:8080/api/v1/runs/order \
  -H 'Content-Type: application/json' \
  -d '{"odate":"20260930","group":"DAILY","operator_id":"kim"}'
```

```json
{"odate":"20260930","ordered_count":6,"ready_count":2,"wait_count":4,"skipped_count":0,"success":true}
```

Ordering by group prevents special jobs (such as month-end procedures) from accidentally executing on standard weekdays. If you maintain distinct groups, **avoid using the console's top "Order Daily Plan" button**, as it orders all groups simultaneously.

### 7.2 Immediate Single Execution (Trigger)

Triggered via `⚡ Trigger Now` in the DAG drawer, `⚡ Run` in the Job Definitions tab, or the edit modal:

- Unconditionally instantiates a **new run** for the active ODATE, even if runs already exist for that date.
- Preconditions are still enforced. If preconditions are missing, the run enters `WAIT`.
- **Immediate Downstream Dependents** (active jobs possessing this job's out-conditions as in-conditions) that do not yet have a run for this ODATE are ordered concurrently into `WAIT`. **Dependents further down the chain are not ordered.** To execute an entire pipeline, use batch ordering.
- Disabled jobs can also be triggered this way.

### 7.3 Executing Immediately Upon Registration

Enabling `Run Once Immediately Upon Creation` in the web console (or setting `"run_now": true` via the API) orders the job for the active ODATE upon registration. Downstream dependent jobs are not ordered.

### 7.4 Automating Daily Ordering

FreeJobScheduler does not have an internal clock or cron daemon. Automation is achieved by invoking the batch ordering API from the management server's OS `crontab`. Handling calendar logic and holidays is covered in the next section.

---

## 8. Business Day Calendar Management

FreeJobScheduler does not contain an internal holiday calendar. Instead, business calendar operations are handled using three components: **a holiday text file**, **logical job groups**, and **an ordering shell script**.

### 8.1 Using Groups as Calendars

Categorize job definitions into groups based on their execution frequencies:

| Group | Ordering Frequency | Examples |
|---|---|---|
| `EVERYDAY` | Daily, including weekends and holidays | Database backups, log cleanup |
| `DAILY` | Business days only | Sales ETL, settlement processing |
| `WEEKLY` | First business day of each week | Weekly aggregation reports |
| `MONTHEND` | Last business day of each month | Monthly accounting close |

You can use any group naming convention; the automation script below uses these four names.

### 8.2 Holiday List File

Store holidays in `/opt/fjs/calendar/holidays-2026.txt`. Each line contains one date (`YYYYMMDD`), with optional comments following `#`. Maintain one file per year:

```
# 2026 Public Holidays (Sample excerpt; update annually based on official government calendars)
20260101  # New Year's Day
20260216  # Lunar New Year Holiday
20260217  # Lunar New Year's Day
20260218  # Lunar New Year Holiday
20260924  # Chuseok Holiday
20260925  # Chuseok
20260926  # Chuseok Holiday
20261005  # Alternative Holiday
20261009  # Hangul Day
20261225  # Christmas Day
```

Saturdays and Sundays are automatically treated as non-business days by the script. Only weekday public holidays and designated company holidays need to be listed.

### 8.3 Ordering Script

Save as `/opt/fjs/bin/order-today.sh`:

```bash
#!/usr/bin/env bash
# Orders batch job groups for today (or a specified ODATE).
# Usage: order-today.sh [YYYYMMDD]
# Dry run: DRY_RUN=1 order-today.sh 20261030
set -euo pipefail

FJS=${FJS:-http://localhost:8080}
CAL_DIR=${CAL_DIR:-/opt/fjs/calendar}
ODATE=${1:-$(date +%Y%m%d)}
DRY_RUN=${DRY_RUN:-0}

# Check if date is in holiday list
is_holiday() {
  awk -v d="$1" '$1 == d { found = 1 } END { exit !found }' "$CAL_DIR"/holidays-*.txt 2>/dev/null
}

# Check if date is Monday-Friday and not a holiday
is_business_day() {
  [ "$(date -d "$1" +%u)" -le 5 ] && ! is_holiday "$1"
}

# Calculate next (+1) or previous (-1) business day
shift_business_day() {
  local d=$1 step=$2
  d=$(date -d "$d $step day" +%Y%m%d)
  until is_business_day "$d"; do d=$(date -d "$d $step day" +%Y%m%d); done
  echo "$d"
}

order() {
  echo "$(date '+%F %T') ORDER ODATE=$ODATE GROUP=$1"
  [ "$DRY_RUN" = 1 ] && return 0
  curl -fsS -X POST "$FJS/api/v1/runs/order" \
    -H 'Content-Type: application/json' \
    -d "{\"odate\":\"$ODATE\",\"group\":\"$1\",\"operator_id\":\"cron\"}"
  echo
}

# 1. Everyday jobs (backups, log rotation, etc.)
order EVERYDAY

# 2. Business day jobs
if ! is_business_day "$ODATE"; then
  echo "$(date '+%F %T') $ODATE is a holiday. Skipping business-day job groups."
  exit 0
fi

order DAILY

# If first business day of the week (previous business day was in prior week)
if [ "$(date -d "$(shift_business_day "$ODATE" -1)" +%G%V)" != "$(date -d "$ODATE" +%G%V)" ]; then
  order WEEKLY
fi

# If last business day of the month (next business day is in next month)
if [ "$(shift_business_day "$ODATE" +1 | cut -c1-6)" != "${ODATE:0:6}" ]; then
  order MONTHEND
fi
```

Register in the crontab of the `fjs` service account on the management server (`crontab -e`):

```
# Order batch jobs daily at 00:05
5 0 * * * /opt/fjs/bin/order-today.sh >> /opt/fjs/logs/order.log 2>&1
```

### 8.4 Verification via Dry Run

After updating holiday files, test logic using `DRY_RUN=1` without sending API requests. Below is actual test output:

```
$ for d in 20260923 20260924 20260927 20260928 20260930 20261005 20261006; do
>   DRY_RUN=1 /opt/fjs/bin/order-today.sh $d; done
… ORDER ODATE=20260923 GROUP=EVERYDAY
… ORDER ODATE=20260923 GROUP=DAILY
… ORDER ODATE=20260924 GROUP=EVERYDAY
… 20260924 is a holiday. Skipping business-day job groups.      ← Chuseok holiday
… ORDER ODATE=20260927 GROUP=EVERYDAY
… 20260927 is a holiday. Skipping business-day job groups.      ← Sunday
… ORDER ODATE=20260928 GROUP=EVERYDAY
… ORDER ODATE=20260928 GROUP=DAILY
… ORDER ODATE=20260928 GROUP=WEEKLY                             ← First business day after holiday
… ORDER ODATE=20260930 GROUP=EVERYDAY
… ORDER ODATE=20260930 GROUP=DAILY
… ORDER ODATE=20260930 GROUP=MONTHEND                           ← Last business day of September
… ORDER ODATE=20261005 GROUP=EVERYDAY
… 20261005 is a holiday. Skipping business-day job groups.      ← Substitute holiday
… ORDER ODATE=20261006 GROUP=EVERYDAY
… ORDER ODATE=20261006 GROUP=DAILY
… ORDER ODATE=20261006 GROUP=WEEKLY                             ← Tuesday is first business day of week
```

### 8.5 Calendar Operational Scenarios

- **Ad-Hoc Substitute Holidays:** Append the date to `holidays-2026.txt` and verify with dry run:
  ```bash
  echo '20261002  # Ad-hoc Holiday' >> /opt/fjs/calendar/holidays-2026.txt
  DRY_RUN=1 /opt/fjs/bin/order-today.sh 20261002
  ```
- **Year-End Preparation:** Create `holidays-2027.txt` in December. If missing, January 1 will be calculated as a normal business day.
- **Executing Holiday Runs Manually:** Trigger the group directly via API bypassing the script:
  ```bash
  curl -fsS -X POST http://localhost:8080/api/v1/runs/order -H 'Content-Type: application/json' \
    -d '{"odate":"20260925","group":"DAILY","operator_id":"kim"}'
  ```
- **Recovering Missed Midnight Ordering:** Re-run the script with the desired date. Because already-ordered jobs are skipped, this is safe to run repeatedly:
  ```bash
  /opt/fjs/bin/order-today.sh 20261001
  ```

---

## 9. Real-World Case Study: E-Commerce Daily Sales Reconciliation

This scenario demonstrates migrating an e-commerce platform's nightly sales settlement pipeline to FreeJobScheduler using the architecture described in [Section 3.5](#35-production-deployment-example).

### 9.1 Business Workflow

1. Verify arrival of settlement files transmitted by the payment gateway (PG).
2. Extract the prior day's order transactions from the primary order database.
3. Once steps 1 and 2 finish, load the combined dataset into the data warehouse.
4. Perform financial reconciliation on the accounting server between PG settlement totals and order totals.
5. Upon review and sign-off by the finance team, generate accounting ledger vouchers.
6. On the final business day of each month, execute the monthly sales ledger close after voucher generation.
7. Independently, execute database backups every night including weekends and holidays.

### 9.2 Job Design

| ID | Name | Group | Labels | In-Conditions | Out-Conditions | Timeout |
|---|---|---|---|---|---|---|
| `SAL-010` | Check_PG_File_Arrival | `DAILY` | `etl` | None | `SAL-010-OK` | 3h (10800s) |
| `SAL-020` | Extract_Order_DB | `DAILY` | `etl` | None | `SAL-020-OK` | 1h (3600s) |
| `SAL-030` | Load_Sales_DW | `DAILY` | `etl` | `SAL-010-OK`, `SAL-020-OK` | `SAL-030-OK` | 2h (7200s) |
| `SAL-040` | Reconcile_Sales | `DAILY` | `fin` | `SAL-030-OK` | `SAL-040-OK` | 1h (3600s) |
| `SAL-050` | Generate_Voucher | `DAILY` | `fin` | `SAL-040-OK`, **`FIN_APPROVE`** | `SAL-050-OK` | 30m (1800s) |
| `SAL-900` | Month_End_Close | `MONTHEND` | `fin` | `SAL-050-OK` | `SAL-900-OK` | 1h (3600s) |
| `SYS-100` | Backup_Sales_DB | `EVERYDAY` | `etl` | None | None | 2h (7200s) |
| `SAL-990` | Manual_Reaggregation | `DAILY` (**Disabled**) | `etl` | None | None | 2h (7200s) |

```
SAL-010 (Check PG File) ─┐
                         ├─▶ SAL-030 (Load) ─▶ SAL-040 (Reconcile) ─┐
SAL-020 (Extract Orders) ┘                                          ├─▶ SAL-050 (Vouchers) ─▶ SAL-900 (Month Close)
                                                FIN_APPROVE (Manual)┘
SYS-100 (Backup, Everyday)     SAL-990 (Manual Reaggregation, Disabled)
```

- `FIN_APPROVE` is not produced by any automated job. It is satisfied manually by an operator when the finance team approves the reconciliation report.
- `SAL-990` is disabled, excluding it from batch orders. It is triggered manually via `⚡ Trigger Now` when data reprocessing is required.
- `SAL-900` belongs to `MONTHEND` and is ordered only on the month's final business day. When ordered, it waits for `SAL-050-OK`.

### 9.3 Creating Job Definition Files

Place JSON definition files in `/opt/fjs/jobs/`. Sample excerpts:

`SAL-010.json`:
```json
{
  "id": "SAL-010",
  "name": "Check_PG_File_Arrival",
  "group": "DAILY",
  "command": "/opt/batch/sales/wait_pg_file.sh",
  "agent_labels": ["etl"],
  "in_conditions": [],
  "out_conditions": ["SAL-010-OK"],
  "timeout_sec": 10800,
  "enabled": true
}
```

`SAL-050.json`:
```json
{
  "id": "SAL-050",
  "name": "Generate_Voucher",
  "group": "DAILY",
  "command": "/opt/batch/fin/make_voucher.sh",
  "agent_labels": ["fin"],
  "in_conditions": ["SAL-040-OK", "FIN_APPROVE"],
  "out_conditions": ["SAL-050-OK"],
  "timeout_sec": 1800,
  "enabled": true
}
```

Apply jobs using `/opt/fjs/bin/register-jobs.sh`.

`SAL-010` polling script on `etl01` (`/opt/batch/sales/wait_pg_file.sh`):

```bash
#!/usr/bin/env bash
set -euo pipefail
BASE_DATE=${BASE_DATE:-$(date -d yesterday +%Y%m%d)}
FILE=/data/pg/inbound/settle_${BASE_DATE}.csv

until [ -s "$FILE" ]; do
  echo "$(date +%T) Awaiting file: $FILE"
  sleep 60
done
echo "File verified: $FILE ($(wc -l < "$FILE") rows)"
```

### 9.4 Initial Test Run

Test using an unused historical date (e.g. `20260101`) to avoid affecting production records:

1. In the console, set ODATE to `20260101`.
2. Check the Agents tab to verify `etl01` and `fin01` are active.
3. Order the `DAILY` group via the API:
   ```bash
   curl -fsS -X POST http://batch-mgr01:8080/api/v1/runs/order -H 'Content-Type: application/json' \
     -d '{"odate":"20260101","group":"DAILY","operator_id":"kim"}'
   ```
4. Verify on the DAG map that `SAL-010` and `SAL-020` transition to `RUNNING` while others remain in `WAIT`.
5. When `SAL-050` pauses in `WAIT`, satisfy `FIN_APPROVE` via `+ Manual Satisfy` in the drawer.

### 9.5 A Typical Day in Operation (September 30, 2026, Month-End)

| Time | Event | Operator Responsibility |
|---|---|---|
| 00:05 | Crontab executes `order-today.sh` -> Orders `EVERYDAY`, `DAILY`, and `MONTHEND`. `SAL-010`, `SAL-020`, and `SYS-100` start running; others wait. | None |
| 00:40 | PG file arrives -> `SAL-010` succeeds -> `SAL-010-OK` emitted. | None |
| 00:55 | `SAL-020` succeeds -> Both preconditions met -> `SAL-030` starts. | None |
| 02:10 | `SAL-030` succeeds -> `SAL-040` starts on `fin01`. | None |
| 02:30 | `SAL-040` succeeds. `SAL-050` waits for `FIN_APPROVE` (`In: 1/2`). | None |
| 09:00 | Morning inspection: Verify ODATE `20260930`, check for 0 FAILED runs. | Verification |
| 09:10 | Finance team emails reconciliation approval. | Click `+ Manual Satisfy` next to `FIN_APPROVE` in `SAL-050` drawer. |
| 09:12 | `SAL-050` runs and succeeds -> `SAL-900` (month-end close) runs automatically. | Verify completion |

### 9.6 Incident Response & Operational Scenarios

#### Scenario 1: Load fails due to DB disconnect — Rerun

- At 02:05 `SAL-030` enters `FAILED` with exit code 2. Downstream jobs remain safely in `WAIT`.
- Inspecting `Log` reveals `connection reset by peer`. The DBA confirms `salesdb01` underwent an unscheduled reboot at 02:00.
- With the DB back online, click `Rerun` on `SAL-030`, input Operator ID and reason ("salesdb01 reboot at 02:00, re-running"), and confirm.
- A new run (`…-rerun-…`) executes immediately. Upon completion, `SAL-030-OK` is emitted and `SAL-040` begins automatically.

#### Scenario 2: Re-executing with modified command parameters — Job Definition Update + Trigger

- `SAL-040` fails due to a minor ledger discrepancy. Developers request running today's task with an additional tolerance flag: `--tolerance 100`.
- **Do not click Rerun.** Rerun copies the original command string embedded in the failed run record.
- Edit `command` in `SAL-040.json` and sync via `register-jobs.sh`.
- In the DAG drawer, click `⚡ Trigger Now` (verify ODATE). A new run executes with the updated command.
- Once finished, revert the JSON file and re-sync.

#### Scenario 3: Payment gateway file never arrives — Bypass

- A vendor outage prevents the PG settlement file from arriving; `SAL-010` times out after 3 hours (`FAILED`, exit code 124).
- Management decides to process orders without PG reconciliation for now, reaggregating once files arrive.
- Open `SAL-010` in the drawer, click `Bypass`, and enter the justification. `SAL-010-OK` is generated immediately, releasing `SAL-030`.
- When the vendor file arrives later, execute `SAL-990` (disabled manual task) via `⚡ Trigger Now`.

#### Scenario 4: Accounting server reboots — Resolving hung RUNNING jobs

- At 02:20, `fin01` crashes and reboots. `SAL-040` remains frozen in `RUNNING`.
- Because the agent connection dropped abruptly, the server has no means to receive the process exit status.
- Verify on `fin01` that the process is dead (`ps -ef | grep reconcile`).
- Click `Rerun` on `SAL-040` with justification ("fin01 reboot recovery").
- To clean up the orphaned original run from confusing the status bar count, click `Bypass` on the original run **only after the new rerun completes successfully**. (Performing Set OK or Bypass earlier would emit out-conditions prematurely).

#### Scenario 5: Chuseok Holiday Period

- With holidays listed for Sept 24–26, `order-today.sh` orders only `EVERYDAY` (backups) from Sept 24–27.
- On Monday, Sept 28, `DAILY` and `WEEKLY` are automatically ordered. The batch scripts handle processing multi-day accumulated holiday volumes.

#### Scenario 6: Scaling out ETL worker capacity

- If ETL jobs queue up in `READY` waiting for slots, launch an additional agent on a new host (`etl02`) with identical labels:
  ```bash
  fjs-agent -server ws://batch-mgr01:8080/ws/agent -id etl02 -labels linux,etl -token …
  ```
- No changes to job definitions are required. The server automatically load-balances ready tasks across available agent slots.

---

## 10. Troubleshooting and Incident Handling

### 10.1 Three Operator Actions

All manual actions require entering an **Operator ID and Reason**, which are permanently logged to the audit trail. The Operator ID relies on honest input (no authentication). Establish internal guidelines (e.g. employee ID or LDAP username).

| Attribute | Rerun | Set OK | Bypass |
|---|---|---|---|
| **Action** | Creates a new run record for the same ODATE and re-executes. | Forces the run state to `SUCCESS`. | Forces the run state to `BYPASS`. |
| **Applicable States** | Any state. | Any state except `SUCCESS` and `BYPASS`. | Any state except `SUCCESS` and `BYPASS`. |
| **Original Run** | Remains unchanged in history. | Transitioned to `SUCCESS`. | Transitioned to `BYPASS`. |
| **In-Conditions** | **Bypassed** (enters `READY` immediately). | N/A | N/A |
| **Out-Conditions** | Generated when the new run succeeds. | **Emitted immediately.** | **Emitted immediately.** |
| **Executed Command** | **Exact copy of original run's command.** | N/A | N/A |
| **Running Process** | Untouched (**both can run concurrently**). | Not terminated. | Not terminated. |

- In the Run Status table, the `Bypass` button is displayed only on `WAIT` and `READY` rows. For other states, trigger Bypass from the DAG drawer.
- Applying Set OK or Bypass to a `RUNNING` task releases downstream jobs immediately, but **the underlying process on the agent host continues to run**. Terminate the process on the agent first if necessary ([Section 10.3](#103-stopping-a-running-job)).
- Triggering Rerun on an already `SUCCESS` run does not re-execute downstream jobs, as their preconditions are already met. Downstream jobs must be rerun individually if needed.

### 10.2 Diagnosis by Symptom

#### Job Stuck in WAIT
1. Click the job in the DAG map and review `⏳ Pending` conditions.
2. Is the upstream job `FAILED`? -> Resolve the upstream failure.
3. Does a similar condition name exist in the Conditions tab? -> Check for typos, casing, or extra whitespace.
4. Does the ODATE match? Conditions generated under a different date will not satisfy today's preconditions.
5. Was the upstream job omitted from ordering? (e.g., belonging to a different group).

#### Job Stuck in READY
1. Check the Agents tab. Is an agent connected? If not, run `systemctl status fjs-agent` on the agent host.
2. Does any agent possess **all** required labels? If a job requires `["etl","gpu"]`, only agents with both labels qualify.
3. Are all matching agents' slots saturated? (Check `Used / Total Slots` in the Agents tab).
4. Verify the secret token. Token mismatches prevent agents from authenticating.

#### Job Stuck in ASSIGNED
- Normal transition takes less than a second. If an agent disconnects at that instant, the server automatically rolls the run back to `READY`.
- If the server restarted during dispatch, inspect the agent process table and log files. Re-sync via Rerun or Set OK.

#### Job Stuck in RUNNING
- Check the log modal. If log lines are actively increasing, the process is still legitimately running.
- If the agent disconnected or rebooted, refer to [Section 10.4](#104-when-an-agent-host-crashes).
- Check the timeout configuration. Jobs will eventually terminate with `FAILED` (exit code 124) when the timeout is reached.

#### Job Fails After Exactly 5 Minutes with Exit Code 124
- The run exceeded the default timeout of 300 seconds. Increase `timeout_sec` via the REST API or JSON definition ([Section 5.2](#52-registration-via-api-and-json-files-recommended-for-production)).

#### Empty Logs for a Running Job
- The executing binary is buffering standard output. In Python, run with unbuffered output (`python3 -u`); for general binaries, wrap execution with `stdbuf -oL`.

#### Ordered Against the Wrong ODATE
- If jobs were ordered with an incorrect ODATE, bypass any pending `WAIT` and `READY` runs under that date, then order against the correct ODATE. If jobs have already begun executing, terminate them manually on the agent host ([Section 10.3](#103-stopping-a-running-job)).

### 10.3 Stopping a Running Job

The web console does not have a process termination button. Terminate jobs directly on the target agent host.
The agent assigns a **unique process group (PGID)** to every job run. Terminating the process group ensures all child and grandchild processes are cleanly killed.

```bash
# 1. Identify PID and Process Group ID (PGID)
ps -eo pid,pgid,etime,args | grep '[l]oad_sales'
#  41230  41230  01:12:09 /bin/bash /opt/batch/sales/load_sales.sh

# 2. Terminate the entire process group (prefix PGID with a negative sign)
kill -TERM -- -41230
```

If the agent is alive, it immediately reports `FAILED` with exit code `-1`, preserving all stdout/stderr generated up to that moment. You can then apply Rerun, Set OK, or Bypass.

### 10.4 When an Agent Host Crashes

| State at Time of Crash | Outcome |
|---|---|
| `ASSIGNED` | Detected by server heartbeat; run reverts to `READY` to be dispatched to another agent. |
| `RUNNING` | **Remains frozen in RUNNING.** The server cannot automatically determine the final process outcome. |

The server sends ping frames every 30 seconds and marks an agent disconnected if no response is received within 90 seconds. If only the agent process dies (not the machine), the WebSocket disconnect is detected immediately.

To recover runs frozen in `RUNNING`:
1. Check if the script process survived on the worker host. (If only the agent daemon died, the script may still be executing).
2. If running, wait for completion and inspect the output, or kill it manually ([Section 10.3](#103-stopping-a-running-job)). A restarted agent will not track previously orphaned processes.
3. If verified successful, apply **Set OK**; if it must run again, apply **Rerun**.

If network connectivity dropped temporarily while the agent stayed online, execution continues normally. If the script finishes after reconnection, results are reported as normal. However, if it finished while disconnected, results are lost and the run must be reconciled manually as above.

### 10.5 When the Server Restarts

- Job definitions, run histories, and conditions remain intact in SQLite.
- Runs in `WAIT` resume condition monitoring; if preconditions were fulfilled while the server was offline, they release immediately.
- Agents reconnect automatically within seconds.
- Runs that finished while the server was down remain in `RUNNING`. Check active runs after server restarts.

---

## 11. API Reference

All endpoints are hosted at `http://<server>:8080` and require **no authentication**.
Request and response bodies are JSON. On error, endpoints return 4xx/5xx status codes with a **plain text** error message.
If `odate` or `date` is omitted, the **server's current calendar date** is used.

### 11.1 Endpoint List

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/jobs?group=DAILY` | List job definitions (all groups if `group` is omitted). |
| `GET` | `/api/v1/jobs/detail?id=SAL-030` | Retrieve a single job definition. Returns 404 if not found. |
| `POST` | `/api/v1/jobs` | Create a job definition. Payload per [Section 5.2](#52-registration-via-api-and-json-files-recommended-for-production). Include `"run_now": true, "odate": "…"` to trigger immediately. Always registered as enabled. |
| `PUT` | `/api/v1/jobs` | Update a job definition. `id` required. **Replaces the definition completely.** |
| `DELETE` | `/api/v1/jobs?id=SAL-030` | Delete a job definition. |
| `POST` | `/api/v1/jobs/trigger` | Immediate single trigger: `{"job_id":"SAL-030","odate":"20260930"}`. |
| `POST` | `/api/v1/runs/order` | Batch order: `{"odate":"20260930","group":"DAILY","operator_id":"cron"}` (`group` optional). |
| `GET` | `/api/v1/runs?date=20260930` | List runs for an ODATE (newest first). |
| `GET` | `/api/v1/runs/logs?run_id=…` | Retrieve run details and last 1,000 log lines. |
| `POST` | `/api/v1/runs/action` | Perform action: `{"action":"RERUN","run_id":"…","operator_id":"kim","reason":"…"}`. Valid actions: `RERUN`, `SET_OK`, `BYPASS`. |
| `GET` | `/api/v1/conditions?date=20260930` | List conditions for an ODATE. |
| `POST` | `/api/v1/conditions` | Create a condition: `{"name":"FIN_APPROVE","odate":"20260930"}`. |
| `DELETE` | `/api/v1/conditions?name=FIN_APPROVE&date=20260930` | Delete a condition. |
| `GET` | `/api/v1/audits?target_id=…` | Audit records. Filter by `target_id` or retrieve latest 500 records. |
| `GET` | `/api/v1/agents` | List connected agents and active slot metrics. |
| WebSocket | `/ws/logs?run_id=…` | Stream real-time logs for a running execution. |
| WebSocket | `/ws/agent` | Agent communication endpoint. |

### 11.2 Common API Examples

```bash
FJS=http://batch-mgr01:8080

# List today's failed runs
curl -s "$FJS/api/v1/runs?date=$(date +%Y%m%d)" \
  | python3 -c 'import json,sys; [print(r["run_id"], r["job_name"], r["exit_code"]) for r in json.load(sys.stdin) or [] if r["state"]=="FAILED"]'

# View logs for a run
curl -s "$FJS/api/v1/runs/logs?run_id=run-SAL-030-91780a9f" \
  | python3 -c 'import json,sys; [print(c["content"]) for c in json.load(sys.stdin)["logs"] or []]'

# Trigger a Rerun
curl -s -X POST "$FJS/api/v1/runs/action" -H 'Content-Type: application/json' \
  -d '{"action":"RERUN","run_id":"run-SAL-030-91780a9f","operator_id":"kim","reason":"Database rebooted"}'

# View audit history for a run
curl -s "$FJS/api/v1/audits?target_id=run-SAL-030-91780a9f"

# View audit history for September 30 batch ordering
curl -s "$FJS/api/v1/audits?target_id=20260930"
```

Primary fields in the `/runs` response payload: `run_id`, `job_def_id`, `job_name`, `state`, `agent_id`, `command`, `args`, `env`, `exit_code`, `scheduled_at`, `started_at`, `finished_at`, `created_date`, and `error_message`.

---

## 12. Operational Checklist

### 12.1 Initial Deployment

- [ ] Changed `-token` on the server and all agents from default values.
- [ ] Restricted port 8080 strictly to private networks (operators, agents, automation scripts). Anyone with network access can trigger or modify jobs.
- [ ] Specified absolute paths for `-db` and `-logdir`.
- [ ] Launched agents under dedicated service accounts with necessary `PATH` and environment variables.
- [ ] Confirmed unique `-id` strings for every agent instance.
- [ ] Configured explicit `timeout_sec` on all jobs (default is 5 minutes).
- [ ] Validated holiday files and `order-today.sh` using `DRY_RUN=1`.
- [ ] Configured ordering cron jobs.
- [ ] Configured database backup, log cleanup, and alerting cron jobs (detailed below).

### 12.2 Backup and Maintenance

Historical run records and log files are not pruned automatically.

```
# Crontab for fjs service account
# SQLite hot backup (safe while server is active)
30 5 * * * sqlite3 /opt/fjs/data/fjs.db ".backup '/backup/fjs/fjs-$(date +\%Y\%m\%d).db'"

# Purge job execution logs older than 90 days
0 6 * * * find /opt/fjs/logs/runs -name '*.log' -mtime +90 -delete
```

If the `sqlite3` CLI is unavailable, stop the server and copy `fjs.db`, `fjs.db-wal`, and `fjs.db-shm` together.
To restore: Stop the server, replace `fjs.db` with the backup file, delete any lingering `-wal` and `-shm` files, and start the server.

### 12.3 Failure Alerting Script

Because native alerting is not built in, deploy a monitoring script to inspect the API periodically. The script inspects the **most recent run** for each job, ignoring resolved failures:

`/opt/fjs/bin/check-failed.sh`:

```bash
#!/usr/bin/env bash
# Checks for jobs whose latest run is FAILED. Emits alerts if any are detected.
set -euo pipefail
FJS=${FJS:-http://localhost:8080}
ODATE=${1:-$(date +%Y%m%d)}

failed=$(curl -fsS "$FJS/api/v1/runs?date=$ODATE" | python3 -c '
import json, sys
seen = set()
for r in json.load(sys.stdin) or []:          # Runs are ordered newest first
    if r["job_def_id"] in seen:
        continue
    seen.add(r["job_def_id"])
    if r["state"] == "FAILED":
        print(r["job_name"], r["run_id"], "Exit Code:", r["exit_code"])
')

if [ -n "$failed" ]; then
  # Insert notification command here (e.g. mail, Slack webhook)
  # curl -X POST -H "Content-Type: application/json" -d "{\"text\":\"$failed\"}" https://hooks.slack.com/...
  echo "$failed"
fi
```

```
*/5 * * * * /opt/fjs/bin/check-failed.sh >> /opt/fjs/logs/check-failed.log 2>&1
```

### 12.4 Daily Morning Inspection

1. Verify the console ODATE matches today's processing date.
2. Review the status bar for `FAILED` runs, long-running `RUNNING` tasks, or unexpected `WAIT` states.
3. Check the Agents tab to verify all agents are online.
4. Fulfill any manual conditions awaiting operator approval (e.g. `FIN_APPROVE`).

---

## 13. Developer Guide

### 13.1 Codebase Architecture

| Directory | Description |
|---|---|
| `cmd/server` | Server entrypoint: Flag parsing, database initialization, component wiring, condition cache cleanup (hourly for dates older than 7 days). |
| `cmd/agent` | Agent entrypoint. |
| `cmd/seed` | Seed data generator (wipes database). |
| `pkg/storage` | Domain models, run state transition rules (`IsValidStateTransition`), storage interfaces. |
| `pkg/storage/sqlstore` | SQLite storage implementation and `schema.sql`. |
| `pkg/storage/storetest` | In-memory SQLite test harnesses. |
| `pkg/protocol` | Server-Agent WebSocket wire protocol messages. |
| `server/engine/runs` | Run lifecycle engine: Ordering, condition evaluation, operator actions, agent report processing. All state changes flow through here. |
| `server/engine/condition` | In-memory inverted condition index mapping conditions to waiting runs. |
| `server/dispatcher` | Dispatches ready runs to agent slots, manages log ring buffers, log streaming. |
| `server/agenthub` | Manages agent WebSocket sessions, token authentication, slot capacity, heartbeats. |
| `server/webapi` | REST API routing and log streaming WebSocket handlers. |
| `agent/client` | Agent connection, reconnect backoff, message dispatch. |
| `agent/executor` | Process group execution, timeouts, stdout/stderr capture. |
| `web/` | Web console single-page application (HTML/JS/CSS). Embedded into the server binary. |
| `server/engine/scheduler` | Business calendar and date variable code. *(Present in tests; not hooked into server runtime).* |
| `server/engine/condition/cycle.go` | Cycle detection logic. *(Not invoked at runtime).* |
| `docs/` | Historical design and planning records. |

### 13.2 Architecture & Flow at a Glance

```
Order (API) ─▶ runs.Service.OrderRun ─▶ Saved to DB as WAIT
                  │
                  ├─ Preconditions satisfied ─▶ Updated to READY, awakens dispatcher
                  └─ Preconditions missing   ─▶ Registered in condition index
dispatcher ─▶ Fetches READY runs (FIFO) ─▶ Acquires matching agent slot ─▶ ASSIGNED ─▶ Sent to agent
Agent ─▶ Reports RUNNING ─▶ Streams output lines ─▶ Reports completion (exit code)
runs.Service ─▶ If SUCCESS, stores out-conditions ─▶ Finds dependent runs in index ─▶ Transitions to READY
```

### 13.3 Building and Testing

```bash
go build ./...
go test ./...
```

- `test/integration`: End-to-end integration tests spawning test servers and agents.
- `test/benchmark`: High-volume condition resolution performance tests.

---

## 14. Current Limitations and Known Issues

### 14.1 Missing Features

- **No Built-in Cron:** Automated ordering must be handled via OS `crontab`.
- **No Built-in Holiday Calendars:** Handled via external shell scripts.
- **No Date Macro Substitution:** Dates are not substituted in command strings.
- **No UI Kill Action:** Running processes must be terminated via CLI on worker hosts.
- **No Hold / Release:** Unsupported.
- **No OR Preconditions & Cross-ODATE Conditions:** AND evaluation only; conditions exist only within their ODATE.
- **No Native Alerting:** Implement external polling scripts.
- **No Authentication / RBAC:** The API and web console are open.
- **Linux/Unix Only:** Windows agents are not supported.
- **SQLite Only:** SQLite driver is embedded.
- **Execution User and Directory:** Inherited from the agent process; cannot be configured per job.

### 14.2 Operational Behaviors to Note

- Command strings and arguments are snapshotted into runs upon ordering; Reruns execute the original command even if the job definition has been modified.
- Triggering an immediate execution orders only immediate downstream children, not the entire downstream lineage.
- If an agent host crashes, runs in `RUNNING` will not recover automatically.
- The `error_message` field records only one-line system errors (`executor: task execution timed out`, `exit status 3`). Detailed errors must be read from the execution log.
- Duplicate job names are permitted.

### 14.3 Known Web Console Issues

| Issue | Impact | Workaround |
|---|---|---|
| Top `📅 Order Daily Plan` button orders all groups | Month-end and weekly jobs are ordered unintentionally on standard days | In multi-group setups, always order via API or `order-today.sh` |
