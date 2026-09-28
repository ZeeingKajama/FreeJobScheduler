# FreeJobScheduler

**Control-M에서 영감을 받은 경량형 오픈소스 작업 스케줄러**

여러 서버에 흩어진 배치 스크립트를 한곳에서 정해진 순서대로 돌리고, 웹 콘솔에서 흐름과 로그를 지켜보는 도구입니다.
"A가 끝나면 B를 돌리고, B와 C가 둘 다 끝나면 D를 돌린다" 같은 선후행 흐름을 등록해 두면, 앞 작업이 성공하는 순간 다음 작업이 알아서 시작됩니다.

> *A lightweight, open-source batch job scheduler inspired by enterprise workload automation tools.
> One Go server, one Go agent per host, SQLite storage, condition-based job chaining, and a built-in web console.*

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Go](https://img.shields.io/badge/Go-1.27%2B-00ADD8?logo=go)

---

## 특징

- **단순한 배포** — 서버 실행 파일 하나, 에이전트 실행 파일 하나. 웹 콘솔과 DB 스키마가 실행 파일에 내장되어 있고, 저장소는 SQLite 파일 하나입니다.
- **조건 기반 선후행 연결** — 작업이 성공하면 *후행 조건*을 만들고, 그 조건을 *선행 조건*으로 가진 작업이 즉시 풀려 실행됩니다. 그룹이 달라도 연결됩니다.
- **업무일자(ODATE)** — 같은 작업을 날짜마다 따로 발주·실행·기록합니다. 조건도 업무일자별로 관리됩니다.
- **에이전트 역방향 접속** — 에이전트가 서버에 WebSocket으로 먼저 접속하므로, 서버에서 작업 서버로 들어가는 방화벽을 열 필요가 없습니다.
- **라벨 배정과 동시 실행 제한** — "이 작업은 회계 서버에서만" 같은 배정, 에이전트별 슬롯 수 제한.
- **시간 제한** — 제한을 넘기면 프로세스 그룹째 종료하고 실패 처리합니다.
- **실시간 로그** — 실행 중인 작업의 출력을 콘솔에서 바로 보고, 끝난 뒤에도 파일로 보관합니다.
- **운영자 조치와 감사 기록** — 재실행(Rerun), 강제 성공(Set OK), 건너뛰기(Bypass). 누가 언제 왜 했는지 남습니다.
- **흐름도(DAG)** — 작업 간 연결과 상태를 그림으로 보여 줍니다.
- **REST API** — 콘솔에서 하는 일은 모두 API로도 할 수 있어 스크립트·CI와 붙이기 쉽습니다.

## 구성

```
 ┌──────────── 브라우저 (웹 콘솔) ────────────┐
 └──────────────────┬─────────────────────────┘
                    │ HTTP (기본 8080)
 ┌──────────────────▼─────────────────────────┐
 │  fjs-server                                │
 │   REST API · 웹 콘솔 · 스케줄링 · SQLite   │
 └───────┬───────────────────────┬────────────┘
         │ WebSocket (/ws/agent) │
 ┌───────▼────────┐      ┌───────▼────────┐
 │ fjs-agent      │      │ fjs-agent      │   ← 작업을 실제로 실행할 서버마다 하나씩
 │ labels: etl    │      │ labels: fin    │
 └────────────────┘      └────────────────┘
```

## 빠른 시작

Go 1.27.1 이상이 필요합니다(빌드할 때만).

```bash
git clone https://github.com/ZeeingKajama/FreeJobScheduler.git
cd FreeJobScheduler

go build -o bin/fjs-server ./cmd/server
go build -o bin/fjs-agent  ./cmd/agent

./bin/fjs-server &        # http://localhost:8080 , DB는 ./fjs.db
./bin/fjs-agent  &        # 라벨 linux,batch 로 서버에 접속
```

브라우저에서 `http://localhost:8080`을 열면 웹 콘솔이 나옵니다.

예제 작업 12개로 화면을 채워 보려면(서버를 한 번 띄워 DB를 만든 뒤):

```bash
go run ./cmd/seed fjs.db   # ⚠ DB의 기존 데이터를 모두 지웁니다. 운영 DB에 쓰지 마세요
```

### 작업 등록과 발주 예

```bash
# 작업 두 개를 선후행으로 연결해서 등록
curl -fsS -X POST http://localhost:8080/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "id": "EXTRACT", "name": "추출", "group": "DAILY",
  "command": "/opt/batch/extract.sh",
  "out_conditions": ["EXTRACT-OK"], "timeout_sec": 3600, "enabled": true }'

curl -fsS -X POST http://localhost:8080/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "id": "LOAD", "name": "적재", "group": "DAILY",
  "command": "/opt/batch/load.sh",
  "in_conditions": ["EXTRACT-OK"], "timeout_sec": 3600, "enabled": true }'

# 2026-09-30 업무일자로 DAILY 그룹 발주 → EXTRACT가 성공하면 LOAD가 자동으로 시작
curl -fsS -X POST http://localhost:8080/api/v1/runs/order -H 'Content-Type: application/json' \
  -d '{"odate":"20260930","group":"DAILY","operator_id":"admin"}'
```

## 실행 옵션

| 서버 (`fjs-server`) | 기본값 | 설명 |
|---|---|---|
| `-port` | `8080` | HTTP / WebSocket 포트 |
| `-db` | `fjs.db` | SQLite 파일 경로 (없으면 생성) |
| `-logdir` | `logs/runs` | 작업 로그 파일 폴더 |
| `-token` | `fjs-secret-token` | 에이전트 접속 토큰. **운영에서는 반드시 변경** |

| 에이전트 (`fjs-agent`) | 기본값 | 설명 |
|---|---|---|
| `-server` | `ws://localhost:8080/ws/agent` | 서버 주소 |
| `-id` | `agent-<호스트 이름>` | 에이전트 ID |
| `-labels` | `linux,batch` | 쉼표로 구분한 라벨 |
| `-token` | `fjs-secret-token` | 서버의 `-token`과 같아야 함 |
| `-max-concurrency` | `0` (서버 기본 10) | 동시에 돌릴 작업 수 |

## API 요약

| 메서드 | 경로 | 설명 |
|---|---|---|
| `GET` / `POST` / `PUT` / `DELETE` | `/api/v1/jobs` | 작업 정의 조회·등록·수정·삭제 |
| `GET` | `/api/v1/jobs/detail?id=` | 작업 정의 하나 |
| `POST` | `/api/v1/jobs/trigger` | 작업 하나 즉시 발주 |
| `POST` | `/api/v1/runs/order` | 업무일자(·그룹) 단위 일괄 발주 |
| `GET` | `/api/v1/runs?date=` | 업무일자별 실행 건 |
| `GET` | `/api/v1/runs/logs?run_id=` | 실행 로그 |
| `POST` | `/api/v1/runs/action` | `RERUN` / `SET_OK` / `BYPASS` |
| `GET` / `POST` / `DELETE` | `/api/v1/conditions` | 조건 조회·수동 발행·삭제 |
| `GET` | `/api/v1/audits` | 감사 기록 |
| `GET` | `/api/v1/agents` | 접속 중인 에이전트 |
| `GET` | `/ws/logs` | 실시간 로그 WebSocket |

자세한 요청·응답 형식은 [사용 설명서 11장](USER_MANUAL.md#11-api-정리)을 보세요.

## 현재 한계

지금 버전은 작고 단순한 것을 우선했습니다. 운영에 쓰기 전에 아래 내용을 꼭 확인하세요.

- **인증·권한이 없습니다.** 웹 콘솔과 REST API에 로그인이 없으므로 신뢰할 수 있는 내부망에서만 접근하게 막아야 합니다.
- **시각 기반 자동 실행(크론)이 없습니다.** 작업의 스케줄 칸은 메모용입니다. OS `crontab`에서 발주 API를 부르는 방식으로 운영합니다.
- **휴일 캘린더가 없습니다.** 휴일 목록 파일 + 발주 스크립트로 처리하는 방법을 설명서에 담았습니다.
- 선행 조건은 AND만 지원하며, 같은 업무일자 안에서만 통합니다.
- 작업 실행은 리눅스·유닉스 계열 에이전트에서만 됩니다.
- 저장소는 SQLite만 지원합니다.

전체 목록과 우회 방법은 [사용 설명서 14장](USER_MANUAL.md#14-지금-버전의-한계와-알려진-문제)에 있습니다.

## 문서

- [USER_MANUAL.md](USER_MANUAL.md) — 설치, 운영, 캘린더, 장애 대응, API, 소스 구조까지 담은 사용 설명서 (현재 동작 기준 문서)
- [docs/](docs/) — 설계·계획 기록. 현재 동작과 다른 내용이 있을 수 있습니다

## 개발

```bash
go build ./...
go test ./...
```

| 경로 | 내용 |
|---|---|
| `cmd/server`, `cmd/agent`, `cmd/seed` | 서버·에이전트·예제 데이터 생성기 시작점 |
| `server/engine/runs` | 실행 건 생명주기(발주, 조건 대기·해제, 운영자 조치) |
| `server/engine/condition` | 조건 색인과 캐시 |
| `server/dispatcher` | 에이전트 배정, 로그 저장·스트리밍 |
| `server/agenthub` | 에이전트 WebSocket 접속·슬롯 관리 |
| `server/webapi` | REST API |
| `agent/` | 에이전트 접속과 프로세스 실행 |
| `pkg/storage` | 도메인 타입, 상태 전이 규칙, SQLite 저장소 |
| `web/` | 웹 콘솔 (실행 파일에 내장) |

## 가져다 쓰기

마음껏 가져다가 고쳐쓰세요.

## 라이선스

[MIT License](LICENSE)로 공개합니다. 상업적 사용을 포함해 자유롭게 쓰고, 고치고, 배포할 수 있습니다.

## 상표 고지

FreeJobScheduler는 독립적인 오픈소스 프로젝트이며, BMC Software, Inc.와 아무런 제휴·후원·보증 관계가 없습니다.
Control-M은 BMC Software, Inc.의 상표 또는 등록 상표이며, 이 문서에서는 설계 배경을 설명하기 위해서만 언급합니다.
FreeJobScheduler는 Control-M의 소스 코드, 문서, 화면 디자인을 사용하지 않았으며, Control-M과의 호환성을 보장하지 않습니다.
