package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage"
	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage/sqlstore"
	_ "modernc.org/sqlite"
)

func main() {
	dbPath := "fjs.db"
	if len(os.Args) > 1 {
		dbPath = os.Args[1]
	}

	db, err := sql.Open("sqlite", fmt.Sprintf("%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)", dbPath))
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// 1. Clean up all existing tables
	fmt.Println(">> Cleaning up existing database tables...")
	_, _ = db.ExecContext(ctx, "DELETE FROM FJS_JOB_RUN;")
	_, _ = db.ExecContext(ctx, "DELETE FROM FJS_JOB_DEF;")
	_, _ = db.ExecContext(ctx, "DELETE FROM FJS_CONDITION;")
	_, _ = db.ExecContext(ctx, "DELETE FROM FJS_AUDIT;")
	fmt.Println(">> Existing records purged successfully.")

	store := sqlstore.NewSQLStore(db)
	today := time.Now().Format("20060102")
	now := time.Now()

	// 2. Define Demo Jobs
	type DemoJobConfig struct {
		ID            string
		Name          string
		Group         string
		CronExpr      string
		Command       string
		InConditions  []string
		OutConditions []string
		InitialState  storage.RunState
		OffsetMinutes int
		ExitCode      int
		ErrorMsg      string
	}

	demoJobs := []DemoJobConfig{
		// -------------------------------------------------------------
		// [FINANCE 그룹] 코어 금융 일마감 & 다이아몬드 DAG
		// -------------------------------------------------------------
		{
			ID:            "def-finance-01",
			Name:          "1.원장_거래내역_추출",
			Group:         "FINANCE",
			Command:       "echo '[EOD] 당일 전표 1,248,500건 원장 추출 완료' && sleep 1",
			OutConditions: []string{"TRAN_EXTRACT_OK"},
			InitialState:  storage.StateSuccess,
			OffsetMinutes: -15,
			ExitCode:      0,
		},
		{
			ID:            "def-finance-02",
			Name:          "2.대차대조표_대사검증",
			Group:         "FINANCE",
			Command:       "echo '[RECONCILE] 차변/대변 100% 일치 확인' && sleep 1",
			InConditions:  []string{"TRAN_EXTRACT_OK"},
			OutConditions: []string{"LEDGER_OK"},
			InitialState:  storage.StateSuccess,
			OffsetMinutes: -10,
			ExitCode:      0,
		},
		{
			ID:            "def-finance-03",
			Name:          "3.부가세_원천세_자동산출",
			Group:         "FINANCE",
			Command:       "echo '[TAX] 법인/개인 원천징수 세액 산정 완료' && sleep 1",
			InConditions:  []string{"TRAN_EXTRACT_OK"},
			OutConditions: []string{"TAX_CALC_OK"},
			InitialState:  storage.StateSuccess,
			OffsetMinutes: -10,
			ExitCode:      0,
		},
		{
			ID:            "def-finance-04",
			Name:          "4.일마감_회계원장_클로징",
			Group:         "FINANCE",
			Command:       "echo '[SETTLE] 2026-09-16 영업일 회계원장 공식 클로징 승인' && sleep 2",
			InConditions:  []string{"LEDGER_OK", "TAX_CALC_OK"},
			OutConditions: []string{"EOD_SETTLE_DONE"},
			InitialState:  storage.StateReady, // 조건 만족되어 발주 대기
			OffsetMinutes: -2,
			ExitCode:      -1,
		},

		// -------------------------------------------------------------
		// [BIGDATA 그룹] DW 및 AI 피처 엔지니어링 파이프라인
		// -------------------------------------------------------------
		{
			ID:            "def-dw-01",
			Name:          "10.Kafka_CDC_스냅샷적재",
			Group:         "BIGDATA",
			Command:       "echo '[CDC] 58만건 변경 로그 파켓(Parquet) 파일 적재 완료'",
			OutConditions: []string{"CDC_INGEST_OK"},
			InitialState:  storage.StateSuccess,
			OffsetMinutes: -20,
			ExitCode:      0,
		},
		{
			ID:            "def-dw-02",
			Name:          "11.고객이탈_ML피처_매트릭스",
			Group:         "BIGDATA",
			Command:       "echo '[AI/ML] 고객 80만명 대상 RFM 피처 128차원 벡터 계산 완료'",
			InConditions:  []string{"CDC_INGEST_OK"},
			OutConditions: []string{"CHURN_FEATURE_OK"},
			InitialState:  storage.StateWait, // 선행 조건 대기 상태 예시
			OffsetMinutes: -1,
			ExitCode:      -1,
		},
		{
			ID:            "def-dw-03",
			Name:          "12.고위험군_이탈추론_배치",
			Group:         "BIGDATA",
			Command:       "echo '[INFERENCE] 고위험 이탈군 12,400명 마케팅 CRM 연동 완료'",
			InConditions:  []string{"CHURN_FEATURE_OK"},
			OutConditions: []string{"CHURN_INFERENCE_DONE"},
			InitialState:  storage.StateWait,
			OffsetMinutes: 0,
			ExitCode:      -1,
		},

		// -------------------------------------------------------------
		// [RISK 그룹] 장마감 시장 리스크 평가 파이프라인
		// -------------------------------------------------------------
		{
			ID:            "def-risk-01",
			Name:          "20.글로벌_환율_시세_수집",
			Group:         "RISK",
			Command:       "echo '[FEED] KOSPI, S&P500, USD/KRW 실시간 종가 파싱 완료'",
			OutConditions: []string{"MARKET_FEED_OK"},
			InitialState:  storage.StateSuccess,
			OffsetMinutes: -8,
			ExitCode:      0,
		},
		{
			ID:            "def-risk-02",
			Name:          "21.포트폴리오_VaR_99_평가",
			Group:         "RISK",
			Command:       "echo '[VaR] 포트폴리오 일일 최대 손실한도(VaR) 정상 범위 통과'",
			InConditions:  []string{"MARKET_FEED_OK"},
			OutConditions: []string{"RISK_EVAL_OK"},
			InitialState:  storage.StateReady, // 조건 만족되어 발주 대기
			OffsetMinutes: -3,
			ExitCode:      -1,
		},

		// -------------------------------------------------------------
		// [INFRA 그룹] 시스템 인프라 정기 점검 및 백업
		// -------------------------------------------------------------
		{
			ID:            "def-infra-01",
			Name:          "30.디스크_임시로그_압축소거",
			Group:         "INFRA",
			CronExpr:      "0 3 * * *",
			Command:       "echo '[CLEANUP] /var/log 디스크 여유공간 84% 확보'",
			InitialState:  storage.StateSuccess,
			OffsetMinutes: -60,
			ExitCode:      0,
		},
		{
			ID:            "def-infra-02",
			Name:          "31.데이터베이스_온라인풀백업",
			Group:         "INFRA",
			CronExpr:      "0 4 * * *",
			Command:       "echo '[BACKUP] fjs_20260916_full.bak 생성 및 원격 S3 미러링 완료'",
			InitialState:  storage.StateSuccess,
			OffsetMinutes: -45,
			ExitCode:      0,
		},
		{
			ID:            "def-infra-03",
			Name:          "32.이상징후_보안_무결성스캔",
			Group:         "INFRA",
			CronExpr:      "0 * * * *",
			Command:       "echo '[SECURITY] 비인가 프로세스 및 취약점 점검 완료'",
			InitialState:  storage.StateReady,
			OffsetMinutes: -5,
			ExitCode:      -1,
		},
	}

	fmt.Println(">> Inserting demo job definitions and runtime runs...")

	for _, dj := range demoJobs {
		// 1. Create JobDef
		def := &storage.JobDef{
			ID:            dj.ID,
			Name:          dj.Name,
			Group:         dj.Group,
			CronExpr:      dj.CronExpr,
			Command:       dj.Command,
			AgentLabels:   []string{"linux"},
			InConditions:  dj.InConditions,
			OutConditions: dj.OutConditions,
			TimeoutSec:    300,
			Enabled:       true,
		}
		if err := store.JobDef().Create(ctx, def); err != nil {
			log.Fatalf("failed to create JobDef %s: %v", dj.ID, err)
		}

		// 2. Create JobRun
		schedTime := now.Add(time.Duration(dj.OffsetMinutes) * time.Minute)
		var startedTime, finishedTime *time.Time
		if dj.InitialState == storage.StateSuccess {
			st := schedTime.Add(1 * time.Second)
			ft := schedTime.Add(3 * time.Second)
			startedTime = &st
			finishedTime = &ft
		}

		runID := fmt.Sprintf("run-%s-%s", dj.ID, schedTime.Format("150405"))
		run := &storage.JobRun{
			RunID:        runID,
			JobDefID:     dj.ID,
			JobName:      dj.Name,
			State:        dj.InitialState,
			Command:      dj.Command,
			AgentID:      "agent-prod-01",
			ExitCode:     dj.ExitCode,
			ScheduledAt:  schedTime,
			StartedAt:    startedTime,
			FinishedAt:   finishedTime,
			CreatedDate:  today,
			ErrorMessage: dj.ErrorMsg,
		}
		if err := store.JobRun().Create(ctx, run); err != nil {
			log.Fatalf("failed to create JobRun %s: %v", runID, err)
		}

		// If SUCCESS, also register OutConditions in FJS_CONDITION
		if dj.InitialState == storage.StateSuccess {
			for _, oc := range dj.OutConditions {
				_ = store.Condition().Add(ctx, oc, today)
			}
		}
	}

	// 3. Register Audit Trail samples
	fmt.Println(">> Inserting compliance audit trail records...")
	_ = store.Audit().Record(ctx, "operator_kim", "SET_OK", "run-def-finance-01", "회계부 요청에 따른 선행 배치 정상 승인")
	_ = store.Audit().Record(ctx, "admin_park", "RERUN", "run-def-dw-01", "네트워크 일시 순단에 따른 재수행")
	_ = store.Audit().Record(ctx, "sec_officer", "BYPASS", "run-def-infra-01", "정기 PM 점검 작업으로 임시 우회")

	fmt.Println(">> All demo jobs, conditions, and audit logs successfully generated!")
	_ = json.NewEncoder(os.Stdout)
}
