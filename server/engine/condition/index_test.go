package condition

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSubscriptionIndex_Basic(t *testing.T) {
	idx := NewSubscriptionIndex()
	odate := "20260916"

	// Task 1 requires COND_A and COND_B
	task1 := &WaitingTask{
		RunID:        "run-1",
		JobDefID:     "def-1",
		ODate:        odate,
		InConditions: []string{"COND_A", "COND_B"},
	}
	// Task 2 requires only COND_B
	task2 := &WaitingTask{
		RunID:        "run-2",
		JobDefID:     "def-2",
		ODate:        odate,
		InConditions: []string{"COND_B"},
	}

	idx.RegisterWaitTask(task1)
	idx.RegisterWaitTask(task2)

	if idx.WaitingCount() != 2 {
		t.Fatalf("expected 2 waiting tasks, got %d", idx.WaitingCount())
	}

	// 1. Emit COND_A
	candidates := idx.RecordCondition("COND_A", odate)
	if len(candidates) != 1 || candidates[0].RunID != "run-1" {
		t.Fatalf("expected only task1 for COND_A, got %v", candidates)
	}
	if idx.IsAllConditionsMet(task1) {
		t.Fatalf("task1 should not be met yet (still requires COND_B)")
	}

	// 2. Emit COND_B
	candidates = idx.RecordCondition("COND_B", odate)
	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidates for COND_B, got %d", len(candidates))
	}

	if !idx.IsAllConditionsMet(task1) {
		t.Fatalf("task1 should be satisfied now")
	}
	if !idx.IsAllConditionsMet(task2) {
		t.Fatalf("task2 should be satisfied now")
	}

	// 3. Unregister task1
	idx.UnregisterWaitTask(task1.RunID, odate)
	if idx.WaitingCount() != 1 {
		t.Fatalf("expected 1 waiting task left, got %d", idx.WaitingCount())
	}

	// Emit COND_A again -> task1 should not be returned
	candidates = idx.RecordCondition("COND_A", odate)
	if len(candidates) != 0 {
		t.Fatalf("task1 was unregistered, expected 0 candidates, got %d", len(candidates))
	}
}

func TestSubscriptionIndex_ODateIsolation(t *testing.T) {
	idx := NewSubscriptionIndex()

	taskToday := &WaitingTask{
		RunID:        "run-today",
		JobDefID:     "def-1",
		ODate:        "20260916",
		InConditions: []string{"DAY_CLOSE_OK"},
	}
	taskYesterday := &WaitingTask{
		RunID:        "run-yesterday",
		JobDefID:     "def-2",
		ODate:        "20260915",
		InConditions: []string{"DAY_CLOSE_OK"},
	}

	idx.RegisterWaitTask(taskToday)
	idx.RegisterWaitTask(taskYesterday)

	// Emit DAY_CLOSE_OK for yesterday only
	candidates := idx.RecordCondition("DAY_CLOSE_OK", "20260915")
	if len(candidates) != 1 || candidates[0].RunID != "run-yesterday" {
		t.Fatalf("expected only taskYesterday, got %v", candidates)
	}

	if !idx.IsAllConditionsMet(taskYesterday) {
		t.Fatalf("yesterday task should be met")
	}
	if idx.IsAllConditionsMet(taskToday) {
		t.Fatalf("today task must NOT be met by yesterday condition")
	}
}

func TestSubscriptionIndex_Scale_10000(t *testing.T) {
	idx := NewSubscriptionIndex()
	odate := "20260916"
	totalTasks := 10000

	// Register 10,000 tasks
	for i := 0; i < totalTasks; i++ {
		cond := fmt.Sprintf("BATCH_COND_%05d", i%200) // 200 groups, 50 tasks each
		task := &WaitingTask{
			RunID:        fmt.Sprintf("run-%05d", i),
			JobDefID:     fmt.Sprintf("def-%05d", i),
			ODate:        odate,
			InConditions: []string{cond},
		}
		idx.RegisterWaitTask(task)
	}

	if idx.WaitingCount() != totalTasks {
		t.Fatalf("expected %d tasks registered, got %d", totalTasks, idx.WaitingCount())
	}

	// Measure O(1) lookup time for a single condition
	start := time.Now()
	candidates := idx.RecordCondition("BATCH_COND_00042", odate)
	duration := time.Since(start)

	if len(candidates) != 50 {
		t.Fatalf("expected exactly 50 candidate tasks for BATCH_COND_00042, got %d", len(candidates))
	}

	// 10,000 tasks in index, lookup should be under 500 microseconds (typically < 10us)
	t.Logf("O(1) RecordCondition lookup among 10,000 tasks took: %v", duration)
	if duration > 5*time.Millisecond {
		t.Fatalf("lookup took too long: %v (expected < 5ms)", duration)
	}
}

func TestSubscriptionIndex_Concurrent(t *testing.T) {
	idx := NewSubscriptionIndex()
	odate := "20260916"

	var wg sync.WaitGroup
	workers := 20
	opsPerWorker := 500

	// Concurrently register, query, and record conditions
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				runID := fmt.Sprintf("worker-%d-task-%d", workerID, i)
				cond := fmt.Sprintf("COND_C_%d", i%50)

				task := &WaitingTask{
					RunID:        runID,
					JobDefID:     "def-c",
					ODate:        odate,
					InConditions: []string{cond},
				}
				idx.RegisterWaitTask(task)

				if i%5 == 0 {
					_ = idx.RecordCondition(cond, odate)
				}
				if i%10 == 0 {
					_ = idx.IsAllConditionsMet(task)
				}
				if i%20 == 0 {
					idx.UnregisterWaitTask(runID, odate)
				}
			}
		}(w)
	}

	wg.Wait()
}

func TestSubscriptionIndex_ForgetCondition(t *testing.T) {
	idx := NewSubscriptionIndex()
	idx.SeedCondition("A_OK", "20260916")
	idx.SeedCondition("B_OK", "20260916")

	idx.ForgetCondition(" A_OK ", "20260916")
	if idx.HasCondition("A_OK", "20260916") {
		t.Fatalf("forgotten condition must no longer be cached")
	}
	if !idx.HasCondition("B_OK", "20260916") {
		t.Fatalf("other conditions of the same ODate must survive")
	}
	idx.ForgetCondition("nope", "20990101") // unknown ODate is a no-op
}

func TestSubscriptionIndex_ForgetBefore(t *testing.T) {
	idx := NewSubscriptionIndex()
	for _, d := range []string{"20260910", "20260915", "20260916"} {
		idx.SeedCondition("DAY_OK", d)
	}

	if dropped := idx.ForgetBefore("20260916"); dropped != 2 {
		t.Fatalf("expected 2 ODates dropped, got %d", dropped)
	}
	if idx.HasCondition("DAY_OK", "20260910") || idx.HasCondition("DAY_OK", "20260915") {
		t.Fatalf("older ODates must be dropped")
	}
	if !idx.HasCondition("DAY_OK", "20260916") {
		t.Fatalf("the cutoff ODate itself must be kept")
	}
	if dropped := idx.ForgetBefore("20260916"); dropped != 0 {
		t.Fatalf("second prune must drop nothing, got %d", dropped)
	}
}
