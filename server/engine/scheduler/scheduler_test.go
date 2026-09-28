package scheduler

import (
	"testing"
	"time"
)

func TestBusinessCalendar_WeekendAndHolidayShifting(t *testing.T) {
	// 2026-09-19 is Saturday, 2026-09-20 is Sunday
	// 2026-09-21 is Monday (let's say it's a holiday e.g. Chuseok)
	holidays := []string{"20260921"}
	cal := NewBusinessCalendar(holidays)

	sat := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)
	if cal.IsBusinessDay(sat) {
		t.Errorf("Saturday should not be a business day")
	}

	mon := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	if cal.IsBusinessDay(mon) {
		t.Errorf("Holiday Monday should not be a business day")
	}

	fri := time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC)
	if !cal.IsBusinessDay(fri) {
		t.Errorf("Friday should be a business day")
	}

	// Policy: Next business day for Saturday -> Tuesday 2026-09-22
	nextDay, run := cal.AdjustDate(sat, PolicyNextBusinessDay)
	if !run || nextDay.Format("20060102") != "20260922" {
		t.Errorf("expected shift to Tuesday 20260922, got %s, run=%v", nextDay.Format("20060102"), run)
	}

	// Policy: Prev business day for Saturday -> Friday 2026-09-18
	prevDay, run := cal.AdjustDate(sat, PolicyPrevBusinessDay)
	if !run || prevDay.Format("20060102") != "20260918" {
		t.Errorf("expected shift to Friday 20260918, got %s, run=%v", prevDay.Format("20060102"), run)
	}

	// Policy: Skip
	_, run = cal.AdjustDate(sat, PolicySkip)
	if run {
		t.Errorf("expected skip policy to return run=false")
	}
}

func TestDynamicVarReplacer_Substitution(t *testing.T) {
	replacer := NewDynamicVarReplacer()
	odate := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)

	cmd := "/app/bin/settle --date=%%$ODATE --prev=%%$PREV_ODATE --year=%%$YEAR --month=%%$MONTH"
	result := replacer.Replace(cmd, odate)

	expected := "/app/bin/settle --date=20260916 --prev=20260915 --year=2026 --month=09"
	if result != expected {
		t.Errorf("expected '%s', got '%s'", expected, result)
	}
}
