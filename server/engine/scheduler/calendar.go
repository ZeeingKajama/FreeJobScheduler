package scheduler

import "time"

type HolidayPolicy string

const (
	PolicyNextBusinessDay HolidayPolicy = "NEXT_DAY"
	PolicyPrevBusinessDay HolidayPolicy = "PREV_DAY"
	PolicySkip            HolidayPolicy = "SKIP"
)

// BusinessCalendar handles weekend and holiday checks and shift adjustments.
type BusinessCalendar struct {
	holidays map[string]bool // "YYYYMMDD" -> true
}

func NewBusinessCalendar(holidays []string) *BusinessCalendar {
	m := make(map[string]bool, len(holidays))
	for _, h := range holidays {
		m[h] = true
	}
	return &BusinessCalendar{holidays: m}
}

// IsBusinessDay returns true if date is Monday-Friday and not in holiday list.
func (c *BusinessCalendar) IsBusinessDay(t time.Time) bool {
	weekday := t.Weekday()
	if weekday == time.Saturday || weekday == time.Sunday {
		return false
	}
	dateStr := t.Format("20060102")
	return !c.holidays[dateStr]
}

// AdjustDate applies the policy if date falls on a weekend or holiday.
// Returns (adjustedDate, shouldRun).
func (c *BusinessCalendar) AdjustDate(t time.Time, policy HolidayPolicy) (time.Time, bool) {
	if c.IsBusinessDay(t) {
		return t, true
	}

	switch policy {
	case PolicySkip:
		return t, false

	case PolicyNextBusinessDay:
		curr := t.AddDate(0, 0, 1)
		for !c.IsBusinessDay(curr) {
			curr = curr.AddDate(0, 0, 1)
		}
		return curr, true

	case PolicyPrevBusinessDay:
		curr := t.AddDate(0, 0, -1)
		for !c.IsBusinessDay(curr) {
			curr = curr.AddDate(0, 0, -1)
		}
		return curr, true

	default:
		return t, true
	}
}
