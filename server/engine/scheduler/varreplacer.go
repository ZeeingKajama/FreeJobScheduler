package scheduler

import (
	"strings"
	"time"
)

// DynamicVarReplacer substitutes ODATE-based date variables in commands and arguments.
type DynamicVarReplacer struct{}

func NewDynamicVarReplacer() *DynamicVarReplacer {
	return &DynamicVarReplacer{}
}

func (r *DynamicVarReplacer) Replace(input string, odate time.Time) string {
	prevDate := odate.AddDate(0, 0, -1)
	nextDate := odate.AddDate(0, 0, 1)

	replacements := []string{
		"%%$ODATE", odate.Format("20060102"),
		"%%$PREV_ODATE", prevDate.Format("20060102"),
		"%%$NEXT_ODATE", nextDate.Format("20060102"),
		"%%$YEAR", odate.Format("2006"),
		"%%$MONTH", odate.Format("01"),
		"%%$DAY", odate.Format("02"),
		"%%$YYYYMM", odate.Format("200601"),
	}

	replacer := strings.NewReplacer(replacements...)
	return replacer.Replace(input)
}

func (r *DynamicVarReplacer) ReplaceMap(env map[string]string, odate time.Time) map[string]string {
	res := make(map[string]string, len(env))
	for k, v := range env {
		res[k] = r.Replace(v, odate)
	}
	return res
}
