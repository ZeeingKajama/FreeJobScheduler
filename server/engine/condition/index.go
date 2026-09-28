package condition

import (
	"fmt"
	"strings"
	"sync"
)

// WaitingTask encapsulates runtime metadata for a task waiting for in-conditions.
type WaitingTask struct {
	RunID        string
	JobDefID     string
	ODate        string
	InConditions []string
}

// SubscriptionIndex provides an in-memory inverted index mapping condition keys to waiting tasks.
// It eliminates O(N) database table scans and N*M query storms during condition cascade evaluation.
type SubscriptionIndex struct {
	mu          sync.RWMutex
	condToTasks map[string]map[string]*WaitingTask // key: "condName:odate" -> runID -> *WaitingTask
	taskToConds map[string][]string                // runID -> []inConditions
	taskMeta    map[string]*WaitingTask            // runID -> *WaitingTask
	condCache   map[string]map[string]bool         // odate -> condName -> satisfied
}

// NewSubscriptionIndex initializes an empty thread-safe inverted condition subscription index.
func NewSubscriptionIndex() *SubscriptionIndex {
	return &SubscriptionIndex{
		condToTasks: make(map[string]map[string]*WaitingTask),
		taskToConds: make(map[string][]string),
		taskMeta:    make(map[string]*WaitingTask),
		condCache:   make(map[string]map[string]bool),
	}
}

// cacheLocked marks a condition as satisfied. Caller holds idx.mu for writing.
func (idx *SubscriptionIndex) cacheLocked(condName, odate string) {
	odate = strings.TrimSpace(odate)
	set := idx.condCache[odate]
	if set == nil {
		set = make(map[string]bool)
		idx.condCache[odate] = set
	}
	set[strings.TrimSpace(condName)] = true
}

// cachedLocked reports whether a condition is cached as satisfied. Caller holds idx.mu.
func (idx *SubscriptionIndex) cachedLocked(condName, odate string) bool {
	return idx.condCache[strings.TrimSpace(odate)][strings.TrimSpace(condName)]
}

// makeKey generates a composite namespaced key "condName:odate".
func makeKey(condName, odate string) string {
	return fmt.Sprintf("%s:%s", strings.TrimSpace(condName), strings.TrimSpace(odate))
}

// RegisterWaitTask indexes a WAIT task by all its required In-Conditions in O(K) where K is len(InConditions).
func (idx *SubscriptionIndex) RegisterWaitTask(task *WaitingTask) {
	if task == nil || task.RunID == "" {
		return
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.taskMeta[task.RunID] = task
	idx.taskToConds[task.RunID] = task.InConditions

	for _, cond := range task.InConditions {
		cond = strings.TrimSpace(cond)
		if cond == "" {
			continue
		}
		key := makeKey(cond, task.ODate)
		if idx.condToTasks[key] == nil {
			idx.condToTasks[key] = make(map[string]*WaitingTask)
		}
		idx.condToTasks[key][task.RunID] = task
	}
}

// UnregisterWaitTask safely removes a task from the inverted index in O(K).
// Typically called when a task transitions to READY, BYPASS, FAILED, or is deleted.
func (idx *SubscriptionIndex) UnregisterWaitTask(runID string, odate string) {
	if runID == "" {
		return
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	conds, exists := idx.taskToConds[runID]
	if !exists {
		delete(idx.taskMeta, runID)
		return
	}

	delete(idx.taskToConds, runID)
	delete(idx.taskMeta, runID)

	for _, cond := range conds {
		cond = strings.TrimSpace(cond)
		key := makeKey(cond, odate)
		if m, ok := idx.condToTasks[key]; ok {
			delete(m, runID)
			if len(m) == 0 {
				delete(idx.condToTasks, key)
			}
		}
	}
}

// SeedCondition caches an already-emitted condition during bootstrap or manual intervention.
func (idx *SubscriptionIndex) SeedCondition(condName, odate string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.cacheLocked(condName, odate)
}

// ForgetCondition removes a condition from the in-memory cache so it is no longer treated as satisfied.
// Runs already released by it are unaffected; only later evaluations see it as missing.
func (idx *SubscriptionIndex) ForgetCondition(condName, odate string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	odate = strings.TrimSpace(odate)
	if set := idx.condCache[odate]; set != nil {
		delete(set, strings.TrimSpace(condName))
		if len(set) == 0 {
			delete(idx.condCache, odate)
		}
	}
}

// ForgetBefore drops the cached conditions of every ODate older than odate (YYYYMMDD strings compare
// chronologically) and returns how many dates were dropped. The cache is only an accelerator in front of
// the database, so a dropped entry is simply looked up again on demand.
func (idx *SubscriptionIndex) ForgetBefore(odate string) int {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	dropped := 0
	for d := range idx.condCache {
		if d < odate {
			delete(idx.condCache, d)
			dropped++
		}
	}
	return dropped
}

// HasCondition reports whether the condition is cached as satisfied.
func (idx *SubscriptionIndex) HasCondition(condName, odate string) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.cachedLocked(condName, odate)
}

// RecordCondition records an emitted condition into the in-memory cache and returns all waiting
// candidate tasks that were listening for this specific condition in O(1).
func (idx *SubscriptionIndex) RecordCondition(condName, odate string) []*WaitingTask {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.cacheLocked(condName, odate)

	taskMap, ok := idx.condToTasks[makeKey(condName, odate)]
	if !ok || len(taskMap) == 0 {
		return nil
	}

	candidates := make([]*WaitingTask, 0, len(taskMap))
	for _, t := range taskMap {
		candidates = append(candidates, t)
	}
	return candidates
}

// IsAllConditionsMet verifies in O(K) in-memory without any database queries if all In-Conditions
// of the given task are satisfied.
func (idx *SubscriptionIndex) IsAllConditionsMet(task *WaitingTask) bool {
	if task == nil || len(task.InConditions) == 0 {
		return true
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	for _, cond := range task.InConditions {
		cond = strings.TrimSpace(cond)
		if cond == "" {
			continue
		}
		if !idx.cachedLocked(cond, task.ODate) {
			return false
		}
	}
	return true
}

// WaitingCount returns the current number of registered WAIT tasks.
func (idx *SubscriptionIndex) WaitingCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.taskMeta)
}
