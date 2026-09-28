package condition

import "errors"

var ErrCyclicDependency = errors.New("condition: cyclic dependency detected in workflow DAG")

// Node represents a Job in a DAG dependency graph.
type Node struct {
	ID            string
	InConditions  []string // In-Conditions needed by this job
	OutConditions []string // Out-Conditions produced by this job
}

// DetectCycle checks whether a collection of jobs contains circular dependencies.
func DetectCycle(nodes []*Node) error {
	// Map conditionName -> producing JobIDs
	producerMap := make(map[string][]string)
	for _, n := range nodes {
		for _, out := range n.OutConditions {
			producerMap[out] = append(producerMap[out], n.ID)
		}
	}

	// Build adjacency list: parentJob -> []childJob
	adj := make(map[string][]string)
	for _, child := range nodes {
		for _, in := range child.InConditions {
			if parents, ok := producerMap[in]; ok {
				for _, parentID := range parents {
					if parentID != child.ID {
						adj[parentID] = append(adj[parentID], child.ID)
					}
				}
			}
		}
	}

	// 0: unvisited, 1: visiting (in recursion stack), 2: visited
	visited := make(map[string]int)

	var dfs func(u string) bool
	dfs = func(u string) bool {
		visited[u] = 1
		for _, v := range adj[u] {
			if visited[v] == 1 {
				return true // Cycle detected!
			}
			if visited[v] == 0 {
				if dfs(v) {
					return true
				}
			}
		}
		visited[u] = 2
		return false
	}

	for _, n := range nodes {
		if visited[n.ID] == 0 {
			if dfs(n.ID) {
				return ErrCyclicDependency
			}
		}
	}

	return nil
}
