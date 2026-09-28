package condition

import "testing"

func TestDetectCycle_Graph(t *testing.T) {
	// A -> B -> C (No cycle)
	linearNodes := []*Node{
		{ID: "JobA", OutConditions: []string{"A_OK"}},
		{ID: "JobB", InConditions: []string{"A_OK"}, OutConditions: []string{"B_OK"}},
		{ID: "JobC", InConditions: []string{"B_OK"}},
	}
	if err := DetectCycle(linearNodes); err != nil {
		t.Fatalf("expected no cycle in linear graph, got: %v", err)
	}

	// Diamond: A -> B, A -> C, (B, C) -> D (No cycle)
	diamondNodes := []*Node{
		{ID: "JobA", OutConditions: []string{"A_OK"}},
		{ID: "JobB", InConditions: []string{"A_OK"}, OutConditions: []string{"B_OK"}},
		{ID: "JobC", InConditions: []string{"A_OK"}, OutConditions: []string{"C_OK"}},
		{ID: "JobD", InConditions: []string{"B_OK", "C_OK"}},
	}
	if err := DetectCycle(diamondNodes); err != nil {
		t.Fatalf("expected no cycle in diamond graph, got: %v", err)
	}

	// Circular: A -> B -> C -> A (Cycle!)
	cyclicNodes := []*Node{
		{ID: "JobA", InConditions: []string{"C_OK"}, OutConditions: []string{"A_OK"}},
		{ID: "JobB", InConditions: []string{"A_OK"}, OutConditions: []string{"B_OK"}},
		{ID: "JobC", InConditions: []string{"B_OK"}, OutConditions: []string{"C_OK"}},
	}
	if err := DetectCycle(cyclicNodes); err != ErrCyclicDependency {
		t.Fatalf("expected ErrCyclicDependency, got: %v", err)
	}
}
