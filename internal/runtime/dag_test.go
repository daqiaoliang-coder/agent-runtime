package runtime

import (
	"agent-runtime/internal/model"
	"testing"
)

func TestDemoPlannerBuildsParallelDAG(t *testing.T) {
	p, err := DemoPlanner{}.Plan(&model.Run{ID: "r1", Input: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Nodes) != 4 {
		t.Fatalf("nodes=%d", len(p.Nodes))
	}
	if len(p.Nodes[0].DependsOn) != 0 || len(p.Nodes[1].DependsOn) != 0 {
		t.Fatal("first two nodes should be parallel")
	}
	if len(p.Nodes[2].DependsOn) != 2 {
		t.Fatal("reason should have two dependencies")
	}
}
