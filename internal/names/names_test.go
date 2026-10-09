package names

import "testing"

func TestSessionIDShape(t *testing.T) {
	id, err := NewSessionID()
	if err != nil {
		t.Fatal(err)
	}
	if !ValidSessionID(id) {
		t.Fatalf("id %q is not a deployment-safe session id", id)
	}
	if DeploymentName(id) != "sess-"+id {
		t.Fatalf("deployment name %s", DeploymentName(id))
	}
}

func TestPlayerWorkflowIDHidesEmail(t *testing.T) {
	id := PlayerWorkflowID("  Ada@Temporal.io ")
	other := PlayerWorkflowID("ada@temporal.io")
	if id != other {
		t.Fatalf("normalization mismatch %s vs %s", id, other)
	}
	if id == "player-ada@temporal.io" || len(id) < 12 {
		t.Fatalf("unexpected id %s", id)
	}
	if PlayerWorkflowID("ada@temporal.io") == PlayerWorkflowID("ada@example.com") {
		t.Fatal("different emails produced the same workflow id")
	}
}
