package game

import (
	"testing"
	"time"
)

func TestDurations(t *testing.T) {
	if MinDuration > DefaultDuration || DefaultDuration > MaxDuration {
		t.Fatalf("duration bounds %s %s %s", MinDuration, DefaultDuration, MaxDuration)
	}
	if MinGateInterval < 200*time.Millisecond {
		t.Fatalf("gate interval %s would reject a real flight", MinGateInterval)
	}
}
