package infra

import (
	"os/exec"
	"testing"
	"time"
)

func TestKillDoesNotTakeTheReplacement(t *testing.T) {
	sup := &Supervisor{
		Delay: 250 * time.Millisecond,
		Command: func(_, _, _ string) *exec.Cmd {
			return exec.Command("/bin/sleep", "30")
		},
	}
	if err := sup.Start("abcd1234", "session-abcd1234"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sup.Stop("abcd1234") })

	pid := waitPID(t, sup, "abcd1234")
	if err := sup.Kill(t.Context(), pid); err != nil {
		t.Fatal(err)
	}
	if alive(pid) {
		t.Fatalf("pid %d still alive after kill", pid)
	}

	// The replacement waits out the scheduling delay. Killing the recorded
	// pid again must not signal whatever process is running now.
	deadline := time.Now().Add(2 * time.Second)
	var next int
	for time.Now().Before(deadline) {
		next = sup.CurrentPID("abcd1234")
		if next != 0 && next != pid {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if next == 0 || next == pid {
		t.Fatalf("replacement pid = %d, old pid = %d", next, pid)
	}
	if err := sup.Kill(t.Context(), pid); err != nil {
		t.Fatal(err)
	}
	if !alive(next) {
		t.Fatal("retry of the original kill took down the replacement process")
	}
}

func waitPID(t *testing.T, sup *Supervisor, sessionID string) int {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if pid := sup.CurrentPID(sessionID); pid != 0 {
			return pid
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("session process never started")
	return 0
}
