//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestPlaythrough(t *testing.T) {
	root := moduleRoot(t)
	if err := startTemporal(t); err != nil {
		t.Fatalf("temporal dev server: %v", err)
	}
	bin := filepath.Join(root, "bin", "podpocalypse-e2e")
	build := exec.Command("go", "build", "-o", bin, "./cmd/podpocalypse")
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build booth: %v\n%s", err, out)
	}
	leadPath := filepath.Join(t.TempDir(), "leads.jsonl")
	booth := exec.Command(bin)
	booth.Dir = root
	booth.Env = append(os.Environ(),
		"ROLE=booth",
		"HTTP_ADDR=127.0.0.1:18080",
		"TEMPORAL_ADDRESS=127.0.0.1:17233",
		"TEMPORAL_NAMESPACE=default",
		"TEMPORAL_UI_URL=http://127.0.0.1:18233",
		"RUNTIME=process",
		"RECOVERY_DELAY=3s",
		"GAME_DURATION=15s",
		"LEAD_PATH="+leadPath,
		"ADMIN_TOKEN=e2e-token",
	)
	booth.Stdout = os.Stdout
	booth.Stderr = os.Stderr
	if err := booth.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = booth.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() {
			_ = booth.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			_ = booth.Process.Kill()
		}
	})
	waitHTTP(t, "http://127.0.0.1:18080/healthz")

	playBody := map[string]any{
		"name": "Ada Lovelace", "title": "SA", "email": "ada@temporal.io", "consent": true,
	}
	var started struct {
		SessionID string `json:"sessionId"`
		State     state  `json:"state"`
	}
	postJSON(t, "http://127.0.0.1:18080/api/play", playBody, &started)
	if started.State.Phase != "playing" || started.State.Seed == "" {
		t.Fatalf("game did not become playable: %+v", started.State)
	}
	if started.State.WorkflowID == "" || started.State.HistoryLength == 0 {
		t.Fatalf("workflow identity missing: %+v", started.State)
	}

	res, raw := postRaw(t, "http://127.0.0.1:18080/api/sessions/"+started.SessionID+"/crash", map[string]any{})
	if res.StatusCode != http.StatusAccepted && res.StatusCode != http.StatusOK {
		t.Fatalf("crash status %d body %s", res.StatusCode, raw)
	}

	sawOutage := res.StatusCode == http.StatusAccepted
	deadline := time.Now().Add(20 * time.Second)
	var recovered state
	for time.Now().Before(deadline) {
		code, body := getRaw(t, "http://127.0.0.1:18080/api/sessions/"+started.SessionID)
		if code == http.StatusServiceUnavailable {
			sawOutage = true
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if code != http.StatusOK {
			t.Fatalf("session status %d body %s", code, body)
		}
		var st state
		if err := json.Unmarshal(body, &st); err != nil {
			t.Fatal(err)
		}
		if st.Phase == "recovering" {
			sawOutage = true
		}
		if st.Phase == "playing" && st.Crashes >= 1 {
			recovered = st
			break
		}
		if st.Finished {
			t.Fatalf("flight ended during the outage: %+v", st)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !sawOutage {
		t.Fatal("deleting the session worker did not make the game wait")
	}
	if recovered.Crashes < 1 {
		t.Fatalf("game did not resume after the worker was replaced: %+v", recovered)
	}
	if recovered.WorkerPod == "" || recovered.WorkerPod == started.State.WorkerPod {
		t.Fatalf("resumed on the same worker %q, started on %q", recovered.WorkerPod, started.State.WorkerPod)
	}

	var gated struct {
		Score int   `json:"score"`
		State state `json:"state"`
	}
	postJSON(t, "http://127.0.0.1:18080/api/sessions/"+started.SessionID+"/gates", map[string]any{"gate": 1}, &gated)
	if gated.Score != 1 {
		t.Fatalf("gate did not score: %+v", gated)
	}

	current := gated.State
	finishBy := time.Now().Add(25 * time.Second)
	for !current.Finished && time.Now().Before(finishBy) {
		time.Sleep(300 * time.Millisecond)
		code, body := getRaw(t, "http://127.0.0.1:18080/api/sessions/"+started.SessionID)
		if code == http.StatusServiceUnavailable {
			continue
		}
		if code != http.StatusOK {
			t.Fatalf("poll status %d %s", code, body)
		}
		if err := json.Unmarshal(body, &current); err != nil {
			t.Fatal(err)
		}
	}
	if !current.Finished || current.Crashes != 1 || current.Score < 1 {
		t.Fatalf("final state %+v", current)
	}

	var board []boardRow
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		getJSON(t, "http://127.0.0.1:18080/api/leaderboard", &board)
		for _, row := range board {
			if row.Name == "Ada Lovelace" && row.Score == current.Score {
				goto prize
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("leaderboard never recorded Ada: %+v", board)

prize:
	req, err := http.NewRequest(http.MethodGet, "http://127.0.0.1:18080/api/prize", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer e2e-token")
	prizeRes, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer prizeRes.Body.Close()
	prizeBody, _ := io.ReadAll(prizeRes.Body)
	if prizeRes.StatusCode != http.StatusOK || !bytes.Contains(prizeBody, []byte("ada@temporal.io")) {
		t.Fatalf("prize status %d body %s", prizeRes.StatusCode, prizeBody)
	}

	deadline = time.Now().Add(10 * time.Second)
	leadWritten := false
	for time.Now().Before(deadline) {
		f, err := os.ReadFile(leadPath)
		if err == nil && bytes.Contains(f, []byte("ada@temporal.io")) {
			leadWritten = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !leadWritten {
		t.Fatalf("lead file %s never received the visit", leadPath)
	}

	// A returning email starts a fresh session while the player entity returns
	// the single best score from prior flights.
	var replay struct {
		SessionID    string `json:"sessionId"`
		PersonalBest int    `json:"personalBest"`
		GamesPlayed  int    `json:"gamesPlayed"`
	}
	postJSON(t, "http://127.0.0.1:18080/api/play", playBody, &replay)
	if replay.SessionID == started.SessionID {
		t.Fatal("returning player reused the old game session")
	}
	if replay.PersonalBest != current.Score || replay.GamesPlayed != 1 {
		t.Fatalf("returning player profile = %+v, want best %d after one game", replay, current.Score)
	}
}

type state struct {
	Phase         string `json:"phase"`
	WorkflowID    string `json:"workflowId"`
	HistoryLength int    `json:"historyLength"`
	WorkerPod     string `json:"workerPod"`
	Crashes       int    `json:"crashes"`
	Score         int    `json:"score"`
	Finished      bool   `json:"finished"`
	Seed          string `json:"seed"`
}

type boardRow struct {
	Name  string `json:"name"`
	Score int    `json:"score"`
}

func startTemporal(t *testing.T) error {
	t.Helper()
	if portOpen("127.0.0.1:17233") {
		return nil
	}
	_ = exec.Command("docker", "rm", "-f", "podpocalypse-e2e-temporal").Run()
	cmd := exec.Command("docker", "run", "-d", "--name", "podpocalypse-e2e-temporal",
		"-p", "17233:7233", "-p", "18233:8233",
		"temporalio/temporal:1.5.1",
		"server", "start-dev", "--ip", "0.0.0.0", "--port", "7233", "--ui-port", "8233", "--log-format", "pretty")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%v: %s", err, out)
	}
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", "podpocalypse-e2e-temporal").Run()
	})
	deadline := time.Now().Add(40 * time.Second)
	for time.Now().Before(deadline) {
		if portOpen("127.0.0.1:17233") {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	logs, _ := exec.Command("docker", "logs", "podpocalypse-e2e-temporal").CombinedOutput()
	return fmt.Errorf("temporal port never opened\n%s", logs)
}

func waitHTTP(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		res, err := http.Get(url)
		if err == nil {
			res.Body.Close()
			if res.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", url)
}

func postJSON(t *testing.T, url string, body any, dest any) {
	t.Helper()
	res, raw := postRaw(t, url, body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("POST %s status %d body %s", url, res.StatusCode, raw)
	}
	if dest != nil {
		if err := json.Unmarshal(raw, dest); err != nil {
			t.Fatal(err)
		}
	}
}

func postRaw(t *testing.T, url string, body any) (*http.Response, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.Post(url, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	got, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res, got
}

func getJSON(t *testing.T, url string, dest any) {
	t.Helper()
	code, body := getRaw(t, url)
	if code != http.StatusOK {
		t.Fatalf("GET %s status %d body %s", url, code, body)
	}
	if err := json.Unmarshal(body, dest); err != nil {
		t.Fatal(err)
	}
}

func getRaw(t *testing.T, url string) (int, []byte) {
	t.Helper()
	res, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, body
}

func portOpen(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}
