package infra

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.temporal.io/sdk/temporal"
)

// Supervisor runs one local process per game session and restarts it after a
// delay when that process exits. The delay is the minikube-free stand-in for
// Kubernetes scheduling: the kill activity returns only after the old process
// is gone, and the replacement is not started until the delay elapses.
type Supervisor struct {
	Delay   time.Duration
	Bin     string
	BaseEnv []string
	// Command, when set, replaces the session binary. Tests use it to spawn sleep.
	Command func(sessionID, taskQueue, podName string) *exec.Cmd

	mu       sync.Mutex
	sessions map[string]*running
}

type running struct {
	sessionID string
	taskQueue string
	stop      chan struct{}
	stopped   chan struct{}

	mu         sync.Mutex
	pid        int
	generation int
	podName    string
	exited     chan struct{}
}

func (s *Supervisor) init() {
	if s.sessions == nil {
		s.sessions = map[string]*running{}
	}
	if s.Delay <= 0 {
		s.Delay = 4 * time.Second
	}
}

func (s *Supervisor) Start(sessionID, taskQueue string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.init()
	if _, ok := s.sessions[sessionID]; ok {
		return nil
	}
	r := &running{
		sessionID: sessionID,
		taskQueue: taskQueue,
		stop:      make(chan struct{}),
		stopped:   make(chan struct{}),
	}
	s.sessions[sessionID] = r
	go s.loop(r)
	return nil
}

func (s *Supervisor) Stop(sessionID string) error {
	s.mu.Lock()
	r := s.sessions[sessionID]
	delete(s.sessions, sessionID)
	s.mu.Unlock()
	if r == nil {
		return nil
	}
	close(r.stop)
	select {
	case <-r.stopped:
		return nil
	case <-time.After(10 * time.Second):
		return fmt.Errorf("session %s did not exit", sessionID)
	}
}

func (s *Supervisor) StopAll() {
	s.mu.Lock()
	ids := make([]string, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	for _, id := range ids {
		_ = s.Stop(id)
	}
}

func (s *Supervisor) CurrentPID(sessionID string) int {
	r := s.get(sessionID)
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pid
}

// Kill waits until pid is gone. It does not start the replacement; the
// supervisor loop does that after Delay, which is what leaves the workflow
// with nobody polling.
func (s *Supervisor) Kill(ctx context.Context, pid int) error {
	if pid <= 0 {
		return temporal.NewApplicationError("session worker has no pid yet", "WorkerNotRunning")
	}
	if !alive(pid) {
		return nil
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil && alive(pid) {
		return fmt.Errorf("signal %d: %w", pid, err)
	}
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		if !alive(pid) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			_ = proc.Signal(syscall.SIGKILL)
			return fmt.Errorf("pid %d ignored SIGTERM", pid)
		case <-tick.C:
		}
	}
}

func (s *Supervisor) get(sessionID string) *running {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions == nil {
		return nil
	}
	return s.sessions[sessionID]
}

func (s *Supervisor) loop(r *running) {
	defer close(r.stopped)
	for {
		if s.stopped(r) {
			return
		}
		cmd, podName, gen := s.spawn(r)
		if cmd == nil {
			return
		}
		if err := cmd.Start(); err != nil {
			if s.waitDelay(r) {
				return
			}
			continue
		}
		exited := make(chan struct{})
		r.mu.Lock()
		r.pid = cmd.Process.Pid
		r.generation = gen
		r.podName = podName
		r.exited = exited
		r.mu.Unlock()

		waitErr := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(waitErr)
			close(exited)
		}()

		select {
		case <-r.stop:
			_ = cmd.Process.Signal(syscall.SIGTERM)
			<-waitErr
			r.clearPID()
			return
		case <-waitErr:
			r.clearPID()
		}
		if s.waitDelay(r) {
			return
		}
	}
}

func (s *Supervisor) spawn(r *running) (*exec.Cmd, string, int) {
	r.mu.Lock()
	r.generation++
	gen := r.generation
	r.mu.Unlock()
	podName := fmt.Sprintf("proc-%s-%d", r.sessionID, gen)
	if s.Command != nil {
		return s.Command(r.sessionID, r.taskQueue, podName), podName, gen
	}
	bin := s.Bin
	if bin == "" {
		var err error
		bin, err = os.Executable()
		if err != nil {
			return nil, podName, gen
		}
	}
	cmd := exec.Command(bin, "-role", "session")
	cmd.Env = sessionEnviron(s.BaseEnv,
		"ROLE=session",
		"TASK_QUEUE="+r.taskQueue,
		"SESSION_ID="+r.sessionID,
		"POD_NAME="+podName,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return cmd, podName, gen
}

func (r *running) clearPID() {
	r.mu.Lock()
	r.pid = 0
	r.mu.Unlock()
}

func (s *Supervisor) stopped(r *running) bool {
	select {
	case <-r.stop:
		return true
	default:
		return false
	}
}

func (s *Supervisor) waitDelay(r *running) bool {
	timer := time.NewTimer(s.Delay)
	defer timer.Stop()
	select {
	case <-r.stop:
		return true
	case <-timer.C:
		return false
	}
}

func sessionEnviron(base []string, extra ...string) []string {
	drop := map[string]bool{"ROLE": true, "TASK_QUEUE": true, "SESSION_ID": true, "POD_NAME": true}
	out := make([]string, 0, len(base)+len(extra))
	for _, entry := range base {
		key, _, _ := strings.Cut(entry, "=")
		if drop[key] {
			continue
		}
		out = append(out, entry)
	}
	return append(out, extra...)
}

func alive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	err = proc.Signal(syscall.Signal(0))
	return err == nil
}
