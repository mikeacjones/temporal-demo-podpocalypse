package workflows

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"podpocalypse/internal/game"
	"podpocalypse/internal/infra"
)

const (
	QueryState  = "state"
	UpdateGate  = "pass_gate"
	UpdateCrash = "crash"
	UpdateTreat = "eat_treat"
)

// SessionInput is one flight. Email stays on the player workflow.
type SessionInput struct {
	SessionID        string
	PlayerID         string
	Name             string
	Duration         time.Duration
	ControlTaskQueue string
	SessionTaskQueue string
}

type PublicState struct {
	SessionID       string    `json:"sessionId"`
	WorkflowID      string    `json:"workflowId"`
	RunID           string    `json:"runId"`
	PlayerName      string    `json:"playerName"`
	Phase           string    `json:"phase"`
	Score           int       `json:"score"`
	Crashes         int       `json:"crashes"`
	Treats          int       `json:"treats"`
	Deadline        time.Time `json:"deadline"`
	RecoveryStarted time.Time `json:"recoveryStarted,omitempty"`
	RecoveryMS      int64     `json:"recoveryMs"`
	WorkerPod       string    `json:"workerPod,omitempty"`
	WorkerPID       int       `json:"workerPid,omitempty"`
	HistoryLength   int       `json:"historyLength"`
	TimedOut        bool      `json:"timedOut"`
	Finished        bool      `json:"finished"`
	Seed            string    `json:"seed"`
}

type GateResult struct {
	Score int         `json:"score"`
	State PublicState `json:"state"`
}

type CrashResult struct {
	Crashes int         `json:"crashes"`
	State   PublicState `json:"state"`
}

type TreatResult struct {
	Treats   int         `json:"treats"`
	Deadline time.Time   `json:"deadline"`
	State    PublicState `json:"state"`
}

type SessionResult struct {
	SessionID  string
	PlayerID   string
	Name       string
	Score      int
	Crashes    int
	Recovery   time.Duration
	TimedOut   bool
	FinishedAt time.Time
	WorkerPod  string
}

type sessionState struct {
	in              SessionInput
	phase           string
	score           int
	crashes         int
	treats          int
	deadline        time.Time
	lastGate        time.Time
	lastTreat       time.Time
	recoveryStarted time.Time
	recoveryTotal   time.Duration
	workerPod       string
	workerPID       int
	finished        bool
	timedOut        bool
	finishedAt      time.Time
	busy            bool
	wake            workflow.Channel
	workflowID      string
	runID           string
}

func GameSessionWorkflow(ctx workflow.Context, in SessionInput) (SessionResult, error) {
	if in.Duration <= 0 || in.ControlTaskQueue == "" || in.SessionTaskQueue == "" || in.SessionID == "" {
		return SessionResult{}, temporal.NewNonRetryableApplicationError("session input is incomplete", "BadInput", nil)
	}
	info := workflow.GetInfo(ctx)
	s := &sessionState{
		in:         in,
		phase:      "starting",
		deadline:   workflow.Now(ctx).Add(in.Duration),
		workflowID: info.WorkflowExecution.ID,
		runID:      info.WorkflowExecution.RunID,
	}
	if err := workflow.SetQueryHandler(ctx, QueryState, func() (PublicState, error) {
		return s.view(ctx), nil
	}); err != nil {
		return SessionResult{}, err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateGate,
		func(ctx workflow.Context, gate int) (GateResult, error) {
			return s.passGate(ctx, gate)
		},
		workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, gate int) error {
			return s.validateGate(ctx, gate)
		}},
	); err != nil {
		return SessionResult{}, err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateTreat,
		func(ctx workflow.Context, treat int) (TreatResult, error) {
			return s.eatTreat(ctx, treat)
		},
		workflow.UpdateHandlerOptions{Validator: func(ctx workflow.Context, treat int) error {
			return s.validateTreat(ctx, treat)
		}},
	); err != nil {
		return SessionResult{}, err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateCrash,
		func(ctx workflow.Context) (CrashResult, error) {
			return s.crash(ctx)
		},
		workflow.UpdateHandlerOptions{Validator: func() error {
			return s.validateCrash()
		}},
	); err != nil {
		return SessionResult{}, err
	}

	s.wake = workflow.NewBufferedChannel(ctx, 16)
	if err := s.identify(ctx); err != nil {
		return SessionResult{}, err
	}
	s.phase = "playing"
	workflow.SetCurrentDetails(ctx, s.details())

	timerFired := false
	timer := workflow.NewTimer(ctx, in.Duration)
	for !s.finished {
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(s.wake, func(c workflow.ReceiveChannel, _ bool) {
			var ping struct{}
			c.Receive(ctx, &ping)
		})
		sel.AddReceive(ctx.Done(), func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
		})
		if !timerFired {
			sel.AddFuture(timer, func(workflow.Future) { timerFired = true })
		}
		sel.Select(ctx)
		if ctx.Err() != nil {
			return SessionResult{}, ctx.Err()
		}
		if s.finished {
			break
		}
		// The clock is a durable timer. It keeps running while this session's
		// worker is gone; the flight only ends once a worker is back and the
		// crash handler has finished deleting the pod.
		if timerFired && !s.busy {
			now := workflow.Now(ctx)
			// A treat pushes the deadline past the timer that was already
			// scheduled. Start another timer for whatever time was added.
			if now.Before(s.deadline) {
				timerFired = false
				timer = workflow.NewTimer(ctx, s.deadline.Sub(now))
				continue
			}
			s.finish(ctx)
		}
	}
	return s.result(), nil
}

func (s *sessionState) validateGate(ctx workflow.Context, gate int) error {
	if s.finished || s.phase != "playing" {
		return fmt.Errorf("not flying (%s)", s.phase)
	}
	if gate != s.score+1 {
		return fmt.Errorf("gate %d is not the next gate", gate)
	}
	if !s.lastGate.IsZero() && workflow.Now(ctx).Sub(s.lastGate) < game.MinGateInterval {
		return fmt.Errorf("gate %d came in too fast", gate)
	}
	return nil
}

func (s *sessionState) passGate(ctx workflow.Context, gate int) (GateResult, error) {
	s.score = gate
	s.lastGate = workflow.Now(ctx)
	s.wake.SendAsync(struct{}{})
	workflow.SetCurrentDetails(ctx, s.details())
	return GateResult{Score: s.score, State: s.view(ctx)}, nil
}

func (s *sessionState) validateTreat(ctx workflow.Context, treat int) error {
	if s.finished || s.phase != "playing" {
		return fmt.Errorf("not flying (%s)", s.phase)
	}
	if treat != s.treats+1 || treat > game.MaxTreats {
		return fmt.Errorf("treat %d is not available", treat)
	}
	if !s.lastTreat.IsZero() && workflow.Now(ctx).Sub(s.lastTreat) < game.TreatCooldown {
		return fmt.Errorf("treat %d came in too fast", treat)
	}
	return nil
}

func (s *sessionState) eatTreat(ctx workflow.Context, treat int) (TreatResult, error) {
	s.treats = treat
	s.lastTreat = workflow.Now(ctx)
	s.deadline = s.deadline.Add(game.TreatBonus)
	s.wake.SendAsync(struct{}{})
	workflow.SetCurrentDetails(ctx, s.details())
	return TreatResult{Treats: s.treats, Deadline: s.deadline, State: s.view(ctx)}, nil
}

func (s *sessionState) validateCrash() error {
	if s.finished || s.phase != "playing" {
		return fmt.Errorf("not flying (%s)", s.phase)
	}
	return nil
}

func (s *sessionState) crash(ctx workflow.Context) (CrashResult, error) {
	s.busy = true
	defer func() {
		s.busy = false
		s.wake.SendAsync(struct{}{})
	}()

	s.phase = "recovering"
	started := workflow.Now(ctx)
	s.recoveryStarted = started
	workflow.SetCurrentDetails(ctx, s.details())

	// Recorded on the control queue before the session pod is deleted, and
	// completed only after that pod is gone, so the replacement resumes here.
	killCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           s.in.ControlTaskQueue,
		StartToCloseTimeout: 2 * time.Minute,
		HeartbeatTimeout:    20 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 5,
			InitialInterval: time.Second,
		},
	})
	var acts *infra.Infra
	if err := workflow.ExecuteActivity(killCtx, acts.DeleteSessionPod, s.in.SessionID).Get(ctx, nil); err != nil {
		return CrashResult{}, err
	}
	s.recoveryTotal += workflow.Now(ctx).Sub(started)
	s.recoveryStarted = time.Time{}
	s.crashes++
	if err := s.identify(ctx); err != nil {
		return CrashResult{}, err
	}
	if !workflow.Now(ctx).Before(s.deadline) {
		s.finish(ctx)
		return CrashResult{Crashes: s.crashes, State: s.view(ctx)}, nil
	}
	s.phase = "playing"
	workflow.SetCurrentDetails(ctx, s.details())
	return CrashResult{Crashes: s.crashes, State: s.view(ctx)}, nil
}

func (s *sessionState) identify(ctx workflow.Context) error {
	idCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              s.in.SessionTaskQueue,
		StartToCloseTimeout:    30 * time.Second,
		ScheduleToStartTimeout: 3 * time.Minute,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 5},
	})
	var acts *infra.SessionActivities
	var id infra.WorkerIdentity
	if err := workflow.ExecuteActivity(idCtx, acts.IdentifyWorker).Get(ctx, &id); err != nil {
		return err
	}
	s.workerPod = id.Pod
	s.workerPID = id.PID
	return nil
}

func (s *sessionState) finish(ctx workflow.Context) {
	if s.finished {
		return
	}
	s.finished = true
	s.timedOut = true
	s.phase = "finished"
	s.finishedAt = workflow.Now(ctx)
	workflow.SetCurrentDetails(ctx, s.details())
}

func (s *sessionState) view(ctx workflow.Context) PublicState {
	return PublicState{
		SessionID:       s.in.SessionID,
		WorkflowID:      s.workflowID,
		RunID:           s.runID,
		PlayerName:      s.in.Name,
		Phase:           s.phase,
		Score:           s.score,
		Crashes:         s.crashes,
		Treats:          s.treats,
		Deadline:        s.deadline,
		RecoveryStarted: s.recoveryStarted,
		RecoveryMS:      s.recoveryTotal.Milliseconds(),
		WorkerPod:       s.workerPod,
		WorkerPID:       s.workerPID,
		HistoryLength:   workflow.GetInfo(ctx).GetCurrentHistoryLength(),
		TimedOut:        s.timedOut,
		Finished:        s.finished,
		Seed:            s.in.SessionID,
	}
}

func (s *sessionState) result() SessionResult {
	return SessionResult{
		SessionID:  s.in.SessionID,
		PlayerID:   s.in.PlayerID,
		Name:       s.in.Name,
		Score:      s.score,
		Crashes:    s.crashes,
		Recovery:   s.recoveryTotal,
		TimedOut:   s.timedOut,
		FinishedAt: s.finishedAt,
		WorkerPod:  s.workerPod,
	}
}

func (s *sessionState) details() string {
	return fmt.Sprintf("**%s** · %s · %d gates · %d crashes · worker `%s`",
		s.in.Name, s.phase, s.score, s.crashes, s.workerPod)
}
