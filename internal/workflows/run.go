package workflows

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"podpocalypse/internal/infra"
	"podpocalypse/internal/names"
)

// RunInput is the durable booth visit. The parent stays on the control-plane
// worker, which is the worker we do not delete.
type RunInput struct {
	SessionID        string
	PlayerID         string
	Name             string
	Title            string
	Email            string
	Consent          bool
	Duration         time.Duration
	ControlTaskQueue string
}

type RunResult struct {
	Session       SessionResult
	BoardRecorded bool
}

func GameRunWorkflow(ctx workflow.Context, in RunInput) (RunResult, error) {
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		HeartbeatTimeout:    20 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	})
	var acts *infra.Infra

	// Teardown has to run even when the game is cancelled, and it has to be
	// recorded in history in the same place on replay. Disconnected context
	// keeps the activity from inheriting that cancellation.
	defer func() {
		dctx, cancel := workflow.NewDisconnectedContext(ctx)
		defer cancel()
		dctx = workflow.WithActivityOptions(dctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
		})
		if err := workflow.ExecuteActivity(dctx, acts.TeardownSessionWorker, in.SessionID).Get(dctx, nil); err != nil {
			workflow.GetLogger(ctx).Error("session teardown failed", "error", err)
		}
	}()

	if err := workflow.ExecuteActivity(actx, acts.ProvisionSessionWorker, infra.ProvisionInput{SessionID: in.SessionID}).Get(ctx, nil); err != nil {
		return RunResult{}, err
	}

	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:               names.SessionWorkflowID(in.SessionID),
		TaskQueue:                names.SessionQueue(in.SessionID),
		WorkflowExecutionTimeout: in.Duration + 5*time.Minute,
		ParentClosePolicy:        enumspb.PARENT_CLOSE_POLICY_TERMINATE,
	})
	var session SessionResult
	err := workflow.ExecuteChildWorkflow(childCtx, GameSessionWorkflow, SessionInput{
		SessionID:        in.SessionID,
		PlayerID:         in.PlayerID,
		Name:             in.Name,
		Duration:         in.Duration,
		ControlTaskQueue: in.ControlTaskQueue,
		SessionTaskQueue: names.SessionQueue(in.SessionID),
	}).Get(ctx, &session)
	if err != nil {
		return RunResult{}, err
	}

	boardErr := workflow.SignalExternalWorkflow(ctx, names.LeaderboardWorkflowID, "", SignalSubmitScore, BoardEntry{
		SessionID:  session.SessionID,
		PlayerID:   session.PlayerID,
		Name:       in.Name,
		Title:      in.Title,
		Score:      session.Score,
		Crashes:    session.Crashes,
		TimedOut:   session.TimedOut,
		FinishedAt: session.FinishedAt,
	}).Get(ctx, nil)
	if boardErr != nil {
		workflow.GetLogger(ctx).Error("leaderboard signal failed", "error", boardErr)
	}
	if err := workflow.SignalExternalWorkflow(ctx, names.PlayerWorkflowID(in.Email), "", SignalGameFinished, PlayerGame{
		SessionID:  session.SessionID,
		Score:      session.Score,
		Crashes:    session.Crashes,
		TimedOut:   session.TimedOut,
		FinishedAt: session.FinishedAt,
	}).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("player signal failed", "error", err)
	}

	leadCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})
	if err := workflow.ExecuteActivity(leadCtx, acts.AppendLead, infra.Lead{
		SessionID:  session.SessionID,
		PlayerID:   session.PlayerID,
		Name:       in.Name,
		Title:      in.Title,
		Email:      in.Email,
		Consent:    in.Consent,
		Score:      session.Score,
		Crashes:    session.Crashes,
		TimedOut:   session.TimedOut,
		FinishedAt: session.FinishedAt,
	}).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("lead append failed", "error", err)
	}

	return RunResult{Session: session, BoardRecorded: boardErr == nil}, nil
}
