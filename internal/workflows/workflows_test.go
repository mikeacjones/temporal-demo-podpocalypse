package workflows

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"podpocalypse/internal/infra"
)

func TestRankOrdersScoreThenCrashesThenTime(t *testing.T) {
	earlier := time.Date(2026, 10, 9, 15, 0, 0, 0, time.UTC)
	later := earlier.Add(time.Minute)
	got := upsert(nil, BoardEntry{SessionID: "a", PlayerID: "pa", Name: "A", Score: 10, Crashes: 0, FinishedAt: earlier})
	got = upsert(got, BoardEntry{SessionID: "b", PlayerID: "pb", Name: "B", Score: 50, Crashes: 2, FinishedAt: later})
	got = upsert(got, BoardEntry{SessionID: "c", PlayerID: "pc", Name: "C", Score: 50, Crashes: 1, FinishedAt: later})
	got = upsert(got, BoardEntry{SessionID: "d", PlayerID: "pd", Name: "D", Score: 50, Crashes: 1, FinishedAt: earlier})
	var names []string
	for _, e := range got {
		names = append(names, e.Name)
	}
	require.Equal(t, []string{"D", "C", "B", "A"}, names)

	got = upsert(got, BoardEntry{SessionID: "a2", PlayerID: "pa", Name: "A", Score: 90, Crashes: 0, FinishedAt: later})
	require.Equal(t, "A", got[0].Name)
	require.Equal(t, "a2", got[0].SessionID)

	got = upsert(got, BoardEntry{SessionID: "a3", PlayerID: "pa", Name: "A", Score: 20, Crashes: 0, FinishedAt: later})
	require.Equal(t, 90, got[0].Score)
	require.Equal(t, "a2", got[0].SessionID)
	require.Len(t, got, 4)
}

func TestPlayerKeepsOnePersonalBestAcrossGames(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(10 * time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalGameFinished, PlayerGame{SessionID: "one", Score: 8})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalGameFinished, PlayerGame{SessionID: "two", Score: 5})
	}, 2*time.Second)
	var got PlayerState
	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow(QueryProfile)
		require.NoError(t, err)
		require.NoError(t, val.Get(&got))
		env.CancelWorkflow()
	}, 3*time.Second)

	env.ExecuteWorkflow(PlayerWorkflow, PlayerInput{
		PlayerID: "player-a",
		Name:     "Ada",
		Title:    "SA",
		Email:    "ada@temporal.io",
	})
	require.Error(t, env.GetWorkflowError())
	require.Equal(t, 8, got.BestScore)
	require.Equal(t, "one", got.BestSessionID)
	require.Equal(t, 2, got.GamesPlayed)
	require.Empty(t, got.Games)
}

func TestGatesBecomeTheScore(t *testing.T) {
	env := newEnv(t)
	var kills int
	var acts *infra.Infra
	env.OnActivity(acts.DeleteSessionPod, mock.Anything, mock.Anything).Return(func(context.Context, string) error {
		kills++
		return nil
	})
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateGate, "g1", accept(t), 1)
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateGate, "g2", accept(t), 2)
	}, 2*time.Second)

	in := testInput()
	in.Duration = 3 * time.Second
	env.ExecuteWorkflow(GameSessionWorkflow, in)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	var result SessionResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 2, result.Score)
	require.Equal(t, 0, result.Crashes)
	require.True(t, result.TimedOut)
	require.Zero(t, kills)
}

func TestTreatPushesTheDeadline(t *testing.T) {
	env := newEnv(t)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateTreat, "yum", accept(t), 1)
	}, time.Second)
	rejected := false
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateTreat, "rush", &testsuite.TestUpdateCallback{
			OnReject: func(error) { rejected = true },
			OnAccept: func() { t.Fatal("a rushed treat was accepted") },
		}, 2)
	}, time.Second+200*time.Millisecond)
	var mid PublicState
	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow(QueryState)
		require.NoError(t, err)
		require.NoError(t, val.Get(&mid))
	}, 3*time.Second)

	in := testInput()
	in.Duration = 2 * time.Second
	env.ExecuteWorkflow(GameSessionWorkflow, in)
	require.NoError(t, env.GetWorkflowError())
	require.True(t, rejected)
	require.Equal(t, "playing", mid.Phase)
	require.Equal(t, 1, mid.Treats)
	var result SessionResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.True(t, result.TimedOut)
	require.True(t, result.FinishedAt.Equal(mid.Deadline))
}

func TestCrashDeletesTheSessionWorker(t *testing.T) {
	env := newEnv(t)
	var killed string
	var acts *infra.Infra
	env.OnActivity(acts.DeleteSessionPod, mock.Anything, mock.Anything).Return(func(_ context.Context, sessionID string) error {
		killed = sessionID
		return nil
	})
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateCrash, "boom", accept(t))
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateGate, "after", accept(t), 1)
	}, 2*time.Second)

	in := testInput()
	in.Duration = 3 * time.Second
	env.ExecuteWorkflow(GameSessionWorkflow, in)
	require.NoError(t, env.GetWorkflowError())
	var result SessionResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "abcd1234", killed)
	require.Equal(t, 1, result.Crashes)
	require.Equal(t, 1, result.Score)
}

func TestQueryIsTheFlightNotAQuiz(t *testing.T) {
	env := newEnv(t)
	var body string
	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow(QueryState)
		require.NoError(t, err)
		var state PublicState
		require.NoError(t, val.Get(&state))
		raw, err := json.Marshal(state)
		require.NoError(t, err)
		body = string(raw)
		require.Equal(t, "playing", state.Phase)
		require.Equal(t, 0, state.Score)
		require.Equal(t, "abcd1234", state.Seed)
	}, time.Second)
	env.ExecuteWorkflow(GameSessionWorkflow, testInput())
	require.NoError(t, env.GetWorkflowError())
	require.NotContains(t, body, "question")
	require.NotContains(t, body, "answer")
}

func TestClockEndsTheFlight(t *testing.T) {
	env := newEnv(t)
	in := testInput()
	in.Duration = 2 * time.Second
	env.ExecuteWorkflow(GameSessionWorkflow, in)
	require.NoError(t, env.GetWorkflowError())
	var result SessionResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.True(t, result.TimedOut)
	require.Equal(t, 0, result.Score)
	require.Equal(t, 0, result.Crashes)
}

func TestValidatorRejectsASkippedOrRushedGate(t *testing.T) {
	env := newEnv(t)
	var rejected int
	reject := &testsuite.TestUpdateCallback{
		OnReject: func(error) { rejected++ },
		OnAccept: func() {},
	}
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateGate, "skip", reject, 2)
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateGate, "first", accept(t), 1)
	}, time.Second+100*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateGate, "rush", reject, 2)
	}, time.Second+200*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateGate, "second", accept(t), 2)
	}, 2*time.Second)

	in := testInput()
	in.Duration = 3 * time.Second
	env.ExecuteWorkflow(GameSessionWorkflow, in)
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 2, rejected)
	var result SessionResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, 2, result.Score)
}

func TestLeaderboardSignalAndQuery(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(10 * time.Second)
	when := time.Date(2026, 10, 9, 15, 0, 0, 0, time.UTC)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalSubmitScore, BoardEntry{SessionID: "s1", Name: "Ada", Score: 10, FinishedAt: when})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalSubmitScore, BoardEntry{SessionID: "s2", Name: "Grace", Score: 40, FinishedAt: when})
	}, 2*time.Second)
	var got []BoardEntry
	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow(QueryBoard)
		require.NoError(t, err)
		require.NoError(t, val.Get(&got))
		env.CancelWorkflow()
	}, 3*time.Second)
	env.ExecuteWorkflow(LeaderboardWorkflow, Board{})
	require.Error(t, env.GetWorkflowError())
	require.Len(t, got, 2)
	require.Equal(t, "Grace", got[0].Name)
}

func TestGameRunRecordsATimedOutVisit(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(15 * time.Second)
	env.RegisterActivity(&infra.SessionActivities{})
	env.RegisterWorkflow(GameSessionWorkflow)
	var acts *infra.Infra
	env.OnActivity(acts.ProvisionSessionWorker, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(acts.TeardownSessionWorker, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(acts.AppendLead, mock.Anything, mock.Anything).Return(nil)
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.ExecuteWorkflow(GameRunWorkflow, RunInput{
		SessionID:        "abcd1234",
		PlayerID:         "player-1",
		Name:             "Ada",
		Title:            "SA",
		Email:            "ada@temporal.io",
		Consent:          true,
		Duration:         time.Second,
		ControlTaskQueue: "podpocalypse-control",
	})
	require.NoError(t, env.GetWorkflowError())
	var result RunResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.True(t, result.Session.TimedOut)
	require.Equal(t, "abcd1234", result.Session.SessionID)
	require.Equal(t, 0, result.Session.Score)
}

func newEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(15 * time.Second)
	env.RegisterActivity(&infra.SessionActivities{})
	return env
}

func testInput() SessionInput {
	return SessionInput{
		SessionID:        "abcd1234",
		PlayerID:         "player-1",
		Name:             "Ada",
		Duration:         time.Minute,
		ControlTaskQueue: "podpocalypse-control",
		SessionTaskQueue: "session-abcd1234",
	}
}

func accept(t *testing.T) *testsuite.TestUpdateCallback {
	t.Helper()
	return &testsuite.TestUpdateCallback{
		OnReject: func(err error) { t.Errorf("update rejected: %v", err) },
		OnAccept: func() {},
		OnComplete: func(_ interface{}, err error) {
			if err != nil {
				t.Errorf("update failed: %v", err)
			}
		},
	}
}
