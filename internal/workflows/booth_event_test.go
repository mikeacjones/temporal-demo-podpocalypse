package workflows

import (
	"context"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// fakeTracker stands in for the event tracker's Nexus service.
type fakeTracker struct {
	created []createLeadEvent
	submits []submitLead
}

func (f *fakeTracker) service() *nexus.Service {
	svc := nexus.NewService(LeadServiceName)
	svc.MustRegister(
		nexus.NewSyncOperation(opCreateEvent, func(_ context.Context, in createLeadEvent, _ nexus.StartOperationOptions) (LeadEvent, error) {
			f.created = append(f.created, in)
			return LeadEvent{ID: "kubecon-2026", Link: "kubecon-2026", Name: in.Name, Status: "open"}, nil
		}),
		nexus.NewSyncOperation(opSubmitParticipant, func(_ context.Context, in submitLead, _ nexus.StartOperationOptions) (map[string]string, error) {
			f.submits = append(f.submits, in)
			return map[string]string{"status": "saved"}, nil
		}),
	)
	return svc
}

func boothEnv(t *testing.T, tracker *fakeTracker) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetTestTimeout(15 * time.Second)
	env.RegisterNexusService(tracker.service())
	env.OnWorkflow(LeaderboardWorkflow, mock.Anything, mock.Anything).Return(nil)
	return env
}

func boothInput() BoothEventInput {
	return BoothEventInput{ID: "e1e1e1e1", LeadsEndpoint: "leads", ControlTaskQueue: "podpocalypse-control", Duration: time.Minute}
}

// step sends one Update at time at, and hands its result to check.
func step(t *testing.T, env *testsuite.TestWorkflowEnvironment, at time.Duration, name string, check func(result any, err error), args ...any) {
	t.Helper()
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(name, name+at.String(), &testsuite.TestUpdateCallback{
			OnReject:   func(err error) { check(nil, err) },
			OnAccept:   func() {},
			OnComplete: func(result any, err error) { check(result, err) },
		}, args...)
	}, at)
}

func ok(t *testing.T) func(any, error) {
	return func(_ any, err error) { require.NoError(t, err) }
}

func TestBoothEventOpensWithTheBoothsDefaults(t *testing.T) {
	tracker := &fakeTracker{}
	env := boothEnv(t, tracker)
	step(t, env, time.Millisecond, UpdateOpen, ok(t), OpenInput{Name: "KubeCon 2026", CreatedBy: "staff@temporal.io"})
	step(t, env, time.Second, UpdateClose, ok(t))
	env.ExecuteWorkflow(BoothEventWorkflow, boothInput())
	require.NoError(t, env.GetWorkflowError())

	require.Len(t, tracker.created, 1)
	created := tracker.created[0]
	require.Equal(t, "e1e1e1e1", created.RequestID)
	require.Equal(t, "KubeCon 2026", created.Name)
	require.Equal(t, "staff@temporal.io", created.CreatedBy)
	var ids []string
	for _, f := range created.Form.Sections[0].Fields {
		ids = append(ids, f.ID)
		require.True(t, f.Required)
	}
	require.Equal(t, []string{"name", "role", "email"}, ids)
	end, err := time.Parse("2006-01-02", created.EndDate)
	require.NoError(t, err)
	require.True(t, end.After(env.Now().UTC().AddDate(0, 0, 2)))
}

func TestBoothEventQueuesEachPersonOnce(t *testing.T) {
	tracker := &fakeTracker{}
	env := boothEnv(t, tracker)
	step(t, env, time.Millisecond, UpdateOpen, ok(t), OpenInput{Name: "KubeCon 2026"})
	var places []JoinResult
	record := func(result any, err error) {
		require.NoError(t, err)
		places = append(places, result.(JoinResult))
	}
	step(t, env, 1*time.Second, UpdateJoin, record, JoinInput{Name: "Ada Lovelace", Title: "SA", Email: "ada@temporal.io", BrowserID: "phone-a"})
	// The same email again, with different capitals.
	step(t, env, 2*time.Second, UpdateJoin, record, JoinInput{Name: "Ada Lovelace", Title: "SA", Email: " ADA@temporal.io", BrowserID: "phone-a"})
	// The same phone under another email.
	step(t, env, 3*time.Second, UpdateJoin, record, JoinInput{Name: "Ada L", Title: "SA", Email: "ada@example.com", BrowserID: "phone-a"})
	step(t, env, 4*time.Second, UpdateJoin, record, JoinInput{Name: "Grace Hopper", Title: "Admiral", Email: "grace@example.com", BrowserID: "phone-g"})
	var view BoothEventView
	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow(QueryBoothEvent)
		require.NoError(t, err)
		require.NoError(t, val.Get(&view))
	}, 5*time.Second)
	step(t, env, 6*time.Second, UpdateClose, ok(t))
	env.ExecuteWorkflow(BoothEventWorkflow, boothInput())
	require.NoError(t, env.GetWorkflowError())

	require.Equal(t, []int{1, 1, 1, 2}, []int{places[0].Position, places[1].Position, places[2].Position, places[3].Position})
	require.Equal(t, places[0].Ticket, places[1].Ticket)
	require.Len(t, view.Queue, 2)
	require.Len(t, tracker.submits, 2, "a person already queued is not submitted again")
	require.Equal(t, "kubecon-2026", tracker.submits[0].EventID)
	require.Len(t, tracker.submits[0].Submission.RequestID, 32)
}

func TestScreenCallsSkipsAndStartsTurns(t *testing.T) {
	tracker := &fakeTracker{}
	env := boothEnv(t, tracker)
	var flights []RunInput
	env.OnWorkflow(GameRunWorkflow, mock.Anything, mock.Anything).Return(func(_ workflow.Context, in RunInput) (RunResult, error) {
		flights = append(flights, in)
		return RunResult{}, nil
	}).After(10 * time.Second)

	step(t, env, time.Millisecond, UpdateOpen, ok(t), OpenInput{Name: "KubeCon 2026"})
	step(t, env, 1*time.Second, UpdateJoin, ok(t), JoinInput{Name: "Ada Lovelace", Title: "SA", Email: "ada@temporal.io"})
	step(t, env, 2*time.Second, UpdateJoin, ok(t), JoinInput{Name: "Grace Hopper", Title: "Admiral", Email: "grace@example.com"})

	var called []string
	callNext := func(result any, err error) {
		require.NoError(t, err)
		next := result.(*ViewEntry)
		if next == nil {
			called = append(called, "")
			return
		}
		called = append(called, next.Name)
	}
	step(t, env, 3*time.Second, UpdateCallNext, callNext)
	// A reloaded screen gets the same person back.
	step(t, env, 4*time.Second, UpdateCallNext, callNext)
	// A stale skip names someone who isn't being called.
	step(t, env, 5*time.Second, UpdateSkip, func(_ any, err error) { require.Error(t, err) }, "e2")
	step(t, env, 6*time.Second, UpdateSkip, ok(t), "e1")
	step(t, env, 7*time.Second, UpdateCallNext, callNext)
	step(t, env, 8*time.Second, UpdateStartTurn, func(result any, err error) {
		require.NoError(t, err)
		require.Equal(t, "Grace Hopper", result.(TurnStarted).Entry.Name)
	}, "e2")
	// The screen can't call anyone while a flight is running.
	step(t, env, 9*time.Second, UpdateCallNext, func(_ any, err error) { require.Error(t, err) })
	// Once the flight is over, Grace can join again, at the back.
	step(t, env, 30*time.Second, UpdateJoin, func(result any, err error) {
		require.NoError(t, err)
		require.Equal(t, 1, result.(JoinResult).Position)
	}, JoinInput{Name: "Grace Hopper", Title: "Admiral", Email: "grace@example.com"})
	step(t, env, 31*time.Second, UpdateClose, ok(t))
	env.ExecuteWorkflow(BoothEventWorkflow, boothInput())
	require.NoError(t, env.GetWorkflowError())

	require.Equal(t, []string{"Ada Lovelace", "Ada Lovelace", "Grace Hopper"}, called)
	require.Len(t, flights, 1)
	require.Equal(t, "grace@example.com", flights[0].Email)
	require.Equal(t, "leaderboard-e1e1e1e1", flights[0].LeaderboardID)
}
