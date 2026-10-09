package workflows

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func TestKioskRemembersTheCheckedInEvent(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateBind, "bind-kubecon", &testsuite.TestUpdateCallback{
			OnReject:   func(err error) { t.Error(err) },
			OnComplete: func(any, error) {},
		}, BoothState{EventID: "kubecon", EventName: "KubeCon"})
	}, time.Second)
	var got BoothState
	env.RegisterDelayedCallback(func() {
		val, err := env.QueryWorkflow(QueryBooth)
		require.NoError(t, err)
		require.NoError(t, val.Get(&got))
		env.CancelWorkflow()
	}, 2*time.Second)
	env.ExecuteWorkflow(KioskWorkflow)
	require.Error(t, env.GetWorkflowError())
	require.Equal(t, "kubecon", got.EventID)
	require.Equal(t, "KubeCon", got.EventName)
}
