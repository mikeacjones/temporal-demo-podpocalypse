package api

import (
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	updatepb "go.temporal.io/api/update/v1"

	"podpocalypse/internal/workflows"
)

func TestCompactEventNamesActivitiesAndUpdates(t *testing.T) {
	labels := map[string]string{}
	events := []*historypb.HistoryEvent{
		{EventId: 5, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{ActivityType: &commonpb.ActivityType{Name: "DeleteSessionPod"}},
		}},
		{EventId: 6, EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
			ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{ScheduledEventId: 5, Identity: "booth-1"},
		}},
		{EventId: 7, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED, Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateAcceptedEventAttributes{
			WorkflowExecutionUpdateAcceptedEventAttributes: &historypb.WorkflowExecutionUpdateAcceptedEventAttributes{AcceptedRequest: &updatepb.Request{
				Meta: &updatepb.Meta{UpdateId: "u1"}, Input: &updatepb.Input{Name: "pass_gate"},
			}},
		}},
		{EventId: 8, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED, Attributes: &historypb.HistoryEvent_WorkflowExecutionUpdateCompletedEventAttributes{
			WorkflowExecutionUpdateCompletedEventAttributes: &historypb.WorkflowExecutionUpdateCompletedEventAttributes{Meta: &updatepb.Meta{UpdateId: "u1"}},
		}},
		{EventId: 9, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
			WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{Identity: "sessions-abc"},
		}},
	}
	want := []HistoryEvent{
		{ID: 5, Type: "ActivityTaskScheduled", Name: "DeleteSessionPod"},
		{ID: 6, Type: "ActivityTaskStarted", Name: "DeleteSessionPod", Identity: "booth-1"},
		{ID: 7, Type: "WorkflowExecutionUpdateAccepted", Name: "pass_gate"},
		{ID: 8, Type: "WorkflowExecutionUpdateCompleted", Name: "pass_gate"},
		{ID: 9, Type: "WorkflowTaskStarted", Identity: "sessions-abc"},
	}
	for i, e := range events {
		if got := compactEvent(e, labels); got != want[i] {
			t.Errorf("event %d: got %+v, want %+v", e.EventId, got, want[i])
		}
	}
}

func TestPublicBoardShowsFirstNamesOnly(t *testing.T) {
	got := publicBoard([]workflows.BoardEntry{
		{Name: "Ada Lovelace", Title: "SA", Score: 12},
		{Name: "  Grace  ", Title: "Admiral", Score: 9},
	})
	if len(got) != 2 || got[0].Name != "Ada" || got[1].Name != "Grace" || got[0].Score != 12 {
		t.Fatalf("public board: %+v", got)
	}
}
