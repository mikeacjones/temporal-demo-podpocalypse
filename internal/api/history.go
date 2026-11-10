package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	"podpocalypse/internal/names"
)

// maxHistoryStreams bounds the long polls the booth holds open on Temporal.
// Each game screen opens one for its flight.
const maxHistoryStreams = 16

// HistoryEvent is one event of a session's Event History, cut down to what
// the screen draws.
type HistoryEvent struct {
	ID   int64  `json:"id"`
	Type string `json:"type"`
	// Name is the Activity type or Update name the event belongs to.
	Name string `json:"name,omitempty"`
	// Identity is the worker that picked up a Workflow or Activity Task.
	Identity string `json:"identity,omitempty"`
}

// sessionHistory streams the session workflow's real Event History as
// Server-Sent Events: what is there already, then each new event as Temporal
// writes it. It long-polls history, so it keeps flowing while the session
// worker is down, and it ends when the workflow closes.
func (s *Server) sessionHistory(w http.ResponseWriter, r *http.Request) {
	id, ok := sessionID(w, r)
	if !ok {
		return
	}
	select {
	case s.historySlots <- struct{}{}:
		defer func() { <-s.historySlots }()
	default:
		writeErr(w, http.StatusTooManyRequests, "too many history viewers")
		return
	}
	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if err := rc.Flush(); err != nil {
		return
	}

	// A flight lasts minutes; nothing should hold a long poll longer.
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Minute)
	defer cancel()
	iter := s.Client.GetWorkflowHistory(ctx, names.SessionWorkflowID(id), "", true, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	labels := map[string]string{}
	for iter.HasNext() {
		event, err := iter.Next()
		if err != nil {
			return
		}
		data, err := json.Marshal(compactEvent(event, labels))
		if err != nil {
			return
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
			return
		}
		if err := rc.Flush(); err != nil {
			return
		}
	}
	_, _ = fmt.Fprint(w, "event: closed\ndata: {}\n\n")
	_ = rc.Flush()
}

// compactEvent names an event's Activity or Update. Later events refer back
// to the one that scheduled or accepted them, so labels remembers each name
// by scheduled event ID or update ID.
func compactEvent(e *historypb.HistoryEvent, labels map[string]string) HistoryEvent {
	out := HistoryEvent{ID: e.GetEventId(), Type: e.GetEventType().String()}
	switch e.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED:
		out.Identity = e.GetWorkflowTaskStartedEventAttributes().GetIdentity()
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
		out.Name = e.GetActivityTaskScheduledEventAttributes().GetActivityType().GetName()
		labels[fmt.Sprint("activity-", e.GetEventId())] = out.Name
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:
		a := e.GetActivityTaskStartedEventAttributes()
		out.Name = labels[fmt.Sprint("activity-", a.GetScheduledEventId())]
		out.Identity = a.GetIdentity()
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		out.Name = labels[fmt.Sprint("activity-", e.GetActivityTaskCompletedEventAttributes().GetScheduledEventId())]
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:
		out.Name = labels[fmt.Sprint("activity-", e.GetActivityTaskFailedEventAttributes().GetScheduledEventId())]
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
		out.Name = labels[fmt.Sprint("activity-", e.GetActivityTaskTimedOutEventAttributes().GetScheduledEventId())]
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
		req := e.GetWorkflowExecutionUpdateAcceptedEventAttributes().GetAcceptedRequest()
		out.Name = req.GetInput().GetName()
		labels["update-"+req.GetMeta().GetUpdateId()] = out.Name
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED:
		out.Name = labels["update-"+e.GetWorkflowExecutionUpdateCompletedEventAttributes().GetMeta().GetUpdateId()]
	}
	return out
}
