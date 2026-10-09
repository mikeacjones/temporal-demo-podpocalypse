package workflows

import (
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	LeadServiceName = "event-leads"
	QueryBooth      = "booth"
	UpdateBind      = "bind"
	UpdateClear     = "clear"
)

type BoothState struct {
	EventID   string `json:"eventId"`
	EventName string `json:"eventName"`
	FormURL   string `json:"formURL"`
	QRURL     string `json:"qrURL"`
}

type LeadEvent struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
}

type leadList struct {
	Events []LeadEvent `json:"events"`
}

type FlightProfile struct {
	EventID string `json:"eventId"`
	Name    string `json:"name"`
	Role    string `json:"role"`
	Email   string `json:"email"`
}

type ClaimFlightInput struct {
	Endpoint string
	EventID  string
	Code     string
}

func KioskWorkflow(ctx workflow.Context) error {
	var state BoothState
	if err := workflow.SetQueryHandler(ctx, QueryBooth, func() (BoothState, error) {
		return state, nil
	}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandler(ctx, UpdateBind, func(_ workflow.Context, next BoothState) (BoothState, error) {
		if next.EventID == "" || next.EventName == "" {
			return BoothState{}, temporal.NewApplicationError("choose an open event", "Invalid")
		}
		state = next
		return state, nil
	}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandler(ctx, UpdateClear, func(workflow.Context) (BoothState, error) {
		state = BoothState{}
		return state, nil
	}); err != nil {
		return err
	}
	return workflow.Await(ctx, func() bool { return false })
}

func ListLeadEvents(ctx workflow.Context, endpoint string) ([]LeadEvent, error) {
	var out leadList
	if err := callLeads(ctx, endpoint, "list_events", struct{}{}, &out); err != nil {
		return nil, err
	}
	open := make([]LeadEvent, 0, len(out.Events))
	for _, event := range out.Events {
		if event.Status == "open" && event.ID != "" {
			open = append(open, event)
		}
	}
	return open, nil
}

func GetLeadEvent(ctx workflow.Context, endpoint, eventID string) (LeadEvent, error) {
	var event LeadEvent
	err := callLeads(ctx, endpoint, "get_event", map[string]string{"eventId": eventID}, &event)
	return event, err
}

func ClaimFlight(ctx workflow.Context, in ClaimFlightInput) (FlightProfile, error) {
	var profile FlightProfile
	err := callLeads(ctx, in.Endpoint, "claim_flight_pass", map[string]string{
		"eventId": in.EventID,
		"code":    in.Code,
	}, &profile)
	return profile, err
}

func callLeads(ctx workflow.Context, endpoint, operation string, input, output any) error {
	if endpoint == "" {
		return temporal.NewNonRetryableApplicationError("event leads are not configured", "Unconfigured", nil)
	}
	future := workflow.NewNexusClient(endpoint, LeadServiceName).ExecuteOperation(ctx, operation, input, workflow.NexusOperationOptions{})
	return future.Get(ctx, output)
}
