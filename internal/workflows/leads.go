package workflows

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// The event tracker's Nexus service. It stores events and their responses and
// knows nothing about this game; everything game-shaped stays on this side.
const (
	LeadServiceName      = "event-leads"
	opCreateEvent        = "create_event"
	opSubmitParticipant  = "submit_participant"
	defaultEventDuration = 3 // days, counted in UTC
)

// LeadEvent is the tracker's view of an event.
type LeadEvent struct {
	ID       string    `json:"id"`
	Link     string    `json:"link"`
	Name     string    `json:"name"`
	Status   string    `json:"status"`
	ClosesAt time.Time `json:"closesAt"`
}

type leadForm struct {
	Sections []leadSection `json:"sections"`
}

type leadSection struct {
	ID     string      `json:"id"`
	Title  string      `json:"title"`
	Fields []leadField `json:"fields"`
}

type leadField struct {
	ID       string `json:"id"`
	Label    string `json:"label"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

type createLeadEvent struct {
	RequestID string   `json:"requestId"`
	CreatedBy string   `json:"createdBy"`
	Name      string   `json:"name"`
	EndDate   string   `json:"endDate"`
	Form      leadForm `json:"form"`
}

type leadSubmission struct {
	Name      string `json:"name"`
	Role      string `json:"role"`
	Email     string `json:"email"`
	RequestID string `json:"requestId"`
}

type submitLead struct {
	EventID    string         `json:"eventId"`
	Submission leadSubmission `json:"submission"`
}

// defaultLeadEvent is the event the booth opens: the title from the screen and
// a form with only name, title, and email. The tracker reads the field IDs
// name, role, and email as its identity fields. Submissions close at 00:00 UTC
// after the end date, so "today" would close mid-afternoon at a US booth.
func defaultLeadEvent(requestID, name, createdBy string, now time.Time) createLeadEvent {
	return createLeadEvent{
		RequestID: requestID,
		CreatedBy: createdBy,
		Name:      name,
		EndDate:   now.UTC().AddDate(0, 0, defaultEventDuration).Format("2006-01-02"),
		Form: leadForm{Sections: []leadSection{{ID: "details", Title: "Your details", Fields: []leadField{
			{ID: "name", Label: "Name", Type: "short_text", Required: true},
			{ID: "role", Label: "Title", Type: "short_text", Required: true},
			{ID: "email", Label: "Email", Type: "email", Required: true},
		}}}},
	}
}

// leadRequestID is the same for the same person and details, so joining again
// with the same details hits the tracker's stored receipt and writes nothing.
func leadRequestID(eventID, email, name, title string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{eventID, strings.ToLower(strings.TrimSpace(email)), name, title}, "\n")))
	return hex.EncodeToString(sum[:16])
}

func callLeads(ctx workflow.Context, endpoint, operation string, input, output any) error {
	if endpoint == "" {
		return temporal.NewNonRetryableApplicationError("this booth is not connected to the event tracker", "Unconfigured", nil)
	}
	future := workflow.NewNexusClient(endpoint, LeadServiceName).ExecuteOperation(ctx, operation, input, workflow.NexusOperationOptions{
		ScheduleToCloseTimeout: 30 * time.Second,
	})
	return future.Get(ctx, output)
}
