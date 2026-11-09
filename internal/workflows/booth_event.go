package workflows

import (
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"podpocalypse/internal/names"
)

const (
	UpdateOpen      = "open"
	UpdateJoin      = "join"
	UpdateCallNext  = "call_next"
	UpdateStartTurn = "start_turn"
	UpdateSkip      = "skip"
	UpdateClose     = "close"
	QueryBoothEvent = "state"

	TurnIdle    = "idle"
	TurnCalling = "calling"
	TurnFlying  = "flying"
)

// BoothEventInput starts one event's queue. State is set only when the
// workflow continues as new.
type BoothEventInput struct {
	ID               string
	LeadsEndpoint    string
	ControlTaskQueue string
	Duration         time.Duration
	State            *BoothEventState
}

type BoothEventState struct {
	Event   LeadEvent
	Queue   []QueueEntry
	Turn    Turn
	Joins   int
	Closing bool
}

// QueueEntry is one place in the queue. The email stays in workflow state
// for the game's lead record; the view sent to browsers never carries it.
type QueueEntry struct {
	EntryID   string
	PlayerID  string
	Name      string
	Title     string
	Email     string
	BrowserID string
}

// Turn is the person the screen has called, or the one flying.
type Turn struct {
	Phase     string
	Entry     *QueueEntry
	SessionID string
}

type OpenInput struct {
	Name      string
	CreatedBy string
}

type JoinInput struct {
	Name      string
	Title     string
	Email     string
	BrowserID string
}

// JoinResult is the phone's ticket. Position 0 means called or flying.
type JoinResult struct {
	Ticket   string `json:"ticket"`
	Position int    `json:"position"`
}

type TurnStarted struct {
	SessionID string
	Entry     QueueEntry
}

// BoothEventView is the screen's and phones' view of the queue.
type BoothEventView struct {
	ID       string      `json:"id"`
	Name     string      `json:"name"`
	Link     string      `json:"link"`
	Open     bool        `json:"open"`
	Phase    string      `json:"phase"`
	Closing  bool        `json:"closing"`
	Current  *ViewEntry  `json:"current,omitempty"`
	Session  string      `json:"sessionId,omitempty"`
	Queue    []ViewEntry `json:"queue"`
	Finished bool        `json:"finished,omitempty"`
}

type ViewEntry struct {
	EntryID string `json:"entryId"`
	Ticket  string `json:"ticket"`
	Name    string `json:"name"`
	Title   string `json:"title"`
}

type boothEvent struct {
	in    BoothEventInput
	state BoothEventState
}

// BoothEventWorkflow is one event this booth opened: its tracker event and its
// queue. It has no timer of its own. The booth screen drives each turn with
// call_next, then start_turn or skip, so nobody is called while the screen is
// closed. The workflow keeps the order and the rules.
func BoothEventWorkflow(ctx workflow.Context, in BoothEventInput) error {
	if in.ID == "" || in.ControlTaskQueue == "" || in.Duration <= 0 {
		return temporal.NewNonRetryableApplicationError("booth event input is incomplete", "BadInput", nil)
	}
	b := &boothEvent{in: in}
	if in.State != nil {
		b.state = *in.State
	}
	if b.state.Turn.Phase == "" {
		b.state.Turn.Phase = TurnIdle
	}
	opening, openFailed := false, false

	if err := workflow.SetQueryHandler(ctx, QueryBoothEvent, func() (BoothEventView, error) {
		return b.view(), nil
	}); err != nil {
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateOpen,
		func(ctx workflow.Context, input OpenInput) (BoothEventView, error) {
			opening = true
			event, err := b.open(ctx, input)
			opening = false
			if err != nil {
				openFailed = true
				return BoothEventView{}, err
			}
			b.state.Event = event
			workflow.SetCurrentDetails(ctx, b.details())
			return b.view(), nil
		},
		workflow.UpdateHandlerOptions{Validator: func(input OpenInput) error {
			if b.opened() || opening {
				return errors.New("this event is already open")
			}
			if n := len(strings.TrimSpace(input.Name)); n < 1 || n > 120 {
				return errors.New("the event title must contain 1–120 characters")
			}
			return nil
		}},
	); err != nil {
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateJoin,
		func(ctx workflow.Context, input JoinInput) (JoinResult, error) {
			return b.join(ctx, input)
		},
		workflow.UpdateHandlerOptions{Validator: func(input JoinInput) error {
			if err := b.accepting(); err != nil {
				return err
			}
			return validJoin(input)
		}},
	); err != nil {
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateCallNext,
		func(ctx workflow.Context) (*ViewEntry, error) {
			if b.state.Turn.Phase == TurnIdle {
				if b.state.Closing || len(b.state.Queue) == 0 {
					return nil, nil
				}
				next := b.state.Queue[0]
				b.state.Queue = b.state.Queue[1:]
				b.state.Turn = Turn{Phase: TurnCalling, Entry: &next}
				workflow.SetCurrentDetails(ctx, b.details())
			}
			// Already calling: a reloaded screen gets the same person back.
			entry := viewEntry(*b.state.Turn.Entry)
			return &entry, nil
		},
		workflow.UpdateHandlerOptions{Validator: func() error {
			if !b.opened() {
				return errors.New("this event is not open yet")
			}
			if b.state.Turn.Phase == TurnFlying {
				return errors.New("a flight is still running")
			}
			return nil
		}},
	); err != nil {
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateStartTurn,
		func(ctx workflow.Context, entryID string) (TurnStarted, error) {
			return b.startTurn(ctx)
		},
		workflow.UpdateHandlerOptions{Validator: b.calling},
	); err != nil {
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateSkip,
		func(ctx workflow.Context, entryID string) (BoothEventView, error) {
			b.state.Turn = Turn{Phase: TurnIdle}
			workflow.SetCurrentDetails(ctx, b.details())
			return b.view(), nil
		},
		workflow.UpdateHandlerOptions{Validator: b.calling},
	); err != nil {
		return err
	}

	if err := workflow.SetUpdateHandlerWithOptions(ctx, UpdateClose,
		func(ctx workflow.Context) (BoothEventView, error) {
			b.state.Closing = true
			workflow.SetCurrentDetails(ctx, b.details())
			return b.view(), nil
		},
		workflow.UpdateHandlerOptions{Validator: func() error { return b.accepting() }},
	); err != nil {
		return err
	}

	// A new run waits for the open Update that came with its start.
	if !b.opened() {
		ok, err := workflow.AwaitWithTimeout(ctx, 2*time.Minute, func() bool { return b.opened() || openFailed })
		if err != nil {
			return err
		}
		if !ok || !b.opened() {
			return nil
		}
	}

	for {
		if err := workflow.Await(ctx, func() bool {
			return b.done() || b.rotate(ctx)
		}); err != nil {
			return err
		}
		if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
			return err
		}
		if b.done() {
			return nil
		}
		// Only between turns, so no flight is ever carried across.
		if b.rotate(ctx) {
			state := b.state
			next := in
			next.State = &state
			return workflow.NewContinueAsNewError(ctx, BoothEventWorkflow, next)
		}
	}
}

func (b *boothEvent) opened() bool { return b.state.Event.ID != "" }

func (b *boothEvent) done() bool { return b.state.Closing && b.state.Turn.Phase != TurnFlying }

func (b *boothEvent) rotate(ctx workflow.Context) bool {
	return b.state.Turn.Phase == TurnIdle && workflow.GetInfo(ctx).GetContinueAsNewSuggested()
}

func (b *boothEvent) accepting() error {
	if !b.opened() {
		return errors.New("this event is not open yet")
	}
	if b.state.Closing {
		return errors.New("this event has closed")
	}
	return nil
}

// calling validates start_turn and skip. Both name the entry, so a late skip
// from a stale tab can't remove the person called after them.
func (b *boothEvent) calling(entryID string) error {
	if b.state.Turn.Phase != TurnCalling || b.state.Turn.Entry == nil || b.state.Turn.Entry.EntryID != entryID {
		return errors.New("that person is no longer being called")
	}
	return nil
}

// open creates the event in the tracker. The booth event ID is the request ID,
// so a retried Nexus call returns the same event.
func (b *boothEvent) open(ctx workflow.Context, input OpenInput) (LeadEvent, error) {
	var event LeadEvent
	create := defaultLeadEvent(b.in.ID, strings.TrimSpace(input.Name), input.CreatedBy, workflow.Now(ctx))
	if err := callLeads(ctx, b.in.LeadsEndpoint, opCreateEvent, create, &event); err != nil {
		return LeadEvent{}, rejected("the event tracker did not create the event", err)
	}
	// The event's standings outlive this run: Continue-As-New and the close of
	// this workflow leave them running.
	boardCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID:            names.EventLeaderboardID(b.in.ID),
		TaskQueue:             b.in.ControlTaskQueue,
		ParentClosePolicy:     enumspb.PARENT_CLOSE_POLICY_ABANDON,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
	})
	board := workflow.ExecuteChildWorkflow(boardCtx, LeaderboardWorkflow, Board{})
	if err := board.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		return LeadEvent{}, err
	}
	return event, nil
}

func (b *boothEvent) join(ctx workflow.Context, input JoinInput) (JoinResult, error) {
	input.Name, input.Title = strings.TrimSpace(input.Name), strings.TrimSpace(input.Title)
	input.Email = strings.ToLower(strings.TrimSpace(input.Email))
	playerID := names.PlayerWorkflowID(input.Email)
	if result, ok := b.place(playerID, input.BrowserID); ok {
		return result, nil
	}
	submit := submitLead{EventID: b.state.Event.ID, Submission: leadSubmission{
		Name: input.Name, Role: input.Title, Email: input.Email,
		RequestID: leadRequestID(b.state.Event.ID, input.Email, input.Name, input.Title),
	}}
	var status struct {
		Status string `json:"status"`
	}
	if err := callLeads(ctx, b.in.LeadsEndpoint, opSubmitParticipant, submit, &status); err != nil {
		return JoinResult{}, rejected("the event tracker did not save your details", err)
	}
	// Another join for the same person may have finished while this one waited.
	if result, ok := b.place(playerID, input.BrowserID); ok {
		return result, nil
	}
	b.state.Joins++
	b.state.Queue = append(b.state.Queue, QueueEntry{
		EntryID:   fmt.Sprintf("e%d", b.state.Joins),
		PlayerID:  playerID,
		Name:      input.Name,
		Title:     input.Title,
		Email:     input.Email,
		BrowserID: input.BrowserID,
	})
	workflow.SetCurrentDetails(ctx, b.details())
	return JoinResult{Ticket: playerID, Position: len(b.state.Queue)}, nil
}

// place finds someone already waiting, called, or flying, by email or browser.
func (b *boothEvent) place(playerID, browserID string) (JoinResult, bool) {
	same := func(e QueueEntry) bool {
		return e.PlayerID == playerID || (browserID != "" && e.BrowserID == browserID)
	}
	if e := b.state.Turn.Entry; e != nil && same(*e) {
		return JoinResult{Ticket: e.PlayerID, Position: 0}, true
	}
	for i, e := range b.state.Queue {
		if same(e) {
			return JoinResult{Ticket: e.PlayerID, Position: i + 1}, true
		}
	}
	return JoinResult{}, false
}

// startTurn starts the called person's flight as a child. When it completes,
// the turn is idle again and the person can rejoin; the screen calls the next.
func (b *boothEvent) startTurn(ctx workflow.Context) (TurnStarted, error) {
	entry := *b.state.Turn.Entry
	b.state.Turn.Phase = TurnFlying
	var sessionID string
	if err := workflow.SideEffect(ctx, func(workflow.Context) any {
		id, err := names.NewSessionID()
		if err != nil {
			return ""
		}
		return id
	}).Get(&sessionID); err != nil || sessionID == "" {
		b.state.Turn.Phase = TurnCalling
		return TurnStarted{}, errors.New("could not start a session")
	}
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: names.RunWorkflowID(sessionID),
		TaskQueue:  b.in.ControlTaskQueue,
		// Nothing about this workflow's run may end a flight.
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
	})
	run := workflow.ExecuteChildWorkflow(childCtx, GameRunWorkflow, RunInput{
		SessionID:        sessionID,
		PlayerID:         entry.PlayerID,
		Name:             entry.Name,
		Title:            entry.Title,
		Email:            entry.Email,
		Duration:         b.in.Duration,
		ControlTaskQueue: b.in.ControlTaskQueue,
		LeaderboardID:    names.EventLeaderboardID(b.in.ID),
	})
	if err := run.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		b.state.Turn.Phase = TurnCalling
		return TurnStarted{}, err
	}
	b.state.Turn.SessionID = sessionID
	workflow.SetCurrentDetails(ctx, b.details())
	workflow.Go(ctx, func(ctx workflow.Context) {
		if err := run.Get(ctx, nil); err != nil {
			workflow.GetLogger(ctx).Warn("flight ended with an error", "sessionId", sessionID, "error", err)
		}
		b.state.Turn = Turn{Phase: TurnIdle}
		workflow.SetCurrentDetails(ctx, b.details())
	})
	return TurnStarted{SessionID: sessionID, Entry: entry}, nil
}

func (b *boothEvent) view() BoothEventView {
	v := BoothEventView{
		ID:      b.in.ID,
		Name:    b.state.Event.Name,
		Link:    b.state.Event.Link,
		Open:    b.opened(),
		Phase:   b.state.Turn.Phase,
		Closing: b.state.Closing,
		Session: b.state.Turn.SessionID,
		Queue:   make([]ViewEntry, 0, len(b.state.Queue)),
	}
	if e := b.state.Turn.Entry; e != nil {
		current := viewEntry(*e)
		v.Current = &current
	}
	for _, e := range b.state.Queue {
		v.Queue = append(v.Queue, viewEntry(e))
	}
	return v
}

func viewEntry(e QueueEntry) ViewEntry {
	return ViewEntry{EntryID: e.EntryID, Ticket: e.PlayerID, Name: e.Name, Title: e.Title}
}

func (b *boothEvent) details() string {
	current := "nobody called"
	if e := b.state.Turn.Entry; e != nil {
		current = fmt.Sprintf("%s %s", e.Name, b.state.Turn.Phase)
	}
	return fmt.Sprintf("**%s** · %d in queue · %s", b.state.Event.Name, len(b.state.Queue), current)
}

func validJoin(in JoinInput) error {
	if n := len(strings.TrimSpace(in.Name)); n < 1 || n > 80 {
		return errors.New("name is required")
	}
	if n := len(strings.TrimSpace(in.Title)); n < 1 || n > 80 {
		return errors.New("title is required")
	}
	email := strings.ToLower(strings.TrimSpace(in.Email))
	address, err := mail.ParseAddress(email)
	if err != nil || address.Address != email || !strings.Contains(email[strings.LastIndex(email, "@")+1:], ".") {
		return errors.New("enter a valid email address")
	}
	if len(in.BrowserID) > 64 {
		return errors.New("browser id is too long")
	}
	return nil
}

// rejected keeps the tracker's own reason, which is what the person needs to
// see ("this event is no longer accepting submissions").
func rejected(fallback string, err error) error {
	msg := fallback
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		msg = cause.Error()
	}
	if i := strings.LastIndex(msg, "): "); i >= 0 {
		msg = msg[i+3:]
	}
	if strings.TrimSpace(msg) == "" {
		msg = fallback
	}
	return temporal.NewNonRetryableApplicationError(msg, "Rejected", nil)
}
