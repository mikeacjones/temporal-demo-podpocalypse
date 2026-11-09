package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	"podpocalypse/internal/names"
	"podpocalypse/internal/qrcode"
	"podpocalypse/internal/workflows"
)

// kiosk tells the screen whether a signed-in employee is at it.
func (s *Server) kiosk(w http.ResponseWriter, r *http.Request) {
	employee, _ := s.employee(r)
	writeJSON(w, http.StatusOK, map[string]any{
		"employee": employee != "",
		"leads":    s.Config.LeadsEndpoint != "",
	})
}

type boothEventSummary struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// listBoothEvents is the events this booth opened and that are still running,
// read from Visibility. The name is in each workflow's memo.
func (s *Server) listBoothEvents(w http.ResponseWriter, r *http.Request) {
	if !s.requireEmployee(w, r) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	resp, err := s.Client.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.Config.TemporalNamespace,
		PageSize:  50,
		Query:     "WorkflowType = 'BoothEventWorkflow' AND ExecutionStatus = 'Running'",
	})
	if err != nil {
		writeErr(w, http.StatusBadGateway, "could not list events")
		return
	}
	events := []boothEventSummary{}
	for _, info := range resp.GetExecutions() {
		id := strings.TrimPrefix(info.GetExecution().GetWorkflowId(), names.BoothEventWorkflowID(""))
		var name string
		if field := info.GetMemo().GetFields()["name"]; field != nil {
			_ = converter.GetDefaultDataConverter().FromPayload(field, &name)
		}
		events = append(events, boothEventSummary{ID: id, Name: name})
	}
	writeJSON(w, http.StatusOK, events)
}

// openBoothEvent starts a booth event with its open Update in one call. The
// open handler creates the event in the tracker over Nexus.
func (s *Server) openBoothEvent(w http.ResponseWriter, r *http.Request) {
	employee, err := s.employee(r)
	if err != nil || employee == "" {
		s.requireEmployee(w, r)
		return
	}
	if s.Config.LeadsEndpoint == "" {
		writeErr(w, http.StatusConflict, "this booth is not connected to the event tracker")
		return
	}
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<12)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "the form could not be read")
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" || len(req.Name) > 120 {
		writeErr(w, http.StatusBadRequest, "give the event a title")
		return
	}
	id, err := names.NewSessionID()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not open an event")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	start := s.Client.NewWithStartWorkflowOperation(client.StartWorkflowOptions{
		ID:        names.BoothEventWorkflowID(id),
		TaskQueue: s.Config.ControlQueue,
		Memo:      map[string]any{"name": req.Name},
		// The ID is new, so an existing workflow would be a bug, not a retry.
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
	}, workflows.BoothEventWorkflow, workflows.BoothEventInput{
		ID:               id,
		LeadsEndpoint:    s.Config.LeadsEndpoint,
		ControlTaskQueue: s.Config.ControlQueue,
		Duration:         s.Config.GameDuration,
	})
	handle, err := s.Client.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{
		StartWorkflowOperation: start,
		UpdateOptions: client.UpdateWorkflowOptions{
			UpdateName: workflows.UpdateOpen, UpdateID: "open-" + id,
			Args: []any{workflows.OpenInput{Name: req.Name, CreatedBy: employee}}, WaitForStage: client.WorkflowUpdateStageCompleted,
		},
	})
	var view workflows.BoothEventView
	if err == nil {
		err = handle.Get(ctx, &view)
	}
	if err != nil {
		writeUpdateErr(w, err, "could not open the event")
		return
	}
	writeJSON(w, http.StatusCreated, publicView(view))
}

// boothEvent is the screen's and phones' read. It comes from the shared cache,
// one Query per event per second however many phones are polling.
func (s *Server) boothEvent(w http.ResponseWriter, r *http.Request) {
	id, ok := boothEventID(w, r)
	if !ok {
		return
	}
	view, err := s.views.get(r.Context(), id, s.queryBoothEvent)
	if err != nil {
		var missing *serviceerror.NotFound
		if errors.As(err, &missing) {
			writeErr(w, http.StatusNotFound, "no such event")
			return
		}
		writeErr(w, http.StatusServiceUnavailable, "the event is not answering yet")
		return
	}
	if ticket := r.URL.Query().Get("ticket"); ticket != "" {
		writeJSON(w, http.StatusOK, ticketView(view, ticket))
		return
	}
	writeJSON(w, http.StatusOK, publicView(view))
}

func (s *Server) queryBoothEvent(ctx context.Context, id string) (workflows.BoothEventView, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var view workflows.BoothEventView
	val, err := s.Client.QueryWorkflow(ctx, names.BoothEventWorkflowID(id), "", workflows.QueryBoothEvent)
	if err != nil {
		return view, err
	}
	err = val.Get(&view)
	return view, err
}

func (s *Server) joinBoothEvent(w http.ResponseWriter, r *http.Request) {
	id, ok := boothEventID(w, r)
	if !ok {
		return
	}
	var req workflows.JoinInput
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<12)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "the form could not be read")
		return
	}
	var result workflows.JoinResult
	if !s.boothUpdate(w, r, id, workflows.UpdateJoin, 25*time.Second, &result, req) {
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) callNext(w http.ResponseWriter, r *http.Request) {
	id, ok := boothEventID(w, r)
	if !ok {
		return
	}
	var next *workflows.ViewEntry
	if !s.boothUpdate(w, r, id, workflows.UpdateCallNext, 5*time.Second, &next) {
		return
	}
	if next == nil {
		writeJSON(w, http.StatusOK, map[string]any{"called": nil})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"called": publicEntry(*next)})
}

func (s *Server) skipTurn(w http.ResponseWriter, r *http.Request) {
	id, ok := boothEventID(w, r)
	if !ok {
		return
	}
	entry, ok := entryID(w, r)
	if !ok {
		return
	}
	var view workflows.BoothEventView
	if !s.boothUpdate(w, r, id, workflows.UpdateSkip, 5*time.Second, &view, entry) {
		return
	}
	writeJSON(w, http.StatusOK, publicView(view))
}

func (s *Server) closeBoothEvent(w http.ResponseWriter, r *http.Request) {
	if !s.requireEmployee(w, r) {
		return
	}
	id, ok := boothEventID(w, r)
	if !ok {
		return
	}
	var view workflows.BoothEventView
	if !s.boothUpdate(w, r, id, workflows.UpdateClose, 5*time.Second, &view) {
		return
	}
	writeJSON(w, http.StatusOK, publicView(view))
}

// startTurn starts the called person's flight, then answers like /api/play.
func (s *Server) startTurn(w http.ResponseWriter, r *http.Request) {
	id, ok := boothEventID(w, r)
	if !ok {
		return
	}
	entry, ok := entryID(w, r)
	if !ok {
		return
	}
	var started workflows.TurnStarted
	if !s.boothUpdate(w, r, id, workflows.UpdateStartTurn, 10*time.Second, &started, entry) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	// The flight records its score on the player when it ends, so the player
	// has to exist by then. It also gives the screen the personal best.
	profile, err := s.ensurePlayer(ctx, workflows.PlayerInput{
		PlayerID: started.Entry.PlayerID, Name: started.Entry.Name, Title: started.Entry.Title, Email: started.Entry.Email,
	})
	if err != nil {
		writeErr(w, http.StatusBadGateway, "could not open the player record")
		return
	}
	state, err := s.awaitPlaying(ctx, started.SessionID)
	if err != nil {
		writeErr(w, http.StatusGatewayTimeout, "the session worker did not start")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"sessionId":    started.SessionID,
		"playerId":     started.Entry.PlayerID,
		"state":        state,
		"personalBest": profile.BestScore,
		"gamesPlayed":  profile.GamesPlayed,
	})
}

func (s *Server) boothQR(w http.ResponseWriter, r *http.Request) {
	id, ok := boothEventID(w, r)
	if !ok {
		return
	}
	origin := strings.TrimRight(s.Config.PublicURL, "/")
	if origin == "" {
		origin = "http://" + r.Host
	}
	png, err := qrcode.EncodePNG(origin+"/join/"+url.PathEscape(id), 512)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not draw the code")
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	_, _ = w.Write(png)
}

// boothUpdate sends one Update to the booth event and writes any error. The
// cached view is dropped, so the next read shows the change.
func (s *Server) boothUpdate(w http.ResponseWriter, r *http.Request, id, name string, wait time.Duration, dest any, args ...any) bool {
	ctx, cancel := context.WithTimeout(r.Context(), wait)
	defer cancel()
	updateID, err := names.NewSessionID()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not send that")
		return false
	}
	if args == nil {
		args = []any{}
	}
	handle, err := s.Client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: names.BoothEventWorkflowID(id), UpdateName: name, UpdateID: name + "-" + updateID,
		Args: args, WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err == nil {
		err = handle.Get(ctx, dest)
	}
	s.views.drop(id)
	if err != nil {
		writeUpdateErr(w, err, "the event did not accept that")
		return false
	}
	return true
}

func writeUpdateErr(w http.ResponseWriter, err error, fallback string) {
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		writeErr(w, http.StatusNotFound, "no such event")
		return
	}
	var app *temporal.ApplicationError
	if errors.As(err, &app) && app.Message() != "" {
		writeErr(w, http.StatusConflict, app.Message())
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		writeErr(w, http.StatusGatewayTimeout, fallback)
		return
	}
	// Validator rejections arrive as a plain error carrying the reason.
	if msg := err.Error(); msg != "" && !strings.Contains(msg, "rpc error") && len(msg) < 200 {
		writeErr(w, http.StatusConflict, msg)
		return
	}
	writeErr(w, http.StatusBadGateway, fallback)
}

func boothEventID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if !names.ValidSessionID(id) {
		writeErr(w, http.StatusNotFound, "no such event")
		return "", false
	}
	return id, true
}

func entryID(w http.ResponseWriter, r *http.Request) (string, bool) {
	var req struct {
		EntryID string `json:"entryId"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<10)).Decode(&req); err != nil || req.EntryID == "" {
		writeErr(w, http.StatusBadRequest, "entry is required")
		return "", false
	}
	return req.EntryID, true
}

// publicEntry is what a screen may show about someone: first name and last
// initial, and no ticket, so nobody can read another person's place.
type publicEntryView struct {
	EntryID string `json:"entryId"`
	Name    string `json:"name"`
}

func publicEntry(e workflows.ViewEntry) publicEntryView {
	return publicEntryView{EntryID: e.EntryID, Name: shortName(e.Name)}
}

func shortName(name string) string {
	parts := strings.Fields(name)
	if len(parts) < 2 {
		return name
	}
	last := []rune(parts[len(parts)-1])
	return parts[0] + " " + string(last[0]) + "."
}

func publicView(v workflows.BoothEventView) map[string]any {
	queue := make([]publicEntryView, 0, len(v.Queue))
	for _, e := range v.Queue {
		queue = append(queue, publicEntry(e))
	}
	out := map[string]any{
		"id": v.ID, "name": v.Name, "open": v.Open, "closing": v.Closing,
		"phase": v.Phase, "queue": queue, "current": nil,
	}
	if v.Current != nil {
		out["current"] = publicEntry(*v.Current)
	}
	return out
}

// ticketView is one phone's place: 0 while called or flying, -1 when it holds
// no place (its flight is over, or its turn was skipped).
func ticketView(v workflows.BoothEventView, ticket string) map[string]any {
	position, phase := -1, ""
	if v.Current != nil && v.Current.Ticket == ticket {
		position, phase = 0, v.Phase
	}
	for i, e := range v.Queue {
		if e.Ticket == ticket {
			position = i + 1
		}
	}
	return map[string]any{
		"name": v.Name, "open": v.Open, "closing": v.Closing,
		"position": position, "phase": phase, "waiting": len(v.Queue),
	}
}

func (s *Server) requireEmployee(w http.ResponseWriter, r *http.Request) bool {
	email, err := s.employee(r)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "employee authentication is unavailable")
		return false
	}
	if email == "" {
		writeErr(w, http.StatusNotFound, "not found")
		return false
	}
	return true
}
