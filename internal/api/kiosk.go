package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/mail"
	"net/url"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"podpocalypse/internal/names"
	"podpocalypse/internal/workflows"
)

func (s *Server) kiosk(w http.ResponseWriter, r *http.Request) {
	employee, _ := s.employee(r)
	// The open booth is not an event. An event page asks for that event itself.
	writeJSON(w, http.StatusOK, map[string]any{
		"employee":  employee != "",
		"eventId":   "",
		"eventName": "",
		"formURL":   "",
		"qrURL":     "",
	})
}

func (s *Server) publicEvent(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	if id == "" || s.Config.LeadsEndpoint == "" {
		writeErr(w, http.StatusNotFound, "no such event")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	token, err := names.NewSessionID()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not open that event")
		return
	}
	var event workflows.LeadEvent
	if err := s.finishWorkflow(ctx, "lead-get-"+token, workflows.GetLeadEvent, &event, s.Config.LeadsEndpoint, id); err != nil {
		writeErr(w, http.StatusNotFound, "no such event")
		return
	}
	if event.ID == "" {
		writeErr(w, http.StatusNotFound, "no such event")
		return
	}
	form, qr := "", ""
	if s.Config.LeadsURL != "" {
		form = s.Config.LeadsURL + "/events/" + url.PathEscape(event.ID)
		qr = form + "/qr.png"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": event.ID, "name": event.Name, "status": event.Status, "formURL": form, "qrURL": qr,
	})
}

func (s *Server) kioskEvents(w http.ResponseWriter, r *http.Request) {
	if !s.requireEmployee(w, r) {
		return
	}
	if s.Config.LeadsEndpoint == "" {
		writeErr(w, http.StatusNotFound, "this booth is not connected to the event tracker")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	id, err := names.NewSessionID()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not list events")
		return
	}
	var events []workflows.LeadEvent
	if err := s.finishWorkflow(ctx, "lead-list-"+id, workflows.ListLeadEvents, &events, s.Config.LeadsEndpoint); err != nil {
		writeErr(w, http.StatusBadGateway, "could not list events")
		return
	}
	if events == nil {
		events = []workflows.LeadEvent{}
	}
	writeJSON(w, http.StatusOK, events)
}

func (s *Server) kioskBind(w http.ResponseWriter, r *http.Request) {
	if !s.requireEmployee(w, r) {
		return
	}
	var req struct {
		EventID string `json:"eventId"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<12)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "choose an event")
		return
	}
	req.EventID = strings.TrimSpace(req.EventID)
	if req.EventID == "" || s.Config.LeadsEndpoint == "" {
		writeErr(w, http.StatusBadRequest, "choose an event")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	id, err := names.NewSessionID()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not open that event")
		return
	}
	var event workflows.LeadEvent
	if err := s.finishWorkflow(ctx, "lead-get-"+id, workflows.GetLeadEvent, &event, s.Config.LeadsEndpoint, req.EventID); err != nil {
		writeErr(w, http.StatusBadGateway, "could not open that event")
		return
	}
	if event.Status != "open" || event.ID == "" {
		writeErr(w, http.StatusBadRequest, "that event is not open")
		return
	}
	form := ""
	qr := ""
	if s.Config.LeadsURL != "" {
		form = s.Config.LeadsURL + "/events/" + url.PathEscape(event.ID)
		qr = form + "/qr.png"
	}
	state := workflows.BoothState{EventID: event.ID, EventName: event.Name, FormURL: form, QRURL: qr}
	if err := s.ensureKiosk(ctx); err != nil {
		writeErr(w, http.StatusBadGateway, "could not check in the booth")
		return
	}
	updateID, err := names.NewSessionID()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not check in the booth")
		return
	}
	handle, err := s.Client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: names.KioskWorkflowID, UpdateName: workflows.UpdateBind,
		UpdateID: "bind-" + updateID, Args: []any{state}, WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		writeErr(w, http.StatusBadGateway, "could not check in the booth")
		return
	}
	if err := handle.Get(ctx, &state); err != nil {
		writeErr(w, http.StatusBadGateway, "could not check in the booth")
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Server) claimFlight(ctx context.Context, eventID, code string) (workflows.FlightProfile, error) {
	var profile workflows.FlightProfile
	eventID = strings.TrimSpace(eventID)
	if s.Config.LeadsEndpoint == "" {
		return profile, errors.New("this booth is not connected to the event tracker")
	}
	if eventID == "" {
		return profile, errors.New("this flight code belongs on an event page")
	}
	code = strings.TrimSpace(code)
	runID := "flight-claim/" + eventID + "/" + code
	err := s.finishWorkflow(ctx, runID, workflows.ClaimFlight, &profile, workflows.ClaimFlightInput{
		Endpoint: s.Config.LeadsEndpoint, EventID: eventID, Code: code,
	})
	if temporal.IsWorkflowExecutionAlreadyStartedError(err) {
		err = s.Client.GetWorkflow(ctx, runID, "").Get(ctx, &profile)
	}
	return profile, err
}

func (s *Server) boothState(ctx context.Context) (workflows.BoothState, error) {
	var state workflows.BoothState
	if s.Config.LeadsEndpoint == "" || s.Client == nil {
		return state, nil
	}
	if err := s.ensureKiosk(ctx); err != nil {
		return state, err
	}
	value, err := s.Client.QueryWorkflow(ctx, names.KioskWorkflowID, "", workflows.QueryBooth)
	if err != nil {
		return state, err
	}
	err = value.Get(&state)
	return state, err
}

func (s *Server) ensureKiosk(ctx context.Context) error {
	_, err := s.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: names.KioskWorkflowID, TaskQueue: s.Config.ControlQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
	}, workflows.KioskWorkflow)
	if temporal.IsWorkflowExecutionAlreadyStartedError(err) {
		return nil
	}
	return err
}

func (s *Server) finishWorkflow(ctx context.Context, id string, wf, output any, args ...any) error {
	run, err := s.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: id, TaskQueue: s.Config.ControlQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
	}, wf, args...)
	if err != nil {
		return err
	}
	return run.Get(ctx, output)
}

func writeFlightErr(w http.ResponseWriter, err error) {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "not connected"), strings.Contains(msg, "not checked"):
		writeErr(w, http.StatusConflict, err.Error())
	case strings.Contains(msg, "already used"):
		writeErr(w, http.StatusConflict, "that flight code was already used")
	case strings.Contains(msg, "expired"), strings.Contains(msg, "not found"):
		writeErr(w, http.StatusNotFound, "that flight code is not valid")
	default:
		writeErr(w, http.StatusBadGateway, "the event tracker did not accept that code")
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

func (s *Server) employee(r *http.Request) (string, error) {
	if s.Config.AuthVerifyURL == "" {
		return "", nil
	}
	cookie, err := r.Cookie("temporal_demo_auth")
	if err != nil || cookie.Value == "" || len(cookie.Value) > 8192 {
		return "", nil
	}
	origin, err := url.Parse(s.Config.PublicURL)
	if err != nil || origin.Host == "" || (origin.Scheme != "https" && origin.Scheme != "http") {
		return "", errors.New("public url is not configured")
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	verify, err := http.NewRequestWithContext(ctx, http.MethodGet, s.Config.AuthVerifyURL, nil)
	if err != nil {
		return "", err
	}
	verify.AddCookie(&http.Cookie{Name: cookie.Name, Value: cookie.Value})
	verify.Header.Set("X-Forwarded-Host", origin.Host)
	verify.Header.Set("X-Forwarded-Proto", origin.Scheme)
	verify.Header.Set("X-Forwarded-Method", r.Method)
	verify.Header.Set("X-Forwarded-Uri", r.URL.RequestURI())
	response, err := http.DefaultClient.Do(verify)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return "", nil
	}
	subject := strings.TrimSpace(response.Header.Get("X-Temporal-Auth-Subject"))
	email := response.Header.Get("X-Temporal-Auth-Email")
	address, err := mail.ParseAddress(email)
	if subject == "" || err != nil || address.Address != email || subject == "bootstrap-catalog-auth" {
		return "", nil
	}
	local, domain, found := strings.Cut(email, "@")
	if !found || local == "" || !strings.EqualFold(domain, "temporal.io") {
		return "", nil
	}
	return strings.ToLower(email), nil
}
