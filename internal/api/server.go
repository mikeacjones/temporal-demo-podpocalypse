package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"podpocalypse/internal/config"
	"podpocalypse/internal/game"
	"podpocalypse/internal/names"
	"podpocalypse/internal/workflows"
)

type Server struct {
	Client client.Client
	Config config.Config
}

type PlayRequest struct {
	Name            string `json:"name"`
	Title           string `json:"title"`
	Email           string `json:"email"`
	Consent         bool   `json:"consent"`
	DurationSeconds int    `json:"durationSeconds"`
	FlightCode      string `json:"flightCode"`
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /api/meta", s.meta)
	mux.HandleFunc("GET /api/kiosk", s.kiosk)
	mux.HandleFunc("GET /api/kiosk/events", s.kioskEvents)
	mux.HandleFunc("POST /api/kiosk/bind", s.kioskBind)
	mux.HandleFunc("POST /api/play", s.play)
	mux.HandleFunc("POST /api/players/lookup", s.lookupPlayer)
	mux.HandleFunc("GET /api/sessions/{id}", s.session)
	mux.HandleFunc("POST /api/sessions/{id}/gates", s.gate)
	mux.HandleFunc("POST /api/sessions/{id}/treats", s.treat)
	mux.HandleFunc("POST /api/sessions/{id}/crash", s.crash)
	mux.HandleFunc("GET /api/leaderboard", s.leaderboard)
	mux.HandleFunc("GET /api/prize", s.prize)
	mux.HandleFunc("GET /assets/{name}", s.asset)
	mux.HandleFunc("GET /", s.index)
	return mux
}

func validEmail(email string) bool {
	at := strings.LastIndex(email, "@")
	return at >= 1 && at <= len(email)-4 && strings.Contains(email[at+1:], ".")
}

func applyReturning(in PlayRequest, profile workflows.PlayerState, found bool) (PlayRequest, error) {
	in.Name = strings.TrimSpace(in.Name)
	in.Title = strings.TrimSpace(in.Title)
	in.Email = strings.TrimSpace(in.Email)
	if in.Name != "" && in.Title != "" {
		return in, nil
	}
	if !found || profile.Name == "" || profile.Title == "" {
		return in, errors.New("name and title are required the first time you fly")
	}
	if in.Name == "" {
		in.Name = profile.Name
	}
	if in.Title == "" {
		in.Title = profile.Title
	}
	if !in.Consent {
		in.Consent = profile.Consent
	}
	return in, nil
}

func ValidatePlay(in PlayRequest) error {
	in.Name = strings.TrimSpace(in.Name)
	in.Title = strings.TrimSpace(in.Title)
	in.Email = strings.TrimSpace(in.Email)
	if len(in.Name) < 1 || len(in.Name) > 80 {
		return errors.New("name is required")
	}
	if len(in.Title) < 1 || len(in.Title) > 80 {
		return errors.New("title is required")
	}
	if !validEmail(in.Email) {
		return errors.New("email is required")
	}
	if in.DurationSeconds != 0 {
		d := time.Duration(in.DurationSeconds) * time.Second
		if d < game.MinDuration || d > game.MaxDuration {
			return fmt.Errorf("duration must be between %s and %s", game.MinDuration, game.MaxDuration)
		}
	}
	return nil
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if _, err := s.Client.CheckHealth(ctx, &client.CheckHealthRequest{}); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "temporal is not reachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) meta(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"temporalUI":      strings.TrimRight(s.Config.TemporalUI, "/"),
		"namespace":       s.Config.TemporalNamespace,
		"durationSeconds": int(s.Config.GameDuration / time.Second),
		"minGateMs":       int(game.MinGateInterval / time.Millisecond),
		"treatSeconds":    int(game.TreatBonus / time.Second),
		"maxTreats":       game.MaxTreats,
		"runtime":         s.Config.Runtime,
		"leads":           s.Config.LeadsEndpoint != "",
	})
}

func (s *Server) play(w http.ResponseWriter, r *http.Request) {
	var req PlayRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "the form could not be read")
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	req.Title = strings.TrimSpace(req.Title)
	req.Email = strings.TrimSpace(req.Email)
	req.FlightCode = strings.TrimSpace(req.FlightCode)
	if req.FlightCode != "" {
		claimCtx, claimCancel := context.WithTimeout(r.Context(), 20*time.Second)
		profile, err := s.claimFlight(claimCtx, req.FlightCode)
		claimCancel()
		if err != nil {
			writeFlightErr(w, err)
			return
		}
		req.Name = profile.Name
		req.Title = profile.Role
		req.Email = profile.Email
		req.Consent = true
	}
	if !validEmail(req.Email) {
		writeErr(w, http.StatusBadRequest, "email is required")
		return
	}
	if req.Name == "" || req.Title == "" {
		lookCtx, lookCancel := context.WithTimeout(r.Context(), 3*time.Second)
		profile, found, err := s.readPlayer(lookCtx, names.PlayerWorkflowID(req.Email))
		lookCancel()
		if err != nil {
			writeErr(w, http.StatusBadGateway, "could not look up that email")
			return
		}
		req, err = applyReturning(req, profile, found)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if err := ValidatePlay(req); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	duration := s.Config.GameDuration
	if req.DurationSeconds > 0 {
		duration = time.Duration(req.DurationSeconds) * time.Second
	}
	sessionID, err := names.NewSessionID()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "could not start a session")
		return
	}
	playerID := names.PlayerWorkflowID(req.Email)
	ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
	defer cancel()
	profile, err := s.ensurePlayer(ctx, workflows.PlayerInput{
		PlayerID: playerID,
		Name:     req.Name,
		Title:    req.Title,
		Email:    req.Email,
		Consent:  req.Consent,
	})
	if err != nil {
		writeErr(w, http.StatusBadGateway, "could not open your player record")
		return
	}
	run, err := s.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        names.RunWorkflowID(sessionID),
		TaskQueue: s.Config.ControlQueue,
	}, workflows.GameRunWorkflow, workflows.RunInput{
		SessionID:        sessionID,
		PlayerID:         playerID,
		Name:             req.Name,
		Title:            req.Title,
		Email:            req.Email,
		Consent:          req.Consent,
		Duration:         duration,
		ControlTaskQueue: s.Config.ControlQueue,
	})
	if err != nil {
		writeErr(w, http.StatusBadGateway, "could not start the game")
		return
	}
	state, err := s.awaitPlaying(ctx, sessionID)
	if err != nil {
		writeErr(w, http.StatusGatewayTimeout, "the session worker did not start")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"sessionId":    sessionID,
		"playerId":     playerID,
		"runId":        run.GetRunID(),
		"state":        state,
		"personalBest": profile.BestScore,
		"gamesPlayed":  profile.GamesPlayed,
	})
}

func (s *Server) session(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := names.RequireSessionID(id); err != nil {
		writeErr(w, http.StatusNotFound, "no such session")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 800*time.Millisecond)
	defer cancel()
	desc, err := s.Client.DescribeWorkflowExecution(ctx, names.SessionWorkflowID(id), "")
	if err != nil {
		var missing *serviceerror.NotFound
		if errors.As(err, &missing) {
			writeErr(w, http.StatusNotFound, "no such session")
			return
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"phase":     "recovering",
			"sessionId": id,
		})
		return
	}
	// The session worker is gone once the flight is over, so a query cannot
	// be answered. The closed result is enough for the score screen.
	if desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		var result workflows.SessionResult
		if err := s.Client.GetWorkflow(ctx, names.SessionWorkflowID(id), "").Get(ctx, &result); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"phase":     "recovering",
				"sessionId": id,
			})
			return
		}
		writeJSON(w, http.StatusOK, workflows.PublicState{
			SessionID:  result.SessionID,
			WorkflowID: names.SessionWorkflowID(id),
			PlayerName: result.Name,
			Phase:      "finished",
			Score:      result.Score,
			Crashes:    result.Crashes,
			WorkerPod:  result.WorkerPod,
			TimedOut:   result.TimedOut,
			Finished:   true,
			Seed:       result.SessionID,
		})
		return
	}
	state, err := s.querySession(ctx, id)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"phase":     "recovering",
			"sessionId": id,
		})
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (s *Server) gate(w http.ResponseWriter, r *http.Request) {
	id, ok := sessionID(w, r)
	if !ok {
		return
	}
	var req struct {
		Gate int `json:"gate"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<12)).Decode(&req); err != nil || req.Gate < 1 {
		writeErr(w, http.StatusBadRequest, "gate is required")
		return
	}
	s.completeUpdate(w, r, id, workflows.UpdateGate, 2*time.Second, []any{req.Gate}, &workflows.GateResult{})
}

func (s *Server) treat(w http.ResponseWriter, r *http.Request) {
	id, ok := sessionID(w, r)
	if !ok {
		return
	}
	var req struct {
		Treat int `json:"treat"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<12)).Decode(&req); err != nil || req.Treat < 1 {
		writeErr(w, http.StatusBadRequest, "treat is required")
		return
	}
	s.completeUpdate(w, r, id, workflows.UpdateTreat, 2*time.Second, []any{req.Treat}, &workflows.TreatResult{})
}

func (s *Server) crash(w http.ResponseWriter, r *http.Request) {
	id, ok := sessionID(w, r)
	if !ok {
		return
	}
	// The crash update deletes the session worker and does not finish until
	// the replacement is polling. Stop waiting here so the page can freeze
	// while the clock keeps moving.
	s.completeUpdate(w, r, id, workflows.UpdateCrash, 1200*time.Millisecond, nil, &workflows.CrashResult{})
}

func sessionID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if err := names.RequireSessionID(id); err != nil {
		writeErr(w, http.StatusNotFound, "no such session")
		return "", false
	}
	return id, true
}

func (s *Server) completeUpdate(w http.ResponseWriter, r *http.Request, id, name string, wait time.Duration, args []any, dest any) {
	ctx, cancel := context.WithTimeout(r.Context(), wait)
	defer cancel()
	if args == nil {
		args = []any{}
	}
	handle, err := s.Client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   names.SessionWorkflowID(id),
		UpdateName:   name,
		Args:         args,
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		if updateWaitEnded(ctx, err) {
			writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true, "phase": "recovering"})
			return
		}
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	if err := handle.Get(ctx, dest); err != nil {
		if updateWaitEnded(ctx, err) {
			writeJSON(w, http.StatusAccepted, map[string]any{"accepted": true, "phase": "recovering"})
			return
		}
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, dest)
}

// updateWaitEnded is the kill still running. The Go SDK reports that as a
// context timeout, or as the update stream being cancelled while it waits.
func updateWaitEnded(ctx context.Context, err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.Canceled) {
		return true
	}
	return strings.Contains(err.Error(), "Timeout or cancellation")
}

func (s *Server) leaderboard(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	val, err := s.Client.QueryWorkflow(ctx, names.LeaderboardWorkflowID, "", workflows.QueryBoard)
	if err != nil {
		writeJSON(w, http.StatusOK, []workflows.BoardEntry{})
		return
	}
	var board []workflows.BoardEntry
	if err := val.Get(&board); err != nil {
		writeErr(w, http.StatusBadGateway, "leaderboard could not be read")
		return
	}
	if board == nil {
		board = []workflows.BoardEntry{}
	}
	writeJSON(w, http.StatusOK, board)
}

func (s *Server) prize(w http.ResponseWriter, r *http.Request) {
	if s.Config.AdminToken == "" || !bearerOK(r, s.Config.AdminToken) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	val, err := s.Client.QueryWorkflow(ctx, names.LeaderboardWorkflowID, "", workflows.QueryBoard)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "leaderboard could not be read")
		return
	}
	var board []workflows.BoardEntry
	if err := val.Get(&board); err != nil || len(board) == 0 {
		writeErr(w, http.StatusNotFound, "no finisher yet")
		return
	}
	winner := board[0]
	profileVal, err := s.Client.QueryWorkflow(ctx, winner.PlayerID, "", workflows.QueryProfile)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "player record could not be read")
		return
	}
	var profile workflows.PlayerState
	if err := profileVal.Get(&profile); err != nil {
		writeErr(w, http.StatusBadGateway, "player record could not be read")
		return
	}
	if !profile.Consent {
		writeJSON(w, http.StatusOK, map[string]any{
			"entry":   winner,
			"consent": false,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"entry":   winner,
		"consent": true,
		"email":   profile.Email,
		"name":    profile.Name,
		"title":   profile.Title,
	})
}

func (s *Server) lookupPlayer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email string `json:"email"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "the form could not be read")
		return
	}
	req.Email = strings.TrimSpace(req.Email)
	if !validEmail(req.Email) {
		writeErr(w, http.StatusBadRequest, "email is required")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	profile, found, err := s.readPlayer(ctx, names.PlayerWorkflowID(req.Email))
	if err != nil || !found {
		writeJSON(w, http.StatusOK, map[string]any{"known": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"known": true,
		"name":  profile.Name,
		"title": profile.Title,
	})
}

func (s *Server) readPlayer(ctx context.Context, id string) (workflows.PlayerState, bool, error) {
	val, err := s.Client.QueryWorkflow(ctx, id, "", workflows.QueryProfile)
	if err != nil {
		var missing *serviceerror.NotFound
		if errors.As(err, &missing) {
			return workflows.PlayerState{}, false, nil
		}
		return workflows.PlayerState{}, false, err
	}
	var profile workflows.PlayerState
	if err := val.Get(&profile); err != nil {
		return workflows.PlayerState{}, false, err
	}
	if profile.Name == "" || profile.Title == "" {
		return profile, false, nil
	}
	return profile, true, nil
}

func (s *Server) ensurePlayer(ctx context.Context, in workflows.PlayerInput) (workflows.PlayerState, error) {
	_, err := s.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    in.PlayerID,
		TaskQueue:             s.Config.ControlQueue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
	}, workflows.PlayerWorkflow, in)
	if err != nil && !temporal.IsWorkflowExecutionAlreadyStartedError(err) {
		return workflows.PlayerState{}, err
	}
	handle, err := s.Client.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   in.PlayerID,
		UpdateName:   workflows.UpdateProfile,
		Args:         []any{in},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return workflows.PlayerState{}, err
	}
	var profile workflows.PlayerState
	if err := handle.Get(ctx, &profile); err != nil {
		return workflows.PlayerState{}, err
	}
	return profile, nil
}

func (s *Server) awaitPlaying(ctx context.Context, sessionID string) (workflows.PublicState, error) {
	var last error
	for {
		state, err := s.querySession(ctx, sessionID)
		if err == nil && (state.Phase == "playing" || state.Phase == "finished") {
			return state, nil
		}
		last = err
		select {
		case <-ctx.Done():
			if last == nil {
				last = ctx.Err()
			}
			return workflows.PublicState{}, last
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (s *Server) querySession(ctx context.Context, sessionID string) (workflows.PublicState, error) {
	val, err := s.Client.QueryWorkflow(ctx, names.SessionWorkflowID(sessionID), "", workflows.QueryState)
	if err != nil {
		return workflows.PublicState{}, err
	}
	var state workflows.PublicState
	if err := val.Get(&state); err != nil {
		return workflows.PublicState{}, err
	}
	return state, nil
}

func bearerOK(r *http.Request, token string) bool {
	got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if len(got) != len(token) {
		return false
	}
	diff := 0
	for i := range token {
		diff |= int(got[i] ^ token[i])
	}
	return diff == 0 && got != ""
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
