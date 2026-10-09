package workflows

import (
	"sort"
	"time"

	"go.temporal.io/sdk/workflow"
)

const (
	QueryBoard         = "board"
	QueryProfile       = "profile"
	UpdateProfile      = "sync_profile"
	SignalSubmitScore  = "submit_score"
	SignalGameFinished = "game_finished"
	boardContinueAfter = 200
)

type BoardEntry struct {
	SessionID  string    `json:"sessionId"`
	PlayerID   string    `json:"playerId"`
	Name       string    `json:"name"`
	Title      string    `json:"title"`
	Score      int       `json:"score"`
	Crashes    int       `json:"crashes"`
	TimedOut   bool      `json:"timedOut"`
	FinishedAt time.Time `json:"finishedAt"`
}

type Board struct {
	Entries []BoardEntry
	Events  int
}

type PlayerInput struct {
	PlayerID string
	Name     string
	Title    string
	Email    string
	Consent  bool
	State    *PlayerState
}

type PlayerGame struct {
	SessionID  string
	Score      int
	Crashes    int
	TimedOut   bool
	FinishedAt time.Time
}

type PlayerState struct {
	PlayerID      string       `json:"playerId"`
	Name          string       `json:"name"`
	Title         string       `json:"title"`
	Email         string       `json:"email"`
	Consent       bool         `json:"consent"`
	CreatedAt     time.Time    `json:"createdAt"`
	BestScore     int          `json:"bestScore"`
	BestSessionID string       `json:"bestSessionId,omitempty"`
	GamesPlayed   int          `json:"gamesPlayed"`
	Games         []PlayerGame `json:"games,omitempty"` // Migrated from the original player history.
}

func LeaderboardWorkflow(ctx workflow.Context, state Board) error {
	if state.Entries == nil {
		state.Entries = []BoardEntry{}
	}
	if err := workflow.SetQueryHandler(ctx, QueryBoard, func() ([]BoardEntry, error) {
		return top(state.Entries, 10), nil
	}); err != nil {
		return err
	}
	ch := workflow.GetSignalChannel(ctx, SignalSubmitScore)
	for {
		var entry BoardEntry
		cancelled := false
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(ch, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &entry)
		})
		sel.AddReceive(ctx.Done(), func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
			cancelled = true
		})
		sel.Select(ctx)
		if cancelled || ctx.Err() != nil {
			return ctx.Err()
		}
		if entry.SessionID == "" {
			continue
		}
		state.Entries = upsert(state.Entries, entry)
		state.Events++
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() || state.Events >= boardContinueAfter {
			return workflow.NewContinueAsNewError(ctx, LeaderboardWorkflow, state)
		}
	}
}

func upsert(entries []BoardEntry, entry BoardEntry) []BoardEntry {
	replaced := false
	for i := range entries {
		samePlayer := entry.PlayerID != "" && entries[i].PlayerID == entry.PlayerID
		if samePlayer || entries[i].SessionID == entry.SessionID {
			if betterScore(entry, entries[i]) {
				entries[i] = entry
			}
			replaced = true
			break
		}
	}
	if !replaced {
		entries = append(entries, entry)
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Score != entries[j].Score {
			return entries[i].Score > entries[j].Score
		}
		if entries[i].Crashes != entries[j].Crashes {
			return entries[i].Crashes < entries[j].Crashes
		}
		return entries[i].FinishedAt.Before(entries[j].FinishedAt)
	})
	if len(entries) > 50 {
		entries = entries[:50]
	}
	return entries
}

func betterScore(next, current BoardEntry) bool {
	if next.Score != current.Score {
		return next.Score > current.Score
	}
	if next.Crashes != current.Crashes {
		return next.Crashes < current.Crashes
	}
	return next.FinishedAt.Before(current.FinishedAt)
}

func top(entries []BoardEntry, n int) []BoardEntry {
	if len(entries) < n {
		n = len(entries)
	}
	out := make([]BoardEntry, n)
	copy(out, entries[:n])
	return out
}

func PlayerWorkflow(ctx workflow.Context, in PlayerInput) error {
	var state PlayerState
	if in.State != nil {
		state = *in.State
	} else {
		state = PlayerState{
			PlayerID:  in.PlayerID,
			Name:      in.Name,
			Title:     in.Title,
			Email:     in.Email,
			Consent:   in.Consent,
			CreatedAt: workflow.Now(ctx),
		}
	}
	// Older runs stored the last 20 games. Fold those into one durable best
	// and discard the list so a player entity stays small for repeat visits.
	for _, game := range state.Games {
		state.GamesPlayed++
		if state.BestSessionID == "" || game.Score > state.BestScore {
			state.BestScore = game.Score
			state.BestSessionID = game.SessionID
		}
	}
	state.Games = nil
	if err := workflow.SetQueryHandler(ctx, QueryProfile, func() (PlayerState, error) {
		return state, nil
	}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandler(ctx, UpdateProfile, func(_ workflow.Context, next PlayerInput) (PlayerState, error) {
		if next.Name != "" {
			state.Name = next.Name
		}
		if next.Title != "" {
			state.Title = next.Title
		}
		state.Consent = next.Consent
		return state, nil
	}); err != nil {
		return err
	}
	ch := workflow.GetSignalChannel(ctx, SignalGameFinished)
	for {
		var game PlayerGame
		cancelled := false
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(ch, func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, &game)
		})
		sel.AddReceive(ctx.Done(), func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
			cancelled = true
		})
		sel.Select(ctx)
		if cancelled || ctx.Err() != nil {
			return ctx.Err()
		}
		if game.SessionID == "" {
			continue
		}
		state.GamesPlayed++
		if state.BestSessionID == "" || game.Score > state.BestScore {
			state.BestScore = game.Score
			state.BestSessionID = game.SessionID
		}
		if workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			in.State = &state
			return workflow.NewContinueAsNewError(ctx, PlayerWorkflow, in)
		}
	}
}
