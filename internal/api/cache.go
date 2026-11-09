package api

import (
	"context"
	"sync"
	"time"

	"podpocalypse/internal/workflows"
)

// viewCache answers every screen and phone poll for an event from one Query
// per second. A Query is a billable call, and a room of phones polling one
// each would be a Query per phone per poll.
type viewCache struct {
	ttl     time.Duration
	mu      sync.Mutex
	entries map[string]*cachedView
}

type cachedView struct {
	mu   sync.Mutex
	at   time.Time
	view workflows.BoothEventView
}

func newViewCache(ttl time.Duration) *viewCache {
	return &viewCache{ttl: ttl, entries: map[string]*cachedView{}}
}

// get returns the cached view, or fetches it. Callers for the same event wait
// for one fetch instead of each sending their own.
func (c *viewCache) get(ctx context.Context, id string, fetch func(context.Context, string) (workflows.BoothEventView, error)) (workflows.BoothEventView, error) {
	c.mu.Lock()
	entry := c.entries[id]
	if entry == nil {
		// The booth runs a handful of events, so entries are never evicted.
		entry = &cachedView{}
		c.entries[id] = entry
	}
	c.mu.Unlock()

	entry.mu.Lock()
	defer entry.mu.Unlock()
	if !entry.at.IsZero() && time.Since(entry.at) < c.ttl {
		return entry.view, nil
	}
	view, err := fetch(ctx, id)
	if err != nil {
		return view, err
	}
	entry.view, entry.at = view, time.Now()
	return view, nil
}

// drop makes the next read fetch, after a change the reader should see.
func (c *viewCache) drop(id string) {
	c.mu.Lock()
	entry := c.entries[id]
	c.mu.Unlock()
	if entry == nil {
		return
	}
	entry.mu.Lock()
	entry.at = time.Time{}
	entry.mu.Unlock()
}
