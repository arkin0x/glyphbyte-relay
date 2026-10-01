package harvest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Span is a stretch of history still to be paged through on one relay, newest
// first: the next page asks for events at or before Until, and the span is
// finished once a page comes back empty or Until drops below Floor. Floor 0
// means the beginning of time.
type Span struct {
	Until int64 `json:"until"`
	Floor int64 `json:"floor"`
}

// Cursor is the progress for one (relay, filter) pair.
type Cursor struct {
	// LiveSince is the newest created_at seen on the live tail. On
	// reconnect the gap from here to now is queued as a span.
	LiveSince int64 `json:"live_since"`
	// Spans are the stretches of history still to fetch, worked newest
	// first. The initial backfill is one span; each reconnect after a gap
	// adds one.
	Spans []Span `json:"spans"`
	// BackfillStarted records that the initial backfill span was queued, so
	// a finished backfill is not queued again on restart.
	BackfillStarted bool `json:"backfill_started"`
	// Stored and Pages are lifetime counters, for operators.
	Stored int64 `json:"stored"`
	Pages  int64 `json:"pages"`
}

// State is every cursor, keyed by relay URL then filter key. It is saved to
// harvest_state.json in the data directory.
type State struct {
	mu      sync.Mutex
	path    string
	Cursors map[string]map[string]*Cursor `json:"cursors"`
	dirty   bool
}

// LoadState reads the state file, or starts empty when it does not exist.
func LoadState(path string) (*State, error) {
	s := &State{path: path, Cursors: map[string]map[string]*Cursor{}}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(data, s); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if s.Cursors == nil {
		s.Cursors = map[string]map[string]*Cursor{}
	}
	return s, nil
}

// Update runs fn on the cursor for (relay, key), creating it if needed, and
// marks the state dirty. fn runs under the state lock.
func (s *State) Update(relay, key string, fn func(c *Cursor)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	byKey := s.Cursors[relay]
	if byKey == nil {
		byKey = map[string]*Cursor{}
		s.Cursors[relay] = byKey
	}
	c := byKey[key]
	if c == nil {
		c = &Cursor{}
		byKey[key] = c
	}
	fn(c)
	s.dirty = true
}

// Get returns a copy of the cursor for (relay, key).
func (s *State) Get(relay, key string) Cursor {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c := s.Cursors[relay][key]; c != nil {
		cp := *c
		cp.Spans = append([]Span(nil), c.Spans...)
		return cp
	}
	return Cursor{}
}

// Save writes the state atomically (temp file + rename) if it changed.
func (s *State) Save() error {
	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return nil
	}
	data, err := json.MarshalIndent(s, "", "  ")
	s.dirty = false
	s.mu.Unlock()
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(s.path), 0755); err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
