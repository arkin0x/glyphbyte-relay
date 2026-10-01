package harvest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	nostr "github.com/0ceanslim/grain/server/types"
	"github.com/0ceanslim/grain/server/utils/log"
)

// job is one received event waiting to be verified and stored.
type job struct {
	evt   nostr.Event
	size  int
	stats *relayStats
	wg    *sync.WaitGroup // backfill page waiting on this event, or nil
}

// Harvester runs one worker per upstream relay and a shared pool that
// verifies and stores what they receive.
type Harvester struct {
	cfg      *Config
	state    *State
	ingester *Ingester
	queue    chan job

	mu    sync.Mutex
	stats map[string]*relayStats
}

// New builds a harvester. statePath is where cursors are saved.
func New(cfg *Config, ingester *Ingester, statePath string) (*Harvester, error) {
	st, err := LoadState(statePath)
	if err != nil {
		return nil, err
	}
	return &Harvester{
		cfg:      cfg,
		state:    st,
		ingester: ingester,
		queue:    make(chan job, 1024),
		stats:    map[string]*relayStats{},
	}, nil
}

// Run harvests until ctx is cancelled, then saves the cursors.
func (h *Harvester) Run(ctx context.Context) {
	log.Harvest().Info("Harvester starting",
		"relays", len(h.cfg.Relays),
		"live", *h.cfg.Live.Enabled,
		"backfill", *h.cfg.Backfill.Enabled,
		"max_age_days", h.cfg.Backfill.MaxAgeDays)

	var pool sync.WaitGroup
	for i := 0; i < h.cfg.Workers; i++ {
		pool.Add(1)
		go func() {
			defer pool.Done()
			h.process(ctx)
		}()
	}

	var relays sync.WaitGroup
	for _, r := range h.cfg.Relays {
		w := &worker{url: r.URL, filters: r.Filters, h: h, stats: h.relayStats(r.URL)}
		relays.Add(1)
		go func() {
			defer relays.Done()
			w.run(ctx)
		}()
	}

	saveTick := time.NewTicker(10 * time.Second)
	defer saveTick.Stop()
	var statsTick <-chan time.Time
	if h.cfg.StatsIntervalSeconds > 0 {
		t := time.NewTicker(time.Duration(h.cfg.StatsIntervalSeconds) * time.Second)
		defer t.Stop()
		statsTick = t.C
	}

	for {
		select {
		case <-ctx.Done():
			relays.Wait()
			pool.Wait()
			if err := h.state.Save(); err != nil {
				log.Harvest().Error("Failed to save harvest state", "error", err)
			}
			log.Harvest().Info("Harvester stopped")
			return
		case <-saveTick.C:
			if err := h.state.Save(); err != nil {
				log.Harvest().Error("Failed to save harvest state", "error", err)
			}
		case <-statsTick:
			h.logStats()
		}
	}
}

func (h *Harvester) relayStats(url string) *relayStats {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.stats[url]
	if s == nil {
		s = &relayStats{}
		h.stats[url] = s
	}
	return s
}

// enqueue hands an event to the pool, blocking when the pool is busy so a
// fast relay slows down instead of growing memory.
func (h *Harvester) enqueue(ctx context.Context, evt nostr.Event, size int, stats *relayStats, wg *sync.WaitGroup) {
	select {
	case h.queue <- job{evt: evt, size: size, stats: stats, wg: wg}:
	case <-ctx.Done():
		if wg != nil {
			wg.Done()
		}
	}
}

func (h *Harvester) process(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// Release any page still waiting on queued events.
			for {
				select {
				case j := <-h.queue:
					if j.wg != nil {
						j.wg.Done()
					}
				default:
					return
				}
			}
		case j := <-h.queue:
			outcome, err := h.ingester.Ingest(ctx, j.evt, j.size)
			j.stats.count(outcome)
			if err != nil {
				log.Harvest().Error("Failed to store harvested event", "event_id", j.evt.ID, "error", err)
			}
			if j.wg != nil {
				j.wg.Done()
			}
			if errors.Is(err, errStoreFull) {
				// Writes are refused until an operator frees space;
				// stop hammering the store.
				_ = sleep(ctx, 30*time.Second)
			}
		}
	}
}

// Status is a point-in-time view of one upstream relay.
type Status struct {
	Relay      string           `json:"relay"`
	Connected  bool             `json:"connected"`
	Received   int64            `json:"received"`
	Stored     int64            `json:"stored"`
	Duplicate  int64            `json:"duplicate"`
	Invalid    int64            `json:"invalid"`
	Failed     int64            `json:"failed"`
	Skipped    map[string]int64 `json:"skipped"` // by reason; zero counts left out
	Pages      int64            `json:"pages"`
	Reconnects int64            `json:"reconnects"`
	Spans      int              `json:"backfill_spans_left"`
}

// Statuses reports every relay's counters since startup.
func (h *Harvester) Statuses() []Status {
	out := make([]Status, 0, len(h.cfg.Relays))
	for _, r := range h.cfg.Relays {
		url := r.URL
		s := h.relayStats(url)
		spans := 0
		for _, f := range r.Filters {
			spans += len(h.state.Get(url, filterKey(f)).Spans)
		}
		skipped := map[string]int64{}
		for o := SkipKind; o <= SkipStoreReject; o++ {
			if n := s.outcomes[o].Load(); n > 0 {
				skipped[o.String()] = n
			}
		}
		out = append(out, Status{
			Relay:      url,
			Connected:  s.connected.Load(),
			Received:   s.received.Load(),
			Stored:     s.outcomes[Stored].Load(),
			Duplicate:  s.outcomes[Duplicate].Load(),
			Invalid:    s.outcomes[Invalid].Load(),
			Failed:     s.outcomes[Failed].Load(),
			Skipped:    skipped,
			Pages:      s.pages.Load(),
			Reconnects: s.reconnects.Load(),
			Spans:      spans,
		})
	}
	return out
}

func (h *Harvester) logStats() {
	for _, s := range h.Statuses() {
		log.Harvest().Info("Harvest stats",
			"relay", s.Relay, "connected", s.Connected,
			"received", s.Received, "stored", s.Stored, "duplicate", s.Duplicate,
			"invalid", s.Invalid, "failed", s.Failed, "skipped", fmt.Sprint(s.Skipped),
			"pages", s.Pages, "reconnects", s.Reconnects, "backfill_spans_left", s.Spans)
	}
}
