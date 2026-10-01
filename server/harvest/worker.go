package harvest

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	nostr "github.com/0ceanslim/grain/server/types"
	"github.com/0ceanslim/grain/server/utils/log"

	"golang.org/x/net/websocket"
)

// relayStats are lifetime counters for one upstream relay.
type relayStats struct {
	connected  atomic.Bool
	received   atomic.Int64
	pages      atomic.Int64
	reconnects atomic.Int64
	outcomes   [SkipStoreReject + 1]atomic.Int64 // indexed by Outcome
}

func (s *relayStats) count(o Outcome) { s.outcomes[o].Add(1) }

// worker harvests one upstream relay, reconnecting with backoff for as long
// as the context lives.
type worker struct {
	url     string
	filters []map[string]interface{}
	h       *Harvester
	stats   *relayStats
}

func (w *worker) run(ctx context.Context) {
	backoff := w.h.cfg.reconnectMin()
	for ctx.Err() == nil {
		started := time.Now()
		err := w.session(ctx)
		w.stats.connected.Store(false)
		if ctx.Err() != nil {
			return
		}
		// A session that stayed up a while resets the backoff.
		if time.Since(started) > 2*time.Minute {
			backoff = w.h.cfg.reconnectMin()
		}
		log.Harvest().Warn("Upstream relay session ended", "relay", w.url, "error", err, "retry_in", backoff.String())
		w.stats.reconnects.Add(1)
		jitter := time.Duration(rand.Int63n(int64(backoff)/4 + 1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff + jitter):
		}
		if backoff *= 2; backoff > w.h.cfg.reconnectMax() {
			backoff = w.h.cfg.reconnectMax()
		}
	}
}

// conn is one live websocket to an upstream relay plus its subscription
// routing.
type conn struct {
	ws      *websocket.Conn
	writeMu sync.Mutex

	mu    sync.Mutex
	pages map[string]*page // backfill subscriptions by id
}

// page collects one backfill request's events.
type page struct {
	until  int64
	count  int
	oldest int64
	newer  int // events after `until`: the relay ignored the bound
	wg     sync.WaitGroup
	done   chan string // "" on EOSE, the reason on CLOSED
	once   sync.Once
}

func (p *page) finish(reason string) {
	p.once.Do(func() { p.done <- reason; close(p.done) })
}

func (c *conn) send(msg []interface{}) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	// The deadline must be cleared again: the websocket library also
	// writes on its own (pong replies to the relay's pings), and a stale
	// deadline kills the socket the next time the relay pings.
	_ = c.ws.SetWriteDeadline(time.Now().Add(15 * time.Second))
	defer c.ws.SetWriteDeadline(time.Time{})
	return websocket.Message.Send(c.ws, string(b))
}

func dial(ctx context.Context, url string) (*websocket.Conn, error) {
	cfg, err := websocket.NewConfig(url, "http://localhost/")
	if err != nil {
		return nil, err
	}
	cfg.Dialer = &net.Dialer{Timeout: 15 * time.Second}
	type result struct {
		ws  *websocket.Conn
		err error
	}
	ch := make(chan result, 1)
	go func() {
		ws, err := websocket.DialConfig(cfg)
		ch <- result{ws, err}
	}()
	select {
	case <-ctx.Done():
		go func() {
			if r := <-ch; r.ws != nil {
				r.ws.Close()
			}
		}()
		return nil, ctx.Err()
	case r := <-ch:
		return r.ws, r.err
	}
}

// session runs one connection until it fails or ctx ends.
func (w *worker) session(ctx context.Context) error {
	ws, err := dial(ctx, w.url)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := &conn{ws: ws, pages: map[string]*page{}}
	defer ws.Close()
	go func() { <-sctx.Done(); ws.Close() }()

	w.stats.connected.Store(true)
	log.Harvest().Info("Connected to upstream relay", "relay", w.url)

	cfg := w.h.cfg
	now := time.Now().Unix()
	overlap := int64(cfg.Live.OverlapSeconds)
	liveKeys := map[string]string{} // live sub id -> filter key

	for _, f := range w.filters {
		key := filterKey(f)
		w.h.state.Update(w.url, key, func(cur *Cursor) {
			// The gap since the last live event goes to the backfill
			// queue, so a long outage is paged in properly instead of
			// trusting one live REQ to return all of it.
			if *cfg.Live.Enabled && cur.LiveSince > 0 && now-cur.LiveSince > overlap {
				cur.Spans = append(cur.Spans, Span{Until: now, Floor: cur.LiveSince - overlap})
			}
			if *cfg.Backfill.Enabled && !cur.BackfillStarted {
				floor := int64(0)
				if cfg.Backfill.MaxAgeDays > 0 {
					floor = now - int64(cfg.Backfill.MaxAgeDays)*86400
				}
				cur.Spans = append(cur.Spans, Span{Until: now, Floor: floor})
				cur.BackfillStarted = true
			}
		})
		if *cfg.Live.Enabled {
			id := "live:" + key
			liveKeys[id] = key
			if err := c.send([]interface{}{"REQ", id, withPaging(f, now-overlap, 0, 0)}); err != nil {
				return fmt.Errorf("live REQ: %w", err)
			}
		}
	}

	readErr := make(chan error, 1)
	go func() { readErr <- w.read(sctx, c, liveKeys) }()

	bfErr := make(chan error, 1)
	go func() { bfErr <- w.backfill(sctx, c) }()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-readErr:
		return err
	case err := <-bfErr:
		if err != nil {
			return err
		}
		// Backfill finished; keep the live tail until the socket dies.
		return <-readErr
	}
}

// read dispatches every message from the relay until the socket fails.
func (w *worker) read(ctx context.Context, c *conn, liveKeys map[string]string) error {
	idle := time.Duration(w.h.cfg.IdleTimeoutSeconds) * time.Second
	for {
		_ = c.ws.SetReadDeadline(time.Now().Add(idle))
		var msg string
		if err := websocket.Message.Receive(c.ws, &msg); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read: %w", err)
		}
		var arr []json.RawMessage
		if err := json.Unmarshal([]byte(msg), &arr); err != nil || len(arr) < 2 {
			continue
		}
		var typ, sub string
		if json.Unmarshal(arr[0], &typ) != nil {
			continue
		}
		_ = json.Unmarshal(arr[1], &sub)

		switch typ {
		case "EVENT":
			if len(arr) < 3 {
				continue
			}
			var evt nostr.Event
			if json.Unmarshal(arr[2], &evt) != nil {
				w.stats.count(Invalid)
				continue
			}
			w.stats.received.Add(1)
			size := len(arr[2])
			if key, ok := liveKeys[sub]; ok {
				seen := evt.CreatedAt
				if now := time.Now().Unix(); seen > now {
					seen = now // a future-dated event must not skip the cursor ahead
				}
				w.h.state.Update(w.url, key, func(cur *Cursor) {
					if seen > cur.LiveSince {
						cur.LiveSince = seen
					}
				})
				w.h.enqueue(ctx, evt, size, w.stats, nil)
				continue
			}
			// Counters and wg.Add happen under the lock, so once
			// fetchPage has taken the page off the map (EOSE, CLOSED or
			// timeout) they are final and wg covers every queued event.
			c.mu.Lock()
			p := c.pages[sub]
			if p != nil {
				if evt.CreatedAt > p.until {
					p.newer++
					p = nil
				} else {
					p.count++
					if p.oldest == 0 || evt.CreatedAt < p.oldest {
						p.oldest = evt.CreatedAt
					}
					p.wg.Add(1)
				}
			}
			c.mu.Unlock()
			if p != nil {
				w.h.enqueue(ctx, evt, size, w.stats, &p.wg)
			}
		case "EOSE":
			if p := w.takePage(c, sub); p != nil {
				p.finish("")
			}
		case "CLOSED":
			reason := ""
			if len(arr) >= 3 {
				_ = json.Unmarshal(arr[2], &reason)
			}
			if _, ok := liveKeys[sub]; ok {
				return fmt.Errorf("live subscription closed by relay: %s", reason)
			}
			if p := w.takePage(c, sub); p != nil {
				p.finish("closed: " + reason)
			}
		case "NOTICE":
			log.Harvest().Debug("Upstream notice", "relay", w.url, "notice", sub)
		}
	}
}

func (w *worker) takePage(c *conn, sub string) *page {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.pages[sub]
	delete(c.pages, sub)
	return p
}

// backfill works through every filter's spans, one page at a time.
func (w *worker) backfill(ctx context.Context, c *conn) error {
	cfg := w.h.cfg
	interval := time.Duration(cfg.Backfill.PageIntervalMillis) * time.Millisecond
	seq := 0
	failures := 0
	for {
		progressed := false
		for _, f := range w.filters {
			key := filterKey(f)
			cur := w.h.state.Get(w.url, key)
			if len(cur.Spans) == 0 {
				continue
			}
			span := cur.Spans[0]
			seq++
			res, err := w.fetchPage(ctx, c, "bf:"+strconv.Itoa(seq), f, span)
			if err != nil {
				return err
			}
			if res.retry != "" {
				failures++
				log.Harvest().Warn("Backfill page failed", "relay", w.url, "reason", res.retry, "failures", failures)
				if failures >= 5 {
					return fmt.Errorf("backfill: %d page failures in a row, last: %s", failures, res.retry)
				}
				if err := sleep(ctx, time.Duration(failures)*10*time.Second); err != nil {
					return err
				}
				continue
			}
			failures = 0
			progressed = true
			w.stats.pages.Add(1)
			w.h.state.Update(w.url, key, func(c *Cursor) {
				c.Pages++
				if len(c.Spans) == 0 || c.Spans[0] != span {
					return // the queue changed under us; leave it
				}
				if res.done {
					c.Spans = c.Spans[1:]
				} else {
					c.Spans[0].Until = res.nextUntil
				}
			})
			if res.done {
				log.Harvest().Info("Backfill span finished", "relay", w.url, "filter", key,
					"floor", span.Floor, "remaining_spans", len(cur.Spans)-1)
			}
			if err := sleep(ctx, interval); err != nil {
				return err
			}
		}
		if !progressed && failures == 0 {
			return nil // every span is done
		}
	}
}

type pageResult struct {
	done      bool   // the span is exhausted
	nextUntil int64  // the next page's until when not done
	retry     string // non-empty: the page failed, try again later
}

func (w *worker) fetchPage(ctx context.Context, c *conn, id string, f map[string]interface{}, span Span) (pageResult, error) {
	p := &page{until: span.Until, done: make(chan string, 1)}
	c.mu.Lock()
	c.pages[id] = p
	c.mu.Unlock()

	if err := c.send([]interface{}{"REQ", id, withPaging(f, span.Floor, span.Until, w.h.cfg.Backfill.PageSize)}); err != nil {
		return pageResult{}, fmt.Errorf("backfill REQ: %w", err)
	}

	timeout := time.Duration(w.h.cfg.Backfill.PageTimeoutSeconds) * time.Second
	var reason string
	timedOut := false
	select {
	case <-ctx.Done():
		return pageResult{}, ctx.Err()
	case reason = <-p.done:
	case <-time.After(timeout):
		timedOut = true
		w.takePage(c, id)
	}
	c.mu.Lock() // pairs with the reader's lock: counters are final now
	count, oldest, newer := p.count, p.oldest, p.newer
	c.mu.Unlock()
	if !strings.HasPrefix(reason, "closed:") {
		_ = c.send([]interface{}{"CLOSE", id})
	}

	// The cursor only moves once every event of the page is stored, so a
	// crash mid-page re-fetches the page instead of skipping it.
	stored := make(chan struct{})
	go func() { p.wg.Wait(); close(stored) }()
	select {
	case <-ctx.Done():
		return pageResult{}, ctx.Err()
	case <-stored:
	}

	if reason != "" && count == 0 {
		return pageResult{retry: reason}, nil
	}
	if timedOut && count == 0 {
		return pageResult{retry: "timeout"}, nil
	}
	if count == 0 {
		if newer > 0 {
			log.Harvest().Warn("Relay ignores until; backfill not possible on this filter",
				"relay", w.url, "events_after_until", newer)
		}
		return pageResult{done: true}, nil
	}
	// until is inclusive, so the next page starts at the oldest second
	// seen: events sharing that second beyond the page limit are not
	// skipped. A page entirely inside one second has to step past it.
	next := oldest
	if next >= span.Until {
		next = span.Until - 1
	}
	if next < span.Floor {
		return pageResult{done: true}, nil
	}
	return pageResult{nextUntil: next}, nil
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
