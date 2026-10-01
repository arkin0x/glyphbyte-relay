package harvest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/0ceanslim/grain/client/core"
	nostr "github.com/0ceanslim/grain/server/types"
	"github.com/0ceanslim/grain/server/validation"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
	"golang.org/x/net/websocket"
)

// signer makes real signed events for one key.
type signer struct {
	priv   *btcec.PrivateKey
	pubkey string
}

func newSigner(t *testing.T) *signer {
	t.Helper()
	priv, err := btcec.NewPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	return &signer{priv: priv, pubkey: hex.EncodeToString(schnorr.SerializePubKey(priv.PubKey()))}
}

func (s *signer) event(t *testing.T, kind int, createdAt int64, content string, tags ...[]string) nostr.Event {
	t.Helper()
	if tags == nil {
		tags = [][]string{}
	}
	evt := nostr.Event{PubKey: s.pubkey, CreatedAt: createdAt, Kind: kind, Tags: tags, Content: content}
	sum := sha256.Sum256([]byte(core.SerializeEvent(evt)))
	evt.ID = hex.EncodeToString(sum[:])
	sig, err := schnorr.Sign(s.priv, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	evt.Sig = hex.EncodeToString(sig.Serialize())
	return evt
}

// fakeStore is an in-memory Store.
type fakeStore struct {
	mu      sync.Mutex
	events  map[string]nostr.Event
	rejects map[string]string // id -> error returned by StoreEvent
	full    bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{events: map[string]nostr.Event{}, rejects: map[string]string{}}
}

func (f *fakeStore) CheckDuplicateEvent(evt nostr.Event) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.events[evt.ID]
	return ok, nil
}

func (f *fakeStore) Query(filters []nostr.Filter, limit int) ([]nostr.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []nostr.Event
	for _, e := range f.events {
		for _, flt := range filters {
			if flt.MatchesEvent(e) {
				out = append(out, e)
				break
			}
		}
	}
	return out, nil
}

func (f *fakeStore) StoreEvent(_ context.Context, evt nostr.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if msg, ok := f.rejects[evt.ID]; ok {
		return errString(msg)
	}
	f.events[evt.ID] = evt
	return nil
}

func (f *fakeStore) ProcessDeletion(ctx context.Context, evt nostr.Event) error {
	f.mu.Lock()
	for _, t := range evt.Tags {
		if len(t) >= 2 && t[0] == "e" {
			if target, ok := f.events[t[1]]; ok && target.PubKey == evt.PubKey {
				delete(f.events, t[1])
			}
		}
	}
	f.mu.Unlock()
	return f.StoreEvent(ctx, evt)
}

func (f *fakeStore) MapUsageFraction() float64 {
	if f.full {
		return 1
	}
	return 0
}

func (f *fakeStore) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.events)
}

func (f *fakeStore) has(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.events[id]
	return ok
}

type errString string

func (e errString) Error() string { return string(e) }

// testChecks are the relay checks without the global config: signature is
// real, everything else passes.
func testChecks() Checks {
	return Checks{Signature: validation.CheckSignature, MapRejectFraction: 0.97}
}

func mustConfig(t *testing.T, yml string) *Config {
	t.Helper()
	c, err := ParseConfig([]byte(yml))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// fakeRelay is an upstream relay over a real websocket. It answers REQs from
// a fixed event set honoring since/until/limit (newest first, capped at
// maxLimit like strfry), then streams events pushed with publish to open
// subscriptions.
type fakeRelay struct {
	t        *testing.T
	srv      *httptest.Server
	maxLimit int

	mu     sync.Mutex
	events []nostr.Event
	subs   map[*websocket.Conn]map[string]map[string]interface{}
	reqs   int
}

func newFakeRelay(t *testing.T, events []nostr.Event, maxLimit int) *fakeRelay {
	r := &fakeRelay{t: t, events: events, maxLimit: maxLimit, subs: map[*websocket.Conn]map[string]map[string]interface{}{}}
	r.srv = httptest.NewServer(websocket.Handler(r.serve))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *fakeRelay) url() string { return "ws" + strings.TrimPrefix(r.srv.URL, "http") }

func (r *fakeRelay) serve(ws *websocket.Conn) {
	r.mu.Lock()
	r.subs[ws] = map[string]map[string]interface{}{}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.subs, ws)
		r.mu.Unlock()
	}()
	for {
		var msg string
		if err := websocket.Message.Receive(ws, &msg); err != nil {
			return
		}
		var arr []json.RawMessage
		if json.Unmarshal([]byte(msg), &arr) != nil || len(arr) < 2 {
			continue
		}
		var typ, sub string
		_ = json.Unmarshal(arr[0], &typ)
		_ = json.Unmarshal(arr[1], &sub)
		switch typ {
		case "REQ":
			var f map[string]interface{}
			_ = json.Unmarshal(arr[2], &f)
			r.mu.Lock()
			r.reqs++
			r.subs[ws][sub] = f
			matched := r.match(f)
			r.mu.Unlock()
			for _, e := range matched {
				r.send(ws, []interface{}{"EVENT", sub, e})
			}
			r.send(ws, []interface{}{"EOSE", sub})
		case "CLOSE":
			r.mu.Lock()
			delete(r.subs[ws], sub)
			r.mu.Unlock()
		}
	}
}

func (r *fakeRelay) send(ws *websocket.Conn, msg []interface{}) {
	b, _ := json.Marshal(msg)
	_ = websocket.Message.Send(ws, string(b))
}

func num(f map[string]interface{}, k string) (int64, bool) {
	v, ok := f[k].(float64)
	return int64(v), ok
}

// match must be called with r.mu held.
func (r *fakeRelay) match(f map[string]interface{}) []nostr.Event {
	var out []nostr.Event
	since, hasSince := num(f, "since")
	until, hasUntil := num(f, "until")
	for _, e := range r.events {
		if hasSince && e.CreatedAt < since {
			continue
		}
		if hasUntil && e.CreatedAt > until {
			continue
		}
		if kinds, ok := f["kinds"].([]interface{}); ok {
			hit := false
			for _, k := range kinds {
				if int(k.(float64)) == e.Kind {
					hit = true
				}
			}
			if !hit {
				continue
			}
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt > out[j].CreatedAt })
	limit := r.maxLimit
	if l, ok := num(f, "limit"); ok && int(l) < limit {
		limit = int(l)
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

// publish adds an event and pushes it to every open subscription it matches.
func (r *fakeRelay) publish(e nostr.Event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	type target struct {
		ws  *websocket.Conn
		sub string
	}
	var targets []target
	for ws, subs := range r.subs {
		for id, f := range subs {
			if strings.HasPrefix(id, "live:") {
				targets = append(targets, target{ws, id})
			}
			_ = f
		}
	}
	r.mu.Unlock()
	for _, tg := range targets {
		r.send(tg.ws, []interface{}{"EVENT", tg.sub, e})
	}
}
