package harvest

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	nostr "github.com/0ceanslim/grain/server/types"
)

func TestParseConfigDefaults(t *testing.T) {
	c := mustConfig(t, `
enabled: true
relays: ["wss://a.example/", "wss://a.example", " wss://b.example "]
filters:
  - {kinds: [1], since: 5, until: 6, limit: 7}
`)
	if got := c.Relays; len(got) != 2 || got[0].URL != "wss://a.example" || got[1].URL != "wss://b.example" {
		t.Fatalf("relays not normalised/deduped: %v", got)
	}
	if len(c.Relays[1].Filters) != 1 || c.Relays[1].Filters[0]["kinds"] == nil {
		t.Fatal("a bare relay URL inherits the global filters")
	}
	f := c.Filters[0]
	for _, k := range []string{"since", "until", "limit"} {
		if _, ok := f[k]; ok {
			t.Fatalf("paging field %q must be stripped from configured filters", k)
		}
	}
	if !*c.HonorDeletions || !*c.Live.Enabled || !*c.Backfill.Enabled {
		t.Fatal("honor_deletions, live and backfill default to true")
	}
	if c.Backfill.PageSize != 500 || c.Workers != 4 || c.Live.OverlapSeconds != 120 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestParseConfigPerRelayFilters(t *testing.T) {
	c := mustConfig(t, `
enabled: true
relays:
  - wss://a.example
  - url: wss://picky.example
    filters: [{kinds: [0, 3], limit: 9}]
`)
	if len(c.Relays[0].Filters) != 1 || len(c.Relays[0].Filters[0]) != 0 {
		t.Fatalf("bare relay must get the default match-all filter: %v", c.Relays[0].Filters)
	}
	f := c.Relays[1].Filters
	if len(f) != 1 || f[0]["kinds"] == nil || f[0]["limit"] != nil {
		t.Fatalf("per-relay filters must be kept, minus paging fields: %v", f)
	}
}

func TestParseConfigEmptyFiltersMatchEverything(t *testing.T) {
	c := mustConfig(t, "enabled: true\nrelays: [wss://a.example]\n")
	if len(c.Filters) != 1 || len(c.Filters[0]) != 0 {
		t.Fatalf("want one empty filter, got %v", c.Filters)
	}
}

func TestParseConfigRejects(t *testing.T) {
	for _, yml := range []string{
		"enabled: true\n", // no relays
		"enabled: true\nrelays: [https://a.example]\n", // not a websocket url
	} {
		if _, err := ParseConfig([]byte(yml)); err == nil {
			t.Fatalf("expected error for %q", yml)
		}
	}
	if c, err := ParseConfig([]byte("enabled: false\n")); err != nil || c.Enabled {
		t.Fatal("a disabled config with no relays is valid")
	}
}

func TestLoadConfigMissingFileMeansOff(t *testing.T) {
	c, err := LoadConfig(filepath.Join(t.TempDir(), "harvest.yml"))
	if c != nil || err != nil {
		t.Fatalf("missing file must return nil, nil; got %v, %v", c, err)
	}
}

func TestFilterKeyIgnoresKeyOrder(t *testing.T) {
	a := filterKey(map[string]interface{}{"kinds": []interface{}{1}, "authors": []interface{}{"ab"}})
	b := filterKey(map[string]interface{}{"authors": []interface{}{"ab"}, "kinds": []interface{}{1}})
	c := filterKey(map[string]interface{}{"kinds": []interface{}{2}})
	if a != b || a == c {
		t.Fatalf("filterKey must be order-independent and content-sensitive: %s %s %s", a, b, c)
	}
}

func TestIngestOutcomes(t *testing.T) {
	ctx := context.Background()
	alice := newSigner(t)
	bob := newSigner(t)
	now := time.Now().Unix()

	note := alice.event(t, 1, now-100, "hello")
	ephemeral := alice.event(t, 20001, now-100, "")
	protected := alice.event(t, 1, now-100, "mine only", []string{"-"})
	excluded := alice.event(t, 7, now-100, "+")
	tampered := alice.event(t, 1, now-100, "original")
	tampered.Content = "edited"

	// Deleted by id: the deletion is already stored.
	gone := alice.event(t, 1, now-300, "regret")
	delGone := alice.event(t, 5, now-200, "", []string{"e", gone.ID})
	// Someone else's deletion must not hide alice's event.
	kept := alice.event(t, 1, now-300, "keep")
	bobDel := bob.event(t, 5, now-200, "", []string{"e", kept.ID})
	// Addressable: deleted up to the deletion's created_at only.
	oldArticle := alice.event(t, 30023, now-500, "v1", []string{"d", "post"})
	newArticle := alice.event(t, 30023, now-100, "v2", []string{"d", "post"})
	delArticle := alice.event(t, 5, now-300, "", []string{"a", "30023:" + alice.pubkey + ":post"})
	// The store refuses a stale replaceable, and an addressable without d.
	stale := alice.event(t, 0, now-1000, "{}")
	noD := alice.event(t, 30001, now-1000, "")

	store := newFakeStore()
	store.rejects[stale.ID] = "blocked: a newer version is stored"
	cfg := mustConfig(t, "enabled: true\nrelays: [wss://a.example]\nexclude_kinds: [7]\n")
	store.rejects[noD.ID] = "no d tag present in addressable event"
	in := NewIngester(cfg, store, testChecks())

	for _, d := range []nostr.Event{delGone, bobDel, delArticle} {
		if o, err := in.Ingest(ctx, d, 100); o != Stored || err != nil {
			t.Fatalf("deletion not stored: %v %v", o, err)
		}
	}

	cases := []struct {
		name string
		evt  nostr.Event
		want Outcome
	}{
		{"new note", note, Stored},
		{"same note again", note, Duplicate},
		{"ephemeral kind", ephemeral, SkipKind},
		{"NIP-70 protected", protected, SkipProtected},
		{"excluded kind", excluded, SkipKind},
		{"bad signature", tampered, Invalid},
		{"deleted by its author", gone, SkipDeleted},
		{"deletion by someone else is ignored", kept, Stored},
		{"addressable older than its deletion", oldArticle, SkipDeleted},
		{"addressable newer than its deletion", newArticle, Stored},
		{"store rejection", stale, SkipStoreReject},
		{"unprefixed permanent store refusal", noD, SkipStoreReject},
		{"malformed", nostr.Event{ID: "xyz"}, Invalid},
	}
	for _, c := range cases {
		got, err := in.Ingest(ctx, c.evt, 200)
		if err != nil {
			t.Fatalf("%s: unexpected error %v", c.name, err)
		}
		if got != c.want {
			t.Errorf("%s: got outcome %s, want %s", c.name, got, c.want)
		}
	}
}

func TestIngestPolicyHooks(t *testing.T) {
	ctx := context.Background()
	s := newSigner(t)
	evt := s.event(t, 1, time.Now().Unix(), "x")
	cfg := mustConfig(t, "enabled: true\nrelays: [wss://a.example]\nrespect_whitelist: true\n")

	block := testChecks()
	block.Blacklisted = func(nostr.Event) bool { return true }
	if o, _ := NewIngester(cfg, newFakeStore(), block).Ingest(ctx, evt, 10); o != SkipBlacklist {
		t.Fatal("blacklisted author must be skipped")
	}
	wl := testChecks()
	wl.Whitelisted = func(nostr.Event) bool { return false }
	if o, _ := NewIngester(cfg, newFakeStore(), wl).Ingest(ctx, evt, 10); o != SkipWhitelist {
		t.Fatal("respect_whitelist must apply the whitelist")
	}
	small := testChecks()
	small.Size = func(kind, size int) bool { return size < 5 }
	if o, _ := NewIngester(cfg, newFakeStore(), small).Ingest(ctx, evt, 10); o != SkipSize {
		t.Fatal("oversized event must be skipped")
	}
	full := newFakeStore()
	full.full = true
	if o, err := NewIngester(cfg, full, testChecks()).Ingest(ctx, evt, 10); o != Failed || err != errStoreFull {
		t.Fatalf("full store must fail with errStoreFull, got %v %v", o, err)
	}
}

var (
	pushedMu sync.Mutex
	pushed   []nostr.Event
)

// runHarvester runs a harvester against relays until cond holds or the
// deadline passes, then stops it.
func runHarvester(t *testing.T, cfg *Config, store *fakeStore, statePath string, cond func(h *Harvester) bool) *Harvester {
	t.Helper()
	h, err := New(cfg, NewIngester(cfg, store, testChecks()), statePath)
	if err != nil {
		t.Fatal(err)
	}
	h.OnStored = func(e nostr.Event) {
		pushedMu.Lock()
		pushed = append(pushed, e)
		pushedMu.Unlock()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.Run(ctx); close(done) }()
	deadline := time.Now().Add(20 * time.Second)
	for !cond(h) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	if !cond(h) {
		t.Fatalf("condition not met before deadline; store has %d events", store.count())
	}
	return h
}

func TestHarvestBackfillPagesThroughHistory(t *testing.T) {
	s := newSigner(t)
	now := time.Now().Unix()
	var history []nostr.Event
	// 230 events over a range of seconds, with clusters sharing a second
	// so the inclusive-until paging is exercised.
	for i := 0; i < 230; i++ {
		history = append(history, s.event(t, 1, now-3600-int64(i/3)*60, "note "+strconv.Itoa(i)))
	}
	relay := newFakeRelay(t, history, 50) // relay caps every page at 50
	cfg := mustConfig(t, `
enabled: true
relays: [`+relay.url()+`]
live: {enabled: false}
backfill: {page_size: 100, page_interval_ms: 1}
stats_interval_seconds: -1
`)
	store := newFakeStore()
	statePath := filepath.Join(t.TempDir(), "harvest_state.json")
	// Done means every event stored AND the span closed by its final,
	// empty page; stopping earlier legitimately leaves a page to fetch.
	h := runHarvester(t, cfg, store, statePath, func(h *Harvester) bool {
		return store.count() == len(history) && h.Statuses()[0].Spans == 0
	})

	st := h.Statuses()[0]
	if st.Stored != int64(len(history)) || st.Invalid != 0 {
		t.Fatalf("status: %+v", st)
	}
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("state not saved: %v", err)
	}
	// A restart must not walk the finished backfill again.
	before := relay.reqs
	store2 := newFakeStore()
	until := time.Now().Add(500 * time.Millisecond)
	runHarvester(t, cfg, store2, statePath, func(*Harvester) bool { return time.Now().After(until) })
	if relay.reqs != before || store2.count() != 0 {
		t.Fatalf("finished backfill was re-run after restart: %d new REQs, %d events",
			relay.reqs-before, store2.count())
	}
}

func TestHarvestLiveTailAndDeletionOrder(t *testing.T) {
	s := newSigner(t)
	now := time.Now().Unix()
	old := s.event(t, 1, now-7200, "will be deleted")
	del := s.event(t, 5, now-60, "", []string{"e", old.ID})
	other := s.event(t, 1, now-7000, "stays")
	relay := newFakeRelay(t, []nostr.Event{old, del, other}, 500)

	cfg := mustConfig(t, `
enabled: true
relays: [`+relay.url()+`]
backfill: {page_interval_ms: 1}
stats_interval_seconds: -1
`)
	store := newFakeStore()
	fresh := s.event(t, 1, now, "live!")
	published := false
	runHarvester(t, cfg, store, filepath.Join(t.TempDir(), "s.json"), func(*Harvester) bool {
		// Once history is in, publish a live event.
		if !published && store.has(other.ID) && store.has(del.ID) {
			relay.publish(fresh)
			published = true
		}
		return store.has(fresh.ID)
	})
	if store.has(old.ID) {
		t.Fatal("an event deleted by its author must not be resurrected by backfill")
	}
	// Only live-tail events reach subscribers; backfilled history does not.
	pushedMu.Lock()
	defer pushedMu.Unlock()
	for _, e := range pushed {
		if e.ID == other.ID {
			t.Fatal("a backfilled event was pushed to live subscribers")
		}
	}
	found := false
	for _, e := range pushed {
		found = found || e.ID == fresh.ID
	}
	if !found {
		t.Fatal("the live event was not pushed to subscribers")
	}
}
