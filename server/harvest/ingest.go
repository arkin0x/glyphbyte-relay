package harvest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/0ceanslim/grain/config"
	nostr "github.com/0ceanslim/grain/server/types"
	"github.com/0ceanslim/grain/server/validation"
)

// Store is the slice of the event store the harvester needs. *nostrdb.NDB
// satisfies it; tests use a fake.
type Store interface {
	CheckDuplicateEvent(evt nostr.Event) (bool, error)
	Query(filters []nostr.Filter, limit int) ([]nostr.Event, error)
	StoreEvent(ctx context.Context, evt nostr.Event) error
	ProcessDeletion(ctx context.Context, evt nostr.Event) error
	MapUsageFraction() float64
}

// Outcome is what happened to one harvested event.
type Outcome int

const (
	Stored    Outcome = iota // new event, now stored
	Duplicate                // already stored
	Invalid                  // malformed or bad id/signature
	Failed                   // store error after retries

	// Refused by policy, one outcome per reason so operators can see why.
	SkipKind        // ephemeral or in exclude_kinds
	SkipProtected   // NIP-70 ["-"] tag: only the author may publish it
	SkipExpired     // NIP-40 expiration already passed
	SkipTimestamp   // created_at outside the relay's bounds
	SkipSize        // over the relay's size limit for its kind
	SkipBlacklist   // author or content banned
	SkipWhitelist   // author not whitelisted (respect_whitelist only)
	SkipDeleted     // the author already deleted it (NIP-09)
	SkipStoreReject // store refused it, e.g. an older replaceable version
)

// IsSkip reports whether the outcome is a policy refusal.
func (o Outcome) IsSkip() bool { return o >= SkipKind }

// String names an outcome for logs and status output.
func (o Outcome) String() string {
	return [...]string{"stored", "duplicate", "invalid", "failed", "kind", "protected",
		"expired", "timestamp", "size", "blacklist", "whitelist", "deleted", "store_reject"}[o]
}

// errStoreFull means the store is near its map ceiling; workers pause.
var errStoreFull = errors.New("store near capacity")

// Checks are the relay-policy hooks the ingester applies. They default to the
// relay's own validators; tests replace them.
type Checks struct {
	// Timestamp reports whether created_at is inside the relay's bounds.
	Timestamp func(evt nostr.Event) bool
	// Size reports whether the serialized event size is allowed for its kind.
	Size func(kind, size int) bool
	// Blacklisted reports whether the author or content is banned.
	Blacklisted func(evt nostr.Event) bool
	// Whitelisted reports whether the author passes the whitelist; only
	// consulted when respect_whitelist is on.
	Whitelisted func(evt nostr.Event) bool
	// Signature verifies the event id and Schnorr signature.
	Signature func(evt nostr.Event) bool
	// MapRejectFraction is the store fill level at which writes stop.
	MapRejectFraction float64
}

// RelayChecks returns the checks a client-published event goes through.
func RelayChecks(mapRejectFraction float64) Checks {
	return Checks{
		Timestamp: func(evt nostr.Event) bool {
			cfg := config.GetConfig()
			return cfg != nil && validation.ValidateEventTimestamp(evt, cfg)
		},
		Size: func(kind, size int) bool {
			lim := config.GetSizeLimiter()
			if lim == nil {
				return true
			}
			ok, _ := lim.AllowSize(kind, size)
			return ok
		},
		Blacklisted: func(evt nostr.Event) bool {
			banned, _ := config.CheckBlacklistCached(evt.PubKey, evt.Content)
			return banned
		},
		Whitelisted: func(evt nostr.Event) bool {
			ok, _ := config.CheckWhitelistCached(evt)
			return ok
		},
		Signature:         validation.CheckSignature,
		MapRejectFraction: mapRejectFraction,
	}
}

// Ingester verifies and stores harvested events.
type Ingester struct {
	cfg    *Config
	store  Store
	checks Checks

	inflightMu sync.Mutex
	inflight   map[string]struct{}
}

// NewIngester builds an ingester over a store.
func NewIngester(cfg *Config, store Store, checks Checks) *Ingester {
	return &Ingester{cfg: cfg, store: store, checks: checks, inflight: map[string]struct{}{}}
}

// Ingest runs one event through the relay's checks and stores it. size is the
// length of the event's JSON as received.
func (in *Ingester) Ingest(ctx context.Context, evt nostr.Event, size int) (Outcome, error) {
	if !wellFormed(evt) {
		return Invalid, nil
	}
	if in.cfg.excluded(evt.Kind) {
		return SkipKind, nil
	}
	// NIP-70: a protected event may only be published by its author over
	// an authenticated connection. A third party copying it is exactly
	// what the tag forbids.
	if validation.IsProtectedEvent(evt) {
		return SkipProtected, nil
	}
	if validation.IsExpired(evt, time.Now().Unix()) {
		return SkipExpired, nil
	}
	if in.checks.Timestamp != nil && !in.checks.Timestamp(evt) {
		return SkipTimestamp, nil
	}
	if in.checks.Size != nil && !in.checks.Size(evt.Kind, size) {
		return SkipSize, nil
	}
	if in.checks.Blacklisted != nil && in.checks.Blacklisted(evt) {
		return SkipBlacklist, nil
	}
	if in.cfg.RespectWhitelist && in.checks.Whitelisted != nil && !in.checks.Whitelisted(evt) {
		return SkipWhitelist, nil
	}

	// The same event usually arrives from several relays at once; let one
	// of them do the work.
	if !in.claim(evt.ID) {
		return Duplicate, nil
	}
	defer in.release(evt.ID)

	dup, err := in.store.CheckDuplicateEvent(evt)
	if err != nil {
		return Failed, fmt.Errorf("duplicate check: %w", err)
	}
	if dup {
		return Duplicate, nil
	}
	if in.checks.Signature != nil && !in.checks.Signature(evt) {
		return Invalid, nil
	}
	if *in.cfg.HonorDeletions && evt.Kind != 5 {
		deleted, err := in.deleted(evt)
		if err != nil {
			return Failed, fmt.Errorf("deletion check: %w", err)
		}
		if deleted {
			return SkipDeleted, nil
		}
	}
	if evt.Kind != 5 && in.checks.MapRejectFraction > 0 &&
		in.store.MapUsageFraction() >= in.checks.MapRejectFraction {
		return Failed, errStoreFull
	}

	if err := in.storeWithRetry(ctx, evt); err != nil {
		if isReject(err.Error()) {
			// e.g. an older version of a replaceable event we already
			// hold a newer one of.
			return SkipStoreReject, nil
		}
		return Failed, err
	}
	return Stored, nil
}

func (in *Ingester) claim(id string) bool {
	in.inflightMu.Lock()
	defer in.inflightMu.Unlock()
	if _, busy := in.inflight[id]; busy {
		return false
	}
	in.inflight[id] = struct{}{}
	return true
}

func (in *Ingester) release(id string) {
	in.inflightMu.Lock()
	delete(in.inflight, id)
	in.inflightMu.Unlock()
}

// deleted reports whether the store holds a NIP-09 deletion of evt by its
// author: a kind 5 naming its id in an `e` tag, or, for a replaceable or
// addressable event, naming its address in an `a` tag with a created_at at or
// after the event's.
func (in *Ingester) deleted(evt nostr.Event) (bool, error) {
	byID := nostr.Filter{
		Authors: []string{evt.PubKey},
		Kinds:   []int{5},
		Tags:    map[string][]string{"e": {evt.ID}},
	}
	hits, err := in.store.Query([]nostr.Filter{byID}, 1)
	if err != nil {
		return false, err
	}
	if len(hits) > 0 {
		return true, nil
	}
	addr := address(evt)
	if addr == "" {
		return false, nil
	}
	byAddr := nostr.Filter{
		Authors: []string{evt.PubKey},
		Kinds:   []int{5},
		Tags:    map[string][]string{"a": {addr}},
	}
	hits, err = in.store.Query([]nostr.Filter{byAddr}, 50)
	if err != nil {
		return false, err
	}
	for _, d := range hits {
		if d.CreatedAt >= evt.CreatedAt {
			return true, nil
		}
	}
	return false, nil
}

// address returns the NIP-01 address of a replaceable or addressable event
// ("kind:pubkey:d"), or "" for other kinds.
func address(evt nostr.Event) string {
	k := evt.Kind
	switch {
	case k == 0 || k == 3 || (k >= 10000 && k < 20000):
		return strconv.Itoa(k) + ":" + evt.PubKey + ":"
	case k >= 30000 && k < 40000:
		d := ""
		for _, t := range evt.Tags {
			if len(t) >= 2 && t[0] == "d" {
				d = t[1]
				break
			}
		}
		return strconv.Itoa(k) + ":" + evt.PubKey + ":" + d
	}
	return ""
}

func (in *Ingester) storeWithRetry(ctx context.Context, evt nostr.Event) error {
	backoff := 5 * time.Millisecond
	var err error
	for attempt := 0; attempt < 8; attempt++ {
		if evt.Kind == 5 {
			err = in.store.ProcessDeletion(ctx, evt)
		} else {
			err = in.store.StoreEvent(ctx, evt)
		}
		if err == nil || isReject(err.Error()) {
			return err
		}
		// Transient (ingest queue full): back off and retry.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > 500*time.Millisecond {
			backoff = 500 * time.Millisecond
		}
	}
	return err
}

// isReject reports a final refusal from the store, not worth retrying: a
// NIP-01 machine-readable rejection, or one of the store's unprefixed
// permanent refusals. Anything else (a full ingest queue) is transient.
func isReject(msg string) bool {
	for _, p := range []string{"blocked:", "duplicate:", "invalid:"} {
		if strings.HasPrefix(msg, p) {
			return true
		}
	}
	for _, s := range []string{"no d tag present", "invalid hex id"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

func wellFormed(evt nostr.Event) bool {
	return isHex(evt.ID, 64) && isHex(evt.PubKey, 64) && isHex(evt.Sig, 128) && evt.Tags != nil
}

func isHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
