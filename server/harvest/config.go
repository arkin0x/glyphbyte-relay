// Package harvest pulls events from upstream relays into the local store.
//
// A harvesting relay subscribes to a configured list of upstream relays, keeps
// a live tail of new events, and pages backwards through their history. Every
// event goes through the same checks a client-published event does (id and
// signature, timestamp bounds, expiration, size, blacklist) before it is
// stored, so harvesting never lets in an event the relay would refuse from a
// client. Harvesting is opt-in: it only runs when harvest.yml exists in the
// data directory and sets enabled: true.
package harvest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the parsed harvest.yml.
type Config struct {
	Enabled bool `yaml:"enabled"`

	// Relays are the upstream relays to harvest from. Each entry is a URL
	// (wss:// or ws://) or {url, filters} to override Filters for that
	// relay, e.g. for relays that refuse filters without kinds.
	Relays []RelaySpec `yaml:"relays"`

	// Filters are raw NIP-01 filter objects sent upstream. since, until and
	// limit are managed by the harvester and ignored here. An empty list
	// means one filter that matches everything ({}).
	Filters []map[string]interface{} `yaml:"filters"`

	// ExcludeKinds are never stored, on top of the always-skipped ephemeral
	// range (20000-29999), which relays must not store.
	ExcludeKinds []int `yaml:"exclude_kinds"`

	// RespectWhitelist applies the relay's pubkey/domain whitelist to
	// harvested events. Off by default: the whitelist governs who may publish
	// directly, while a harvesting relay wants everyone's events. The
	// blacklist always applies.
	RespectWhitelist bool `yaml:"respect_whitelist"`

	// HonorDeletions refuses to store an event when this relay already holds
	// a NIP-09 deletion (kind 5) for it from the same author. Backfill walks
	// newest to oldest, so a deletion usually arrives before the event it
	// deletes, and the store does not keep tombstones. Default true.
	HonorDeletions *bool `yaml:"honor_deletions"`

	Live     LiveConfig     `yaml:"live"`
	Backfill BackfillConfig `yaml:"backfill"`

	// Workers is the number of goroutines verifying and storing events,
	// shared by all relays. Default 4.
	Workers int `yaml:"workers"`

	// ReconnectMinSeconds / ReconnectMaxSeconds bound the exponential
	// backoff between reconnect attempts to one relay. Defaults 5 and 300.
	ReconnectMinSeconds int `yaml:"reconnect_min_seconds"`
	ReconnectMaxSeconds int `yaml:"reconnect_max_seconds"`

	// IdleTimeoutSeconds reconnects a relay that has sent nothing for this
	// long. A reconnect is cheap (the gap is refilled from the cursor), and
	// it catches sockets that died without closing. Default 600.
	IdleTimeoutSeconds int `yaml:"idle_timeout_seconds"`

	// StatsIntervalSeconds logs a per-relay summary this often. Default 300;
	// 0 keeps the default, a negative value disables it.
	StatsIntervalSeconds int `yaml:"stats_interval_seconds"`
}

// RelaySpec is one upstream relay and, optionally, its own filters.
type RelaySpec struct {
	URL     string                   `yaml:"url"`
	Filters []map[string]interface{} `yaml:"filters"`
}

// UnmarshalYAML accepts a bare URL or a {url, filters} mapping.
func (r *RelaySpec) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		r.URL = n.Value
		return nil
	}
	type plain RelaySpec
	return n.Decode((*plain)(r))
}

// LiveConfig controls the live tail.
type LiveConfig struct {
	// Enabled keeps a live subscription open on every relay. Default true.
	Enabled *bool `yaml:"enabled"`
	// OverlapSeconds re-requests this much history on every (re)connect so
	// events published around a disconnect are not missed. Default 120.
	OverlapSeconds int `yaml:"overlap_seconds"`
}

// BackfillConfig controls the walk back through history.
type BackfillConfig struct {
	// Enabled pages backwards through each relay's history. Default true.
	Enabled *bool `yaml:"enabled"`
	// MaxAgeDays stops the walk at events older than this many days.
	// 0 walks back to the beginning.
	MaxAgeDays int `yaml:"max_age_days"`
	// PageSize is the limit sent with each page request. Relays usually cap
	// it (strfry at 500). Default 500.
	PageSize int `yaml:"page_size"`
	// PageIntervalMillis is the pause between pages on one relay, to stay
	// under upstream rate limits. Default 1000.
	PageIntervalMillis int `yaml:"page_interval_ms"`
	// PageTimeoutSeconds gives up on a page that has not finished (EOSE)
	// after this long and retries it later. Default 60.
	PageTimeoutSeconds int `yaml:"page_timeout_seconds"`
}

// LoadConfig reads harvest.yml. A missing file returns (nil, nil): harvesting
// is simply off.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return ParseConfig(data)
}

// ParseConfig parses harvest.yml content, applies defaults and validates.
func ParseConfig(data []byte) (*Config, error) {
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse harvest config: %w", err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	t := true
	if c.HonorDeletions == nil {
		c.HonorDeletions = &t
	}
	if c.Live.Enabled == nil {
		c.Live.Enabled = &t
	}
	if c.Backfill.Enabled == nil {
		c.Backfill.Enabled = &t
	}
	if c.Live.OverlapSeconds <= 0 {
		c.Live.OverlapSeconds = 120
	}
	if c.Backfill.PageSize <= 0 {
		c.Backfill.PageSize = 500
	}
	if c.Backfill.PageIntervalMillis <= 0 {
		c.Backfill.PageIntervalMillis = 1000
	}
	if c.Backfill.PageTimeoutSeconds <= 0 {
		c.Backfill.PageTimeoutSeconds = 60
	}
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.ReconnectMinSeconds <= 0 {
		c.ReconnectMinSeconds = 5
	}
	if c.ReconnectMaxSeconds <= 0 {
		c.ReconnectMaxSeconds = 300
	}
	if c.ReconnectMaxSeconds < c.ReconnectMinSeconds {
		c.ReconnectMaxSeconds = c.ReconnectMinSeconds
	}
	if c.IdleTimeoutSeconds <= 0 {
		c.IdleTimeoutSeconds = 600
	}
	if c.StatsIntervalSeconds == 0 {
		c.StatsIntervalSeconds = 300
	}
	if len(c.Filters) == 0 {
		c.Filters = []map[string]interface{}{{}}
	}
	stripPaging(c.Filters)
	// Normalise relay URLs, drop duplicates (keeping order) and give every
	// relay its filters.
	seen := map[string]bool{}
	relays := c.Relays[:0]
	for _, r := range c.Relays {
		r.URL = strings.TrimRight(strings.TrimSpace(r.URL), "/")
		if r.URL == "" || seen[r.URL] {
			continue
		}
		seen[r.URL] = true
		if len(r.Filters) == 0 {
			r.Filters = c.Filters
		}
		stripPaging(r.Filters)
		relays = append(relays, r)
	}
	c.Relays = relays
}

// stripPaging removes the fields the harvester manages itself.
func stripPaging(filters []map[string]interface{}) {
	for _, f := range filters {
		delete(f, "since")
		delete(f, "until")
		delete(f, "limit")
	}
}

func (c *Config) validate() error {
	if !c.Enabled {
		return nil
	}
	if len(c.Relays) == 0 {
		return fmt.Errorf("harvest: enabled but no relays configured")
	}
	for _, r := range c.Relays {
		if !strings.HasPrefix(r.URL, "wss://") && !strings.HasPrefix(r.URL, "ws://") {
			return fmt.Errorf("harvest: relay %q must start with wss:// or ws://", r.URL)
		}
	}
	return nil
}

// excluded reports whether a kind is never stored by the harvester.
func (c *Config) excluded(kind int) bool {
	if kind >= 20000 && kind < 30000 {
		return true // ephemeral: relays must not store these
	}
	for _, k := range c.ExcludeKinds {
		if k == kind {
			return true
		}
	}
	return false
}

func (c *Config) reconnectMin() time.Duration {
	return time.Duration(c.ReconnectMinSeconds) * time.Second
}

func (c *Config) reconnectMax() time.Duration {
	return time.Duration(c.ReconnectMaxSeconds) * time.Second
}

// filterKey is a stable identifier for one configured filter, so cursors
// survive reordering the filter list and reset when a filter changes.
func filterKey(f map[string]interface{}) string {
	keys := make([]string, 0, len(f))
	for k := range f {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ordered := make([][2]interface{}, 0, len(keys))
	for _, k := range keys {
		ordered = append(ordered, [2]interface{}{k, f[k]})
	}
	b, _ := json.Marshal(ordered)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// withPaging copies a configured filter and adds the paging fields.
func withPaging(f map[string]interface{}, since, until int64, limit int) map[string]interface{} {
	out := make(map[string]interface{}, len(f)+3)
	for k, v := range f {
		out[k] = v
	}
	if since > 0 {
		out["since"] = since
	}
	if until > 0 {
		out["until"] = until
	}
	if limit > 0 {
		out["limit"] = limit
	}
	return out
}
