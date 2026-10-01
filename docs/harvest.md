# Harvesting events from other relays

A harvesting relay subscribes to a list of upstream relays and stores what
they serve, so it ends up holding the union of their events. Use it to run a
search or lookup relay (for example one that answers id-prefix queries) over
more of the network than people publish to it directly.

Harvesting is off unless `harvest.yml` exists in the data directory with
`enabled: true`. Start from [`examples/harvest.example.yml`](examples/harvest.example.yml).

## What it does

For every upstream relay, one worker keeps a single websocket open and runs two
things over it:

| Part | What it sends | When it is done |
|---|---|---|
| Live tail | `REQ live:<filter> {…filter, since: now − overlap}` | never; it stays open |
| Backfill | `REQ bf:<n> {…filter, until: U, limit: page_size}`, one page at a time, newest first | when a page comes back empty, or `until` passes `max_age_days` |

The next page's `until` is the oldest `created_at` seen in the page. `until` is
inclusive, so events sharing that second are fetched again (duplicates are
cheap) rather than skipped when a page is cut off in the middle of a second.
A page that lies entirely inside one second steps one second past it.

**Gaps.** On every reconnect the stretch between the newest live event seen
and now is queued as a new backfill span, so an outage of any length is paged
in instead of trusting one live `REQ` to return all of it.

**Cursors.** Progress is saved every 10 seconds to `harvest_state.json`, keyed
by relay URL and by a hash of each filter. A changed filter therefore starts
fresh; a reordered list does not. A page's cursor only moves once every event
in it has been verified and stored, so a crash re-fetches a page instead of
skipping it.

## What gets stored

Each event goes through these checks, in order. The first failure decides.

| Check | Outcome name in stats |
|---|---|
| id, pubkey and sig are lowercase hex of the right length | `invalid` |
| kind is not ephemeral (20000-29999) and not in `exclude_kinds` | `kind` |
| no NIP-70 `["-"]` tag: only the author may publish a protected event | `protected` |
| NIP-40 expiration not passed | `expired` |
| `created_at` within the relay's bounds (`event_time_constraints`) | `timestamp` |
| size within the relay's limits for the kind | `size` |
| author and content not blacklisted | `blacklist` |
| author whitelisted, only with `respect_whitelist: true` | `whitelist` |
| not already stored | `duplicate` |
| id hash and Schnorr signature verify | `invalid` |
| no stored NIP-09 deletion of it by its author | `deleted` |
| the store accepts it (e.g. not an older replaceable version) | `store_reject` |

**Why the deletion check exists.** The store removes events when a kind 5
arrives but keeps no tombstone. Backfill walks newest to oldest, so a deletion
usually arrives *before* the event it deletes; without this check every
deleted event would come back. The check looks for a stored kind 5 by the same
author naming the event's id (`e` tag) or, for a replaceable or addressable
event, its address (`a` tag) with a `created_at` at or after the event's.

Stored events are pushed to this relay's live subscribers exactly like
published ones.

## Operating it

- Logs use the `harvest` component. Every `stats_interval_seconds` each relay
  logs `received`, `stored`, `duplicate`, `invalid`, `failed`, `skipped` (by
  reason), `pages`, `reconnects` and `backfill_spans_left`.
- Keep `event_purge` off (the default) or the purge will delete harvested
  events from non-whitelisted authors.
- Keep `database.map_size_mb` well above the expected size. Writes stop at 97%
  of the map; the harvester then pauses its workers for 30 s at a time.
- Relays that require NIP-42 AUTH to read are not supported yet; their pages
  fail with `auth-required` and are retried with backoff.
- A relay that ignores `until` cannot be backfilled; the worker logs a warning
  and finishes that span.
