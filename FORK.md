# glyphbyte relay

This is [grain](https://github.com/0ceanSlim/grain) by OceanSlim (MIT, see `license`) plus:

- **Harvester** (`server/harvest`): with `harvest.yml` in the data directory, the relay copies events from configured relays (live tail + paged backfill), running every event through the relay's own checks, skipping ephemeral and NIP-70 protected events, and never bringing back an event its author deleted.
- **Deploy profile** (`deploy/glyphbyte`): content kinds only, per-kind size limits lifted for an archive.

Docs: [docs/harvest.md](docs/harvest.md), [docs/examples/harvest.example.yml](docs/examples/harvest.example.yml)

## Updating from upstream grain

The full grain history is kept, so updates are ordinary merges:

```sh
git remote add upstream https://github.com/0ceanSlim/grain.git   # once
git fetch upstream
git merge upstream/main
git submodule update --init --recursive
```

Conflicts can only happen in the few grain files this repo changes:

| File | Change |
|---|---|
| `server/startup.go` | starts the harvester; watches `harvest.yml` |
| `server/utils/log/components.go` | `harvest` log component |
| `.gitignore` | nostrdb build outputs; deploy profile configs |

Everything else this repo adds lives in new files. After merging, build and test:

```sh
(cd server/db/nostrdb && bash build.sh)   # needs gcc, make, autoconf, automake, libtool
go build . && go test ./server/...
```
