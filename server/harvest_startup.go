package server

import (
	"context"

	"github.com/0ceanslim/grain/config"
	"github.com/0ceanslim/grain/server/db/nostrdb"
	"github.com/0ceanslim/grain/server/harvest"
	"github.com/0ceanslim/grain/server/utils/log"
)

const (
	harvestConfigFile = "harvest.yml"
	harvestStateFile  = "harvest_state.json"
)

// startHarvester starts pulling events from upstream relays when harvest.yml
// exists and is enabled. It stops with ctx, i.e. on every config reload or
// shutdown, and saves its cursors so the next instance resumes.
func startHarvester(ctx context.Context, db *nostrdb.NDB) {
	cfg, err := harvest.LoadConfig(config.ConfigPath(harvestConfigFile))
	if err != nil {
		log.Harvest().Error("Harvest config invalid; harvesting disabled", "error", err)
		return
	}
	if cfg == nil || !cfg.Enabled {
		return
	}
	ingester := harvest.NewIngester(cfg, db, harvest.RelayChecks(nostrdb.MapUsageRejectFraction))
	ingester.OnStored = BroadcastEvent
	h, err := harvest.New(cfg, ingester, config.ConfigPath(harvestStateFile))
	if err != nil {
		log.Harvest().Error("Harvest state unreadable; harvesting disabled", "error", err)
		return
	}
	go h.Run(ctx)
}
