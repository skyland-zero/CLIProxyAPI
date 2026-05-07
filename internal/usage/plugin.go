package usage

import (
	"context"
	"sync/atomic"

	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

var statisticsEnabled atomic.Bool

func init() {
	statisticsEnabled.Store(true)
	coreusage.RegisterPlugin(plugin{})
}

type plugin struct{}

func (plugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	if !StatisticsEnabled() {
		return
	}
	store := DefaultStore()
	if store == nil {
		return
	}
	if err := store.Record(ctx, BuildEvent(ctx, record)); err != nil {
		log.WithError(err).Warn("usage: failed to record usage event")
	}
}

// SetStatisticsEnabled toggles built-in usage recording.
func SetStatisticsEnabled(enabled bool) { statisticsEnabled.Store(enabled) }

// StatisticsEnabled reports whether built-in usage recording is enabled.
func StatisticsEnabled() bool { return statisticsEnabled.Load() }
