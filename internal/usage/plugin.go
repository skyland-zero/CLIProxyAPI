package usage

import (
	"sync/atomic"
)

var statisticsEnabled atomic.Bool

func init() {
	statisticsEnabled.Store(true)
}

// SetStatisticsEnabled toggles built-in usage recording.
func SetStatisticsEnabled(enabled bool) { statisticsEnabled.Store(enabled) }

// StatisticsEnabled reports whether built-in usage recording is enabled.
func StatisticsEnabled() bool { return statisticsEnabled.Load() }
