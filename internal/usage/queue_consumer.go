package usage

import (
	"context"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/redisqueue"
	log "github.com/sirupsen/logrus"
)

// StartRedisQueueConsumer persists usage records by consuming the redisqueue usage buffer.
// The redisqueue is destructive, so /usage-queue should not be used by external consumers
// while this background consumer is enabled.
func StartRedisQueueConsumer(ctx context.Context, batchSize int, interval time.Duration) {
	if ctx == nil {
		ctx = context.Background()
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	if interval <= 0 {
		interval = time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		consume := func() {
			for {
				items := redisqueue.PopOldest(batchSize)
				if len(items) == 0 {
					return
				}
				for _, item := range items {
					writeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					if err := RecordQueuedPayload(writeCtx, item); err != nil {
						log.WithError(err).Warn("usage: failed to persist queued usage record")
					}
					cancel()
				}
			}
		}
		for {
			consume()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}
