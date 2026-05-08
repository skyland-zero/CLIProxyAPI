package logdb

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

const (
	defaultAppLogBatchSize      = 200
	defaultRequestLogBatchSize  = 100
	defaultFlushInterval        = 250 * time.Millisecond
	defaultRetention            = 7 * 24 * time.Hour
	defaultAppLogQueueSize      = 5000
	defaultRequestLogQueueSize  = 2000
	defaultRequestLogQueueBytes = 64 << 20 // 64 MiB
)

type Config struct {
	DSN string

	Schema string

	AppLogQueueSize      int
	RequestLogQueueSize  int
	RequestLogQueueBytes int64

	AppLogBatchSize     int
	RequestLogBatchSize int

	FlushInterval time.Duration
	Retention     time.Duration
}

type Manager struct {
	pool   *pgxpool.Pool
	schema string

	appLogCh          chan AppLogEntry
	requestLogCh      chan requestLogQueueItem
	requestLogReserve chan struct{}
	requestLogBytes   atomic.Int64
	enqueueMu         sync.RWMutex

	flushInterval           time.Duration
	retention               time.Duration
	maxRequestLogQueueBytes int64

	closed      atomic.Bool
	shutdownCtx atomic.Value
	stopCh      chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
}

var defaultManager = struct {
	sync.RWMutex
	mgr *Manager
}{}

func DefaultManager() *Manager {
	defaultManager.RLock()
	defer defaultManager.RUnlock()
	return defaultManager.mgr
}

func SetDefaultManager(mgr *Manager) {
	defaultManager.Lock()
	defaultManager.mgr = mgr
	defaultManager.Unlock()
}

func Enabled() bool {
	return DefaultManager() != nil
}

func RetentionDays() int {
	mgr := DefaultManager()
	if mgr == nil || mgr.retention <= 0 {
		return 0
	}
	days := int(mgr.retention / (24 * time.Hour))
	if mgr.retention%(24*time.Hour) != 0 {
		days++
	}
	return days
}

func ForceEnableRequestLog(cfg *config.Config) bool {
	if cfg == nil || !Enabled() || cfg.RequestLog {
		return false
	}
	cfg.RequestLog = true
	return true
}

func InitializePostgres(ctx context.Context, cfg Config) error {
	manager, err := NewManager(ctx, cfg)
	if err != nil {
		return err
	}
	if previous := DefaultManager(); previous != nil {
		_ = previous.Shutdown(context.Background())
	}
	SetDefaultManager(manager)
	return nil
}

func ShutdownDefault(ctx context.Context) error {
	manager := DefaultManager()
	if manager == nil {
		return nil
	}
	SetDefaultManager(nil)
	return manager.Shutdown(ctx)
}

func NewManager(ctx context.Context, cfg Config) (*Manager, error) {
	dsn := strings.TrimSpace(cfg.DSN)
	if dsn == "" {
		return nil, fmt.Errorf("logdb: DSN is required")
	}
	if cfg.AppLogQueueSize <= 0 {
		cfg.AppLogQueueSize = defaultAppLogQueueSize
	}
	if cfg.RequestLogQueueSize <= 0 {
		cfg.RequestLogQueueSize = defaultRequestLogQueueSize
	}
	if cfg.RequestLogQueueBytes <= 0 {
		cfg.RequestLogQueueBytes = defaultRequestLogQueueBytes
	}
	if cfg.AppLogBatchSize <= 0 {
		cfg.AppLogBatchSize = defaultAppLogBatchSize
	}
	if cfg.RequestLogBatchSize <= 0 {
		cfg.RequestLogBatchSize = defaultRequestLogBatchSize
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = defaultFlushInterval
	}
	if cfg.Retention <= 0 {
		cfg.Retention = defaultRetention
	}

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("logdb: parse DSN: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("logdb: create pool: %w", err)
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("logdb: ping database: %w", err)
	}

	mgr := &Manager{
		pool:                    pool,
		schema:                  strings.TrimSpace(cfg.Schema),
		appLogCh:                make(chan AppLogEntry, cfg.AppLogQueueSize),
		requestLogCh:            make(chan requestLogQueueItem, cfg.RequestLogQueueSize),
		requestLogReserve:       make(chan struct{}, cfg.RequestLogQueueSize),
		flushInterval:           cfg.FlushInterval,
		retention:               cfg.Retention,
		maxRequestLogQueueBytes: cfg.RequestLogQueueBytes,
		stopCh:                  make(chan struct{}),
	}
	if err = mgr.ensureSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err = mgr.prune(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("logdb: prune existing rows: %w", err)
	}
	mgr.start(cfg)
	return mgr, nil
}

func (m *Manager) Shutdown(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m.shutdownCtx.Store(ctx)
	m.stopOnce.Do(func() {
		m.enqueueMu.Lock()
		defer m.enqueueMu.Unlock()
		m.closed.Store(true)
		close(m.stopCh)
	})

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		m.pool.Close()
		return ctx.Err()
	}
	m.pool.Close()
	return nil
}

func (m *Manager) schemaTable(name string) string {
	if m == nil || strings.TrimSpace(m.schema) == "" {
		return quoteIdentifier(name)
	}
	return quoteIdentifier(m.schema) + "." + quoteIdentifier(name)
}

func quoteIdentifier(identifier string) string {
	replaced := strings.ReplaceAll(identifier, "\"", "\"\"")
	return "\"" + replaced + "\""
}

func (m *Manager) writeError(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
}

func (m *Manager) flushContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	if m != nil {
		if value := m.shutdownCtx.Load(); value != nil {
			if ctx, ok := value.(context.Context); ok {
				return ctx, func() {}
			}
		}
	}
	return context.WithTimeout(context.Background(), timeout)
}
