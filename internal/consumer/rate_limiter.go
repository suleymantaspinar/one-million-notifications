package consumer

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// Rate Limiter with Worker Pool Pattern
// =============================================================================

// RateLimiterConfig holds configuration for the rate limiter.
type RateLimiterConfig struct {
	// MaxPerSecond is the maximum number of messages allowed per second.
	MaxPerSecond int
	// BurstSize allows temporary bursts above the rate limit.
	// Set to MaxPerSecond for no bursting.
	BurstSize int
}

// DefaultRateLimiterConfig returns the default configuration (100 msg/sec, no burst).
func DefaultRateLimiterConfig() RateLimiterConfig {
	return RateLimiterConfig{
		MaxPerSecond: 100,
		BurstSize:    100,
	}
}

// RateLimiter implements a token bucket rate limiter with worker pool pattern.
// It releases tokens at a fixed rate, and workers must acquire a token before processing.
type RateLimiter struct {
	config      RateLimiterConfig
	tokens      chan struct{}    // Token bucket channel
	ticker      *time.Ticker     // Releases tokens at fixed rate
	stopCh      chan struct{}    // Signal to stop token generation
	logger      *slog.Logger
	wg          sync.WaitGroup
	running     atomic.Bool
	
	// Metrics
	acquired    atomic.Int64     // Total tokens acquired
	waitTimeMs  atomic.Int64     // Total wait time in milliseconds
}

// NewRateLimiter creates a new rate limiter with the given configuration.
func NewRateLimiter(config RateLimiterConfig, logger *slog.Logger) *RateLimiter {
	if config.MaxPerSecond <= 0 {
		config.MaxPerSecond = 100
	}
	if config.BurstSize <= 0 {
		config.BurstSize = config.MaxPerSecond
	}

	rl := &RateLimiter{
		config:  config,
		tokens:  make(chan struct{}, config.BurstSize),
		stopCh:  make(chan struct{}),
		logger:  logger,
	}

	return rl
}

// Start begins the token generation process.
// Must be called before using Wait().
func (r *RateLimiter) Start() {
	if r.running.Swap(true) {
		return // Already running
	}

	// Calculate interval between token releases
	// For 100 msg/sec, interval = 10ms
	interval := time.Second / time.Duration(r.config.MaxPerSecond)
	r.ticker = time.NewTicker(interval)

	r.logger.Info("rate limiter started",
		slog.Int("max_per_second", r.config.MaxPerSecond),
		slog.Int("burst_size", r.config.BurstSize),
		slog.Duration("token_interval", interval),
	)

	// Pre-fill the bucket with initial tokens (allows initial burst)
	for i := 0; i < r.config.BurstSize; i++ {
		select {
		case r.tokens <- struct{}{}:
		default:
			break
		}
	}

	r.wg.Add(1)
	go r.generateTokens()
}

// generateTokens runs in a goroutine and adds tokens at a fixed rate.
func (r *RateLimiter) generateTokens() {
	defer r.wg.Done()

	for {
		select {
		case <-r.stopCh:
			r.ticker.Stop()
			return
		case <-r.ticker.C:
			// Try to add a token (non-blocking)
			select {
			case r.tokens <- struct{}{}:
				// Token added
			default:
				// Bucket is full, discard token
			}
		}
	}
}

// Stop stops the token generation.
func (r *RateLimiter) Stop() {
	if !r.running.Swap(false) {
		return // Not running
	}

	close(r.stopCh)
	r.wg.Wait()

	r.logger.Info("rate limiter stopped",
		slog.Int64("total_acquired", r.acquired.Load()),
		slog.Int64("total_wait_ms", r.waitTimeMs.Load()),
	)
}

// Wait blocks until a token is available or context is cancelled.
// Implements the RateLimiterInterface.
func (r *RateLimiter) Wait(ctx context.Context) error {
	startTime := time.Now()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.tokens:
		// Token acquired
		r.acquired.Add(1)
		waitTime := time.Since(startTime)
		r.waitTimeMs.Add(waitTime.Milliseconds())
		return nil
	}
}

// TryAcquire attempts to acquire a token without blocking.
// Returns true if a token was acquired, false otherwise.
func (r *RateLimiter) TryAcquire() bool {
	select {
	case <-r.tokens:
		r.acquired.Add(1)
		return true
	default:
		return false
	}
}

// Available returns the number of tokens currently available.
func (r *RateLimiter) Available() int {
	return len(r.tokens)
}

// Stats returns rate limiter statistics.
func (r *RateLimiter) Stats() RateLimiterStats {
	return RateLimiterStats{
		MaxPerSecond:   r.config.MaxPerSecond,
		BurstSize:      r.config.BurstSize,
		Available:      len(r.tokens),
		TotalAcquired:  r.acquired.Load(),
		TotalWaitTimeMs: r.waitTimeMs.Load(),
	}
}

// RateLimiterStats holds rate limiter statistics.
type RateLimiterStats struct {
	MaxPerSecond    int
	BurstSize       int
	Available       int
	TotalAcquired   int64
	TotalWaitTimeMs int64
}

// =============================================================================
// Rate Limiter Manager (manages rate limiters per channel)
// =============================================================================

// RateLimiterManager manages rate limiters for multiple channels.
type RateLimiterManager struct {
	limiters map[string]*RateLimiter
	config   RateLimiterConfig
	logger   *slog.Logger
	mu       sync.RWMutex
}

// NewRateLimiterManager creates a new rate limiter manager.
func NewRateLimiterManager(config RateLimiterConfig, logger *slog.Logger) *RateLimiterManager {
	return &RateLimiterManager{
		limiters: make(map[string]*RateLimiter),
		config:   config,
		logger:   logger,
	}
}

// GetOrCreate returns the rate limiter for a channel, creating one if it doesn't exist.
func (m *RateLimiterManager) GetOrCreate(channel string) *RateLimiter {
	m.mu.RLock()
	if rl, ok := m.limiters[channel]; ok {
		m.mu.RUnlock()
		return rl
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if rl, ok := m.limiters[channel]; ok {
		return rl
	}

	rl := NewRateLimiter(m.config, m.logger.With(slog.String("channel", channel)))
	rl.Start()
	m.limiters[channel] = rl

	m.logger.Info("created rate limiter for channel",
		slog.String("channel", channel),
		slog.Int("max_per_second", m.config.MaxPerSecond),
	)

	return rl
}

// StartAll starts all rate limiters.
func (m *RateLimiterManager) StartAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, rl := range m.limiters {
		rl.Start()
	}
}

// StopAll stops all rate limiters.
func (m *RateLimiterManager) StopAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, rl := range m.limiters {
		rl.Stop()
	}
}

// Stats returns statistics for all rate limiters.
func (m *RateLimiterManager) Stats() map[string]RateLimiterStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]RateLimiterStats, len(m.limiters))
	for channel, rl := range m.limiters {
		stats[channel] = rl.Stats()
	}
	return stats
}
