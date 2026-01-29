package consumer

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// RateLimiterConfig holds configuration for the rate limiter.
type RateLimiterConfig struct {
	// MaxPerSecond is the maximum number of messages allowed per second.
	MaxPerSecond int
	// BurstSize allows temporary bursts above the rate limit.
	// Set to MaxPerSecond for no bursting.
	BurstSize int
}

// RateLimiter implements a token bucket rate limiter.
// It releases tokens at a fixed rate, and workers must acquire a token before processing.
type RateLimiter struct {
	config  RateLimiterConfig
	tokens  chan struct{} // Token bucket channel
	ticker  *time.Ticker  // Releases tokens at fixed rate
	stopCh  chan struct{} // Signal to stop token generation
	logger  *slog.Logger
	wg      sync.WaitGroup
	running atomic.Bool

	// Metrics
	acquired   atomic.Int64 // Total tokens acquired
	waitTimeMs atomic.Int64 // Total wait time in milliseconds
}

// NewRateLimiter creates a new rate limiter with the given configuration.
func NewRateLimiter(config RateLimiterConfig, logger *slog.Logger) *RateLimiter {
	if config.MaxPerSecond <= 0 {
		config.MaxPerSecond = 100
	}
	if config.BurstSize <= 0 {
		config.BurstSize = config.MaxPerSecond
	}

	return &RateLimiter{
		config: config,
		tokens: make(chan struct{}, config.BurstSize),
		stopCh: make(chan struct{}),
		logger: logger,
	}
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
