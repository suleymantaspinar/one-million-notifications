package consumer

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// RateLimiter provides simple rate limiting using a sliding window counter.
type RateLimiter struct {
	maxRequests int           // maximum requests allowed per interval
	interval    time.Duration // time window
	requests    int           // current request count
	windowStart time.Time     // start of current window
	mu          sync.Mutex
	logger      *slog.Logger
}

// NewRateLimiter creates a new simple rate limiter.
// maxRequests: maximum number of requests allowed per interval
// interval: the time window for rate limiting
func NewRateLimiter(maxRequests int, interval time.Duration, logger *slog.Logger) *RateLimiter {
	return &RateLimiter{
		maxRequests: maxRequests,
		interval:    interval,
		requests:    0,
		windowStart: time.Now(),
		logger:      logger,
	}
}

// Allow checks if a request is allowed.
// Returns true if under the limit, false if rate limited.
func (r *RateLimiter) Allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()

	// Reset window if interval has passed
	if now.Sub(r.windowStart) >= r.interval {
		r.requests = 0
		r.windowStart = now
	}

	// Check if under limit
	if r.requests < r.maxRequests {
		r.requests++
		return true
	}

	return false
}

// Wait blocks until a request is allowed or context is cancelled.
func (r *RateLimiter) Wait(ctx context.Context) error {
	for {
		if r.Allow() {
			return nil
		}

		// Calculate time until window resets
		r.mu.Lock()
		timeUntilReset := r.interval - time.Since(r.windowStart)
		r.mu.Unlock()

		if timeUntilReset <= 0 {
			continue
		}

		// Wait a small amount before retrying
		waitTime := timeUntilReset / 10
		if waitTime < 10*time.Millisecond {
			waitTime = 10 * time.Millisecond
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
			// Try again
		}
	}
}

// Remaining returns the number of requests remaining in the current window.
func (r *RateLimiter) Remaining() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Reset window if interval has passed
	if time.Since(r.windowStart) >= r.interval {
		return r.maxRequests
	}

	remaining := r.maxRequests - r.requests
	if remaining < 0 {
		return 0
	}
	return remaining
}
