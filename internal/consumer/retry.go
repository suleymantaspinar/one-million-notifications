package consumer

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"time"
)

// RetryConfig holds retry configuration.
type RetryConfig struct {
	MaxRetries     int           // maximum number of retry attempts
	InitialBackoff time.Duration // initial backoff duration
	MaxBackoff     time.Duration // maximum backoff duration
	Multiplier     float64       // backoff multiplier
}

// Retryer handles retry logic with exponential backoff.
type Retryer struct {
	config RetryConfig
	logger *slog.Logger
}

// NewRetryer creates a new Retryer.
func NewRetryer(config RetryConfig, logger *slog.Logger) *Retryer {
	return &Retryer{
		config: config,
		logger: logger,
	}
}

// RetryResult contains the result of a retry operation.
type RetryResult struct {
	Attempts int           // number of attempts made
	LastErr  error         // last error encountered
	Duration time.Duration // total duration of all attempts
}

// RetryableFunc is a function that can be retried.
type RetryableFunc func(ctx context.Context) error

// Do executes a function with retry logic.
func (r *Retryer) Do(ctx context.Context, operation string, fn RetryableFunc) RetryResult {
	result := RetryResult{}
	startTime := time.Now()

	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		result.Attempts = attempt + 1

		// Execute the function
		err := fn(ctx)
		if err == nil {
			result.Duration = time.Since(startTime)
			return result
		}

		result.LastErr = err

		// Check if we should retry
		if !r.shouldRetry(err) {
			r.logger.Warn("non-retryable error, not retrying",
				slog.String("operation", operation),
				slog.Int("attempt", attempt+1),
				slog.String("error", err.Error()),
			)
			result.Duration = time.Since(startTime)
			return result
		}

		// Check if we've exhausted retries
		if attempt >= r.config.MaxRetries {
			r.logger.Error("max retries exhausted",
				slog.String("operation", operation),
				slog.Int("attempts", attempt+1),
				slog.String("error", err.Error()),
			)
			result.Duration = time.Since(startTime)
			return result
		}

		// Calculate backoff
		backoff := r.calculateBackoff(attempt)

		r.logger.Warn("operation failed, retrying",
			slog.String("operation", operation),
			slog.Int("attempt", attempt+1),
			slog.Int("max_retries", r.config.MaxRetries),
			slog.Duration("backoff", backoff),
			slog.String("error", err.Error()),
		)

		// Wait before retry
		select {
		case <-ctx.Done():
			result.LastErr = ctx.Err()
			result.Duration = time.Since(startTime)
			return result
		case <-time.After(backoff):
			// Continue to next attempt
		}
	}

	result.Duration = time.Since(startTime)
	return result
}

// shouldRetry determines if an error should trigger a retry.
func (r *Retryer) shouldRetry(err error) bool {
	if err == nil {
		return false
	}

	// Context errors are not retryable
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Check if it's a provider error
	var providerErr *ProviderError
	if errors.As(err, &providerErr) {
		return providerErr.IsRetryable()
	}

	// For other errors, retry by default (network issues, etc.)
	return true
}

// calculateBackoff calculates the backoff duration for an attempt.
func (r *Retryer) calculateBackoff(attempt int) time.Duration {
	backoff := float64(r.config.InitialBackoff) * math.Pow(r.config.Multiplier, float64(attempt))

	// Cap at max backoff
	if backoff > float64(r.config.MaxBackoff) {
		backoff = float64(r.config.MaxBackoff)
	}

	return time.Duration(backoff)
}

// Success returns true if the operation succeeded.
func (r *RetryResult) Success() bool {
	return r.LastErr == nil
}
