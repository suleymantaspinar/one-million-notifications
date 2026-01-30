package repository

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// IdempotencyRepository provides database-based idempotency checking.
type IdempotencyRepository struct {
	pool   *pgxpool.Pool
	ttl    time.Duration
	logger *slog.Logger
}

// NewIdempotencyRepository creates a new idempotency repository.
func NewIdempotencyRepository(pool *pgxpool.Pool, ttl time.Duration, logger *slog.Logger) *IdempotencyRepository {
	return &IdempotencyRepository{
		pool:   pool,
		ttl:    ttl,
		logger: logger,
	}
}

// IsProcessed checks if a notification has already been processed.
// It also marks the notification as processed if it hasn't been (atomic operation).
// Returns true if already processed, false if this is the first time.
func (r *IdempotencyRepository) IsProcessed(ctx context.Context, messageID string) (bool, error) {
	notificationID, err := uuid.Parse(messageID)
	if err != nil {
		r.logger.Error("failed to parse notification ID",
			slog.String("message_id", messageID),
			slog.String("error", err.Error()),
		)
		return false, err
	}

	// Use INSERT ... ON CONFLICT to atomically check and mark as processed
	// If the record exists and hasn't expired, we return true (already processed)
	// If the record doesn't exist or has expired, we insert/update and return false
	query := `INSERT INTO processed_notifications (notification_id, processed_at, expires_at)
		VALUES ($1, NOW(), NOW() + $2::interval)
		ON CONFLICT (notification_id) DO UPDATE 
		SET processed_at = CASE 
			WHEN processed_notifications.expires_at < NOW() THEN NOW()
			ELSE processed_notifications.processed_at
		END,
		expires_at = CASE
			WHEN processed_notifications.expires_at < NOW() THEN NOW() + $2::interval
			ELSE processed_notifications.expires_at
		END
		RETURNING (xmax = 0) AS is_new, expires_at > NOW() AS is_valid`

	var isNew, isValid bool
	err = r.pool.QueryRow(ctx, query, notificationID, r.ttl.String()).Scan(&isNew, &isValid)
	if err != nil {
		r.logger.Error("failed to check/mark idempotency",
			slog.String("notification_id", messageID),
			slog.String("error", err.Error()),
		)
		return false, err
	}

	// If it's a new record, it wasn't processed before
	if isNew {
		return false, nil
	}

	// If the record existed and is still valid, it was already processed
	return isValid, nil
}
