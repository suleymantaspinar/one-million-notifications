package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/notifications-management-api/internal/database"
	"github.com/notifications-management-api/internal/model"
)

// OutboxRepository handles outbox event persistence.
type OutboxRepository struct {
	pool *pgxpool.Pool
}

// NewOutboxRepository creates a new OutboxRepository.
func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

// Create creates a new outbox event.
func (r *OutboxRepository) Create(ctx context.Context, event *model.OutboxEvent) error {
	return r.CreateWithTx(ctx, r.pool, event)
}

// CreateWithTx creates a new outbox event within a transaction.
func (r *OutboxRepository) CreateWithTx(ctx context.Context, tx database.DBTX, event *model.OutboxEvent) error {
	query := `
		INSERT INTO outbox_events (id, notification_id, recipient, channel, content, priority, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := tx.Exec(ctx, query,
		event.ID,
		event.NotificationID,
		event.Recipient,
		event.Channel,
		event.Content,
		event.Priority,
		event.Status,
		event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create outbox event: %w", err)
	}
	return nil
}

// GetUnprocessed retrieves unprocessed events with retry count lower than maxRetries.
func (r *OutboxRepository) GetUnprocessed(ctx context.Context, limit, maxRetries int) ([]model.OutboxEvent, error) {
	query := `
		SELECT id, notification_id, recipient, channel, content, priority, status, retry_count, created_at, processed_at
		FROM outbox_events
		WHERE status = $1 AND retry_count < $2
		ORDER BY created_at ASC
		LIMIT $3
	`
	rows, err := r.pool.Query(ctx, query, model.OutboxStatusReady, maxRetries, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get unprocessed outbox events: %w", err)
	}
	defer rows.Close()

	events := []model.OutboxEvent{}
	for rows.Next() {
		var e model.OutboxEvent
		err := rows.Scan(
			&e.ID,
			&e.NotificationID,
			&e.Recipient,
			&e.Channel,
			&e.Content,
			&e.Priority,
			&e.Status,
			&e.RetryCount,
			&e.CreatedAt,
			&e.ProcessedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating outbox events: %w", err)
	}

	return events, nil
}

// IncrementRetryCount increments the retry count for an outbox event.
func (r *OutboxRepository) IncrementRetryCount(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE outbox_events
		SET retry_count = retry_count + 1
		WHERE id = $1
	`
	result, err := r.pool.Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to increment retry count: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("outbox event not found: %s", id)
	}
	return nil
}

// MarkProcessed marks an outbox event as processed (status = 'sent').
func (r *OutboxRepository) MarkProcessed(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE outbox_events
		SET status = $2, processed_at = $3
		WHERE id = $1
	`
	result, err := r.pool.Exec(ctx, query, id, model.OutboxStatusSent, time.Now())
	if err != nil {
		return fmt.Errorf("failed to mark outbox event as processed: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("outbox event not found: %s", id)
	}
	return nil
}

// MarkFailed marks an outbox event as failed (status = 'failed').
func (r *OutboxRepository) MarkFailed(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE outbox_events
		SET status = $2, processed_at = $3
		WHERE id = $1
	`
	result, err := r.pool.Exec(ctx, query, id, model.OutboxStatusFailed, time.Now())
	if err != nil {
		return fmt.Errorf("failed to mark outbox event as failed: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("outbox event not found: %s", id)
	}
	return nil
}
