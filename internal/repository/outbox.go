package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/notifications-management-api/internal/database"
	"github.com/notifications-management-api/internal/model"
)

// ErrOutboxEventNotFound is returned when an outbox event is not found.
var ErrOutboxEventNotFound = errors.New("outbox event not found")

// TimeProvider allows injecting time for testing.
type TimeProvider func() time.Time

// OutboxRepository handles outbox event persistence.
type OutboxRepository struct {
	pool    *pgxpool.Pool
	nowFunc TimeProvider
}

// NewOutboxRepository creates a new OutboxRepository.
func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{
		pool:    pool,
		nowFunc: time.Now,
	}
}

// NewOutboxRepositoryWithTime creates a new OutboxRepository with custom time provider (for testing).
func NewOutboxRepositoryWithTime(pool *pgxpool.Pool, nowFunc TimeProvider) *OutboxRepository {
	return &OutboxRepository{
		pool:    pool,
		nowFunc: nowFunc,
	}
}

// Create creates a new outbox event.
func (r *OutboxRepository) Create(ctx context.Context, event *model.OutboxEvent) error {
	return r.CreateWithTx(ctx, r.pool, event)
}

// CreateWithTx creates a new outbox event within a transaction.
func (r *OutboxRepository) CreateWithTx(ctx context.Context, tx database.DBTX, event *model.OutboxEvent) error {
	query := `INSERT INTO outbox_events (id, notification_id, recipient, channel, content, priority, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

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

// GetUnprocessed retrieves unprocessed events (status = 'ready').
func (r *OutboxRepository) GetUnprocessed(ctx context.Context, limit int) ([]model.OutboxEvent, error) {
	query := `SELECT id, notification_id, recipient, channel, content, priority, status, created_at, processed_at
		FROM outbox_events
		WHERE status = $1
		ORDER BY created_at ASC
		LIMIT $2`

	rows, err := r.pool.Query(ctx, query, model.OutboxStatusReady, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get unprocessed outbox events: %w", err)
	}
	defer rows.Close()

	events := make([]model.OutboxEvent, 0, limit)
	for rows.Next() {
		var e model.OutboxEvent
		if err := rows.Scan(
			&e.ID,
			&e.NotificationID,
			&e.Recipient,
			&e.Channel,
			&e.Content,
			&e.Priority,
			&e.Status,
			&e.CreatedAt,
			&e.ProcessedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating outbox events: %w", err)
	}

	return events, nil
}

// MarkProcessed marks an outbox event as processed (status = 'sent').
func (r *OutboxRepository) MarkProcessed(ctx context.Context, id uuid.UUID) error {
	return r.updateStatus(ctx, id, model.OutboxStatusSent, "mark as processed")
}

// MarkFailed marks an outbox event as failed (status = 'failed').
func (r *OutboxRepository) MarkFailed(ctx context.Context, id uuid.UUID) error {
	return r.updateStatus(ctx, id, model.OutboxStatusFailed, "mark as failed")
}

// updateStatus updates the status of an outbox event.
func (r *OutboxRepository) updateStatus(ctx context.Context, id uuid.UUID, status model.OutboxEventStatus, action string) error {
	query := `UPDATE outbox_events
		SET status = $2, processed_at = $3
		WHERE id = $1`

	result, err := r.pool.Exec(ctx, query, id, status, r.nowFunc())
	if err != nil {
		return fmt.Errorf("failed to %s outbox event: %w", action, err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrOutboxEventNotFound, id)
	}
	return nil
}
