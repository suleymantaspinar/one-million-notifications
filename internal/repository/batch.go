package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/notifications-management-api/internal/database"
	"github.com/notifications-management-api/internal/model"
)

// BatchRepository handles batch persistence.
type BatchRepository struct {
	pool *pgxpool.Pool
}

// NewBatchRepository creates a new BatchRepository.
func NewBatchRepository(pool *pgxpool.Pool) *BatchRepository {
	return &BatchRepository{pool: pool}
}

// Create creates a new batch.
func (r *BatchRepository) Create(ctx context.Context, batch *model.Batch) error {
	return r.CreateWithTx(ctx, r.pool, batch)
}

// CreateWithTx creates a new batch within a transaction.
func (r *BatchRepository) CreateWithTx(ctx context.Context, tx database.DBTX, batch *model.Batch) error {
	query := `
		INSERT INTO batches (batch_id, total_count, success_count, failure_count, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err := tx.Exec(ctx, query,
		batch.BatchID,
		batch.TotalCount,
		batch.SuccessCount,
		batch.FailureCount,
		batch.Status,
		batch.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create batch: %w", err)
	}
	return nil
}

// GetByID retrieves a batch by its ID.
func (r *BatchRepository) GetByID(ctx context.Context, batchID uuid.UUID) (*model.Batch, error) {
	query := `
		SELECT batch_id, total_count, success_count, failure_count, status, created_at
		FROM batches
		WHERE batch_id = $1
	`
	row := r.pool.QueryRow(ctx, query, batchID)

	var b model.Batch
	err := row.Scan(
		&b.BatchID,
		&b.TotalCount,
		&b.SuccessCount,
		&b.FailureCount,
		&b.Status,
		&b.CreatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, model.ErrBatchNotFound
		}
		return nil, fmt.Errorf("failed to get batch: %w", err)
	}
	return &b, nil
}

// UpdateCounts updates the success and failure counts of a batch.
func (r *BatchRepository) UpdateCounts(ctx context.Context, batchID uuid.UUID, successCount, failureCount int) error {
	query := `
		UPDATE batches
		SET success_count = $2, failure_count = $3
		WHERE batch_id = $1
	`
	result, err := r.pool.Exec(ctx, query, batchID, successCount, failureCount)
	if err != nil {
		return fmt.Errorf("failed to update batch counts: %w", err)
	}
	if result.RowsAffected() == 0 {
		return model.ErrBatchNotFound
	}
	return nil
}

// UpdateStatus updates the status of a batch.
func (r *BatchRepository) UpdateStatus(ctx context.Context, batchID uuid.UUID, status model.BatchStatus) error {
	query := `
		UPDATE batches
		SET status = $2
		WHERE batch_id = $1
	`
	result, err := r.pool.Exec(ctx, query, batchID, status)
	if err != nil {
		return fmt.Errorf("failed to update batch status: %w", err)
	}
	if result.RowsAffected() == 0 {
		return model.ErrBatchNotFound
	}
	return nil
}

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
		INSERT INTO outbox_events (id, notification_id, recipient, channel, content, priority, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err := tx.Exec(ctx, query,
		event.ID,
		event.NotificationID,
		event.Recipient,
		event.Channel,
		event.Content,
		event.Priority,
		event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create outbox event: %w", err)
	}
	return nil
}

// GetUnprocessed retrieves unprocessed outbox events.
func (r *OutboxRepository) GetUnprocessed(ctx context.Context, limit int) ([]model.OutboxEvent, error) {
	query := `
		SELECT id, notification_id, recipient, channel, content, priority, created_at, processed_at
		FROM outbox_events
		WHERE processed_at IS NULL
		ORDER BY created_at ASC
		LIMIT $1
	`
	rows, err := r.pool.Query(ctx, query, limit)
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
			&e.CreatedAt,
			&e.ProcessedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan outbox event: %w", err)
		}
		events = append(events, e)
	}

	return events, nil
}

// MarkProcessed marks an outbox event as processed.
func (r *OutboxRepository) MarkProcessed(ctx context.Context, id uuid.UUID) error {
	query := `
		UPDATE outbox_events
		SET processed_at = $2
		WHERE id = $1
	`
	_, err := r.pool.Exec(ctx, query, id, time.Now())
	return err
}
