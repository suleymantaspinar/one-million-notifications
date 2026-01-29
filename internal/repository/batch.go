package repository

import (
	"context"
	"fmt"

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
	query := `INSERT INTO batches (batch_id, total_count, success_count, failure_count, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`

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
	query := `SELECT batch_id, total_count, success_count, failure_count, status, created_at
		FROM batches
		WHERE batch_id = $1`

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
