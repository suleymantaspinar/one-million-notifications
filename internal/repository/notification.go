package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/notifications-management-api/internal/database"
	"github.com/notifications-management-api/internal/model"
)

// NotificationRepository handles notification persistence.
type NotificationRepository struct {
	pool *pgxpool.Pool
}

// NewNotificationRepository creates a new NotificationRepository.
func NewNotificationRepository(pool *pgxpool.Pool) *NotificationRepository {
	return &NotificationRepository{pool: pool}
}

// Create creates a new notification.
func (r *NotificationRepository) Create(ctx context.Context, notification *model.Notification) error {
	return r.CreateWithTx(ctx, r.pool, notification)
}

// CreateWithTx creates a new notification within a transaction.
func (r *NotificationRepository) CreateWithTx(ctx context.Context, tx database.DBTX, notification *model.Notification) error {
	query := `
		INSERT INTO notifications (message_id, batch_id, recipient, channel, content, priority, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`
	_, err := tx.Exec(ctx, query,
		notification.MessageID,
		notification.BatchID,
		notification.Recipient,
		notification.Channel,
		notification.Content,
		notification.Priority,
		notification.Status,
		notification.CreatedAt,
		notification.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create notification: %w", err)
	}
	return nil
}

// GetByID retrieves a notification by its message ID.
func (r *NotificationRepository) GetByID(ctx context.Context, messageID uuid.UUID) (*model.Notification, error) {
	query := `
		SELECT message_id, batch_id, recipient, channel, content, priority, status, created_at, updated_at
		FROM notifications
		WHERE message_id = $1
	`
	row := r.pool.QueryRow(ctx, query, messageID)

	var n model.Notification
	err := row.Scan(
		&n.MessageID,
		&n.BatchID,
		&n.Recipient,
		&n.Channel,
		&n.Content,
		&n.Priority,
		&n.Status,
		&n.CreatedAt,
		&n.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, model.ErrNotificationNotFound
		}
		return nil, fmt.Errorf("failed to get notification: %w", err)
	}
	return &n, nil
}

// UpdateStatus updates the status of a notification.
func (r *NotificationRepository) UpdateStatus(ctx context.Context, messageID uuid.UUID, status model.NotificationStatus) error {
	query := `
		UPDATE notifications
		SET status = $2, updated_at = $3
		WHERE message_id = $1
	`
	result, err := r.pool.Exec(ctx, query, messageID, status, time.Now())
	if err != nil {
		return fmt.Errorf("failed to update notification status: %w", err)
	}
	if result.RowsAffected() == 0 {
		return model.ErrNotificationNotFound
	}
	return nil
}

// List retrieves notifications with filtering and pagination.
func (r *NotificationRepository) List(ctx context.Context, filter model.NotificationFilter) ([]model.Notification, int, error) {
	conditions := []string{}
	args := []interface{}{}
	argIndex := 1

	if filter.Status != nil {
		conditions = append(conditions, fmt.Sprintf("status = $%d", argIndex))
		args = append(args, *filter.Status)
		argIndex++
	}
	if filter.Channel != nil {
		conditions = append(conditions, fmt.Sprintf("channel = $%d", argIndex))
		args = append(args, *filter.Channel)
		argIndex++
	}
	if filter.StartDate != nil {
		conditions = append(conditions, fmt.Sprintf("created_at >= $%d", argIndex))
		args = append(args, *filter.StartDate)
		argIndex++
	}
	if filter.EndDate != nil {
		conditions = append(conditions, fmt.Sprintf("created_at <= $%d", argIndex))
		args = append(args, *filter.EndDate)
		argIndex++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM notifications %s", whereClause)
	var totalCount int
	err := r.pool.QueryRow(ctx, countQuery, args...).Scan(&totalCount)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count notifications: %w", err)
	}

	offset := (filter.Page - 1) * filter.PageSize
	args = append(args, filter.PageSize, offset)

	query := fmt.Sprintf(`
		SELECT message_id, batch_id, recipient, channel, content, priority, status, created_at, updated_at
		FROM notifications
		%s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d
	`, whereClause, argIndex, argIndex+1)

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list notifications: %w", err)
	}
	defer rows.Close()

	notifications := []model.Notification{}
	for rows.Next() {
		var n model.Notification
		err := rows.Scan(
			&n.MessageID,
			&n.BatchID,
			&n.Recipient,
			&n.Channel,
			&n.Content,
			&n.Priority,
			&n.Status,
			&n.CreatedAt,
			&n.UpdatedAt,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan notification: %w", err)
		}
		notifications = append(notifications, n)
	}

	return notifications, totalCount, nil
}

// GetByBatchID retrieves all notifications for a batch.
func (r *NotificationRepository) GetByBatchID(ctx context.Context, batchID uuid.UUID) ([]model.Notification, error) {
	query := `
		SELECT message_id, batch_id, recipient, channel, content, priority, status, created_at, updated_at
		FROM notifications
		WHERE batch_id = $1
		ORDER BY created_at ASC
	`
	rows, err := r.pool.Query(ctx, query, batchID)
	if err != nil {
		return nil, fmt.Errorf("failed to get notifications by batch: %w", err)
	}
	defer rows.Close()

	notifications := []model.Notification{}
	for rows.Next() {
		var n model.Notification
		err := rows.Scan(
			&n.MessageID,
			&n.BatchID,
			&n.Recipient,
			&n.Channel,
			&n.Content,
			&n.Priority,
			&n.Status,
			&n.CreatedAt,
			&n.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan notification: %w", err)
		}
		notifications = append(notifications, n)
	}

	return notifications, nil
}
