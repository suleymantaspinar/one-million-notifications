package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/notifications-management-api/internal/database"
	"github.com/notifications-management-api/internal/kafka"
	"github.com/notifications-management-api/internal/model"
	"github.com/notifications-management-api/internal/repository"
)

// NotificationService handles notification business logic.
type NotificationService struct {
	db               *database.PostgresDB
	notificationRepo *repository.NotificationRepository
	batchRepo        *repository.BatchRepository
	outboxRepo       *repository.OutboxRepository
	kafkaProducer    *kafka.Producer
	logger           *slog.Logger
}

// NewNotificationService creates a new NotificationService.
func NewNotificationService(
	db *database.PostgresDB,
	notificationRepo *repository.NotificationRepository,
	batchRepo *repository.BatchRepository,
	outboxRepo *repository.OutboxRepository,
	kafkaProducer *kafka.Producer,
	logger *slog.Logger,
) *NotificationService {
	return &NotificationService{
		db:               db,
		notificationRepo: notificationRepo,
		batchRepo:        batchRepo,
		outboxRepo:       outboxRepo,
		kafkaProducer:    kafkaProducer,
		logger:           logger,
	}
}

// Create creates a new notification.
func (s *NotificationService) Create(ctx context.Context, req model.CreateNotificationRequest) (*model.NotificationResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	if req.Priority == "" {
		req.Priority = model.PriorityNormal
	}

	now := time.Now()
	notification := &model.Notification{
		MessageID: uuid.New(),
		Recipient: req.To,
		Channel:   req.Channel,
		Content:   req.Content,
		Priority:  req.Priority,
		Status:    model.StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}

	outboxEvent := &model.OutboxEvent{
		ID:             uuid.New(),
		NotificationID: notification.MessageID,
		Recipient:      notification.Recipient,
		Channel:        notification.Channel,
		Content:        notification.Content,
		Priority:       notification.Priority,
		CreatedAt:      now,
	}

	s.logger.Info("starting transaction for notification creation",
		slog.String("message_id", notification.MessageID.String()),
	)

	tx, err := s.db.BeginTx(ctx)
	if err != nil {
		s.logger.Error("failed to begin transaction", slog.String("error", err.Error()))
		return nil, err
	}

	if err = s.notificationRepo.CreateWithTx(ctx, tx, notification); err != nil {
		tx.Rollback(ctx)
		s.logger.Warn("transaction rolled back: failed to create notification",
			slog.String("error", err.Error()),
			slog.String("message_id", notification.MessageID.String()),
		)
		return nil, err
	}

	if err = s.outboxRepo.CreateWithTx(ctx, tx, outboxEvent); err != nil {
		tx.Rollback(ctx)
		s.logger.Warn("transaction rolled back: failed to create outbox event",
			slog.String("error", err.Error()),
			slog.String("message_id", notification.MessageID.String()),
			slog.String("outbox_id", outboxEvent.ID.String()),
		)
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		s.logger.Error("failed to commit transaction",
			slog.String("error", err.Error()),
			slog.String("message_id", notification.MessageID.String()),
		)
		return nil, err
	}

	s.logger.Info("transaction committed successfully: notification created",
		slog.String("message_id", notification.MessageID.String()),
		slog.String("outbox_id", outboxEvent.ID.String()),
		slog.String("channel", string(notification.Channel)),
		slog.String("priority", string(notification.Priority)),
	)

	response := notification.ToResponse()
	return &response, nil
}

// GetByID retrieves a notification by its message ID.
func (s *NotificationService) GetByID(ctx context.Context, messageID uuid.UUID) (*model.NotificationResponse, error) {
	notification, err := s.notificationRepo.GetByID(ctx, messageID)
	if err != nil {
		return nil, err
	}
	response := notification.ToResponse()
	return &response, nil
}

// Cancel cancels a pending notification.
func (s *NotificationService) Cancel(ctx context.Context, messageID uuid.UUID) (*model.NotificationResponse, error) {
	notification, err := s.notificationRepo.GetByID(ctx, messageID)
	if err != nil {
		return nil, err
	}

	if notification.Status != model.StatusPending {
		return nil, model.ErrCannotCancel
	}

	if err := s.notificationRepo.UpdateStatus(ctx, messageID, model.StatusCancelled); err != nil {
		return nil, err
	}

	notification.Status = model.StatusCancelled
	notification.UpdatedAt = time.Now()

	s.logger.Info("notification cancelled", slog.String("message_id", messageID.String()))

	response := notification.ToResponse()
	return &response, nil
}

// List retrieves notifications with filtering and pagination.
func (s *NotificationService) List(ctx context.Context, filter model.NotificationFilter) (*model.NotificationListResponse, error) {
	if filter.Page < 1 {
		filter.Page = 1
	}
	if filter.PageSize < 1 {
		filter.PageSize = 20
	}
	if filter.PageSize > 100 {
		filter.PageSize = 100
	}

	notifications, totalCount, err := s.notificationRepo.List(ctx, filter)
	if err != nil {
		return nil, err
	}

	data := make([]model.NotificationResponse, len(notifications))
	for i, n := range notifications {
		data[i] = n.ToResponse()
	}

	totalPages := totalCount / filter.PageSize
	if totalCount%filter.PageSize > 0 {
		totalPages++
	}

	return &model.NotificationListResponse{
		Data: data,
		Pagination: model.Pagination{
			Page:       filter.Page,
			PageSize:   filter.PageSize,
			TotalPages: totalPages,
			TotalCount: totalCount,
		},
	}, nil
}

// CreateBatch creates multiple notifications in a batch.
func (s *NotificationService) CreateBatch(ctx context.Context, req model.CreateNotificationBatchRequest) (*model.BatchResponse, error) {
	if len(req.Notifications) == 0 {
		return nil, model.ErrEmptyBatch
	}
	if len(req.Notifications) > 1000 {
		return nil, model.ErrBatchTooLarge
	}

	for _, notif := range req.Notifications {
		if err := notif.Validate(); err != nil {
			return nil, err
		}
	}

	now := time.Now()
	batchID := uuid.New()

	batch := &model.Batch{
		BatchID:      batchID,
		TotalCount:   len(req.Notifications),
		SuccessCount: 0,
		FailureCount: 0,
		Status:       model.BatchStatusProcessing,
		CreatedAt:    now,
	}

	s.logger.Info("starting transaction for batch creation",
		slog.String("batch_id", batchID.String()),
		slog.Int("total_count", len(req.Notifications)),
	)

	tx, err := s.db.BeginTx(ctx)
	if err != nil {
		s.logger.Error("failed to begin transaction", slog.String("error", err.Error()))
		return nil, err
	}

	if err = s.batchRepo.CreateWithTx(ctx, tx, batch); err != nil {
		tx.Rollback(ctx)
		s.logger.Warn("transaction rolled back: failed to create batch",
			slog.String("error", err.Error()),
			slog.String("batch_id", batchID.String()),
		)
		return nil, err
	}

	notifications := make([]model.NotificationResponse, 0, len(req.Notifications))
	for _, notifReq := range req.Notifications {
		if notifReq.Priority == "" {
			notifReq.Priority = model.PriorityNormal
		}

		notification := &model.Notification{
			MessageID: uuid.New(),
			BatchID:   &batchID,
			Recipient: notifReq.To,
			Channel:   notifReq.Channel,
			Content:   notifReq.Content,
			Priority:  notifReq.Priority,
			Status:    model.StatusPending,
			CreatedAt: now,
			UpdatedAt: now,
		}

		if err = s.notificationRepo.CreateWithTx(ctx, tx, notification); err != nil {
			tx.Rollback(ctx)
			s.logger.Warn("transaction rolled back: failed to create notification in batch",
				slog.String("error", err.Error()),
				slog.String("batch_id", batchID.String()),
				slog.String("message_id", notification.MessageID.String()),
			)
			return nil, err
		}

		outboxEvent := &model.OutboxEvent{
			ID:             uuid.New(),
			NotificationID: notification.MessageID,
			Recipient:      notification.Recipient,
			Channel:        notification.Channel,
			Content:        notification.Content,
			Priority:       notification.Priority,
			CreatedAt:      now,
		}

		if err = s.outboxRepo.CreateWithTx(ctx, tx, outboxEvent); err != nil {
			tx.Rollback(ctx)
			s.logger.Warn("transaction rolled back: failed to create outbox event in batch",
				slog.String("error", err.Error()),
				slog.String("batch_id", batchID.String()),
				slog.String("message_id", notification.MessageID.String()),
			)
			return nil, err
		}

		notifications = append(notifications, notification.ToResponse())
	}

	if err = tx.Commit(ctx); err != nil {
		s.logger.Error("failed to commit transaction",
			slog.String("error", err.Error()),
			slog.String("batch_id", batchID.String()),
		)
		return nil, err
	}

	s.logger.Info("transaction committed successfully: batch created",
		slog.String("batch_id", batchID.String()),
		slog.Int("total_count", len(req.Notifications)),
	)

	return &model.BatchResponse{
		BatchID:       batchID,
		TotalCount:    batch.TotalCount,
		SuccessCount:  batch.SuccessCount,
		FailureCount:  batch.FailureCount,
		Status:        batch.Status,
		Notifications: notifications,
		CreatedAt:     batch.CreatedAt,
	}, nil
}

// GetBatchByID retrieves a batch by its ID.
func (s *NotificationService) GetBatchByID(ctx context.Context, batchID uuid.UUID) (*model.BatchResponse, error) {
	batch, err := s.batchRepo.GetByID(ctx, batchID)
	if err != nil {
		return nil, err
	}

	notifications, err := s.notificationRepo.GetByBatchID(ctx, batchID)
	if err != nil {
		return nil, err
	}

	notificationResponses := make([]model.NotificationResponse, len(notifications))
	for i, n := range notifications {
		notificationResponses[i] = n.ToResponse()
	}

	response := batch.ToBatchResponse(notificationResponses)
	return &response, nil
}
