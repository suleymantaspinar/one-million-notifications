package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/notifications-management-api/internal/kafka"
	"github.com/notifications-management-api/internal/model"
	"github.com/notifications-management-api/internal/repository"
)

// NotificationService handles notification business logic.
type NotificationService struct {
	notificationRepo *repository.NotificationRepository
	batchRepo        *repository.BatchRepository
	outboxRepo       *repository.OutboxRepository
	kafkaProducer    *kafka.Producer
	logger           *slog.Logger
}

// NewNotificationService creates a new NotificationService.
func NewNotificationService(
	notificationRepo *repository.NotificationRepository,
	batchRepo *repository.BatchRepository,
	outboxRepo *repository.OutboxRepository,
	kafkaProducer *kafka.Producer,
	logger *slog.Logger,
) *NotificationService {
	return &NotificationService{
		notificationRepo: notificationRepo,
		batchRepo:        batchRepo,
		outboxRepo:       outboxRepo,
		kafkaProducer:    kafkaProducer,
		logger:           logger,
	}
}

// Create creates a new notification.
func (s *NotificationService) Create(ctx context.Context, req model.CreateNotificationRequest) (*model.NotificationResponse, error) {
	// Validate request
	if err := req.Validate(); err != nil {
		return nil, err
	}

	// Set default priority if not provided
	if req.Priority == "" {
		req.Priority = model.PriorityNormal
	}

	now := time.Now()
	notification := &model.Notification{
		MessageID: uuid.New(),
		To:        req.To,
		Channel:   req.Channel,
		Content:   req.Content,
		Priority:  req.Priority,
		Status:    model.StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}

	// Create notification in database
	if err := s.notificationRepo.Create(ctx, notification); err != nil {
		s.logger.Error("failed to create notification",
			slog.String("error", err.Error()),
		)
		return nil, err
	}

	// Create outbox event for eventual publishing to Kafka
	topic := s.kafkaProducer.GetTopicForNotification(notification.Channel, notification.Priority)
	payload, _ := json.Marshal(notification)

	outboxEvent := &model.OutboxEvent{
		ID:          uuid.New(),
		AggregateID: notification.MessageID,
		EventType:   "notification.created",
		Topic:       topic,
		Payload:     payload,
		CreatedAt:   now,
	}

	if err := s.outboxRepo.Create(ctx, outboxEvent); err != nil {
		s.logger.Error("failed to create outbox event",
			slog.String("error", err.Error()),
			slog.String("notification_id", notification.MessageID.String()),
		)
		// Note: In a real implementation, this should be in a transaction
	}

	s.logger.Info("notification created",
		slog.String("message_id", notification.MessageID.String()),
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
	// Get current notification
	notification, err := s.notificationRepo.GetByID(ctx, messageID)
	if err != nil {
		return nil, err
	}

	// Only pending notifications can be cancelled
	if notification.Status != model.StatusPending {
		return nil, model.ErrCannotCancel
	}

	// Update status to cancelled
	if err := s.notificationRepo.UpdateStatus(ctx, messageID, model.StatusCancelled); err != nil {
		return nil, err
	}

	notification.Status = model.StatusCancelled
	notification.UpdatedAt = time.Now()

	s.logger.Info("notification cancelled",
		slog.String("message_id", messageID.String()),
	)

	response := notification.ToResponse()
	return &response, nil
}

// List retrieves notifications with filtering and pagination.
func (s *NotificationService) List(ctx context.Context, filter model.NotificationFilter) (*model.NotificationListResponse, error) {
	// Set defaults
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

	// Convert to response format
	data := make([]model.NotificationResponse, len(notifications))
	for i, n := range notifications {
		data[i] = n.ToResponse()
	}

	// Calculate pagination
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
	// Validate batch size
	if len(req.Notifications) == 0 {
		return nil, model.ErrEmptyBatch
	}
	if len(req.Notifications) > 1000 {
		return nil, model.ErrBatchTooLarge
	}

	// Validate all notifications first
	for _, notif := range req.Notifications {
		if err := notif.Validate(); err != nil {
			return nil, err
		}
	}

	now := time.Now()
	batchID := uuid.New()

	// Create batch record
	batch := &model.Batch{
		BatchID:      batchID,
		TotalCount:   len(req.Notifications),
		SuccessCount: 0,
		FailureCount: 0,
		Status:       model.BatchStatusProcessing,
		CreatedAt:    now,
	}

	if err := s.batchRepo.Create(ctx, batch); err != nil {
		return nil, err
	}

	// Create all notifications
	notifications := make([]model.NotificationResponse, 0, len(req.Notifications))
	for _, notifReq := range req.Notifications {
		// Set default priority if not provided
		if notifReq.Priority == "" {
			notifReq.Priority = model.PriorityNormal
		}

		notification := &model.Notification{
			MessageID: uuid.New(),
			BatchID:   &batchID,
			To:        notifReq.To,
			Channel:   notifReq.Channel,
			Content:   notifReq.Content,
			Priority:  notifReq.Priority,
			Status:    model.StatusPending,
			CreatedAt: now,
			UpdatedAt: now,
		}

		if err := s.notificationRepo.Create(ctx, notification); err != nil {
			s.logger.Error("failed to create notification in batch",
				slog.String("error", err.Error()),
				slog.String("batch_id", batchID.String()),
			)
			continue
		}

		// Create outbox event
		topic := s.kafkaProducer.GetTopicForNotification(notification.Channel, notification.Priority)
		payload, _ := json.Marshal(notification)

		outboxEvent := &model.OutboxEvent{
			ID:          uuid.New(),
			AggregateID: notification.MessageID,
			EventType:   "notification.created",
			Topic:       topic,
			Payload:     payload,
			CreatedAt:   now,
		}

		if err := s.outboxRepo.Create(ctx, outboxEvent); err != nil {
			s.logger.Error("failed to create outbox event",
				slog.String("error", err.Error()),
				slog.String("notification_id", notification.MessageID.String()),
			)
		}

		notifications = append(notifications, notification.ToResponse())
	}

	s.logger.Info("batch created",
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

	// Get all notifications in the batch
	notifications, err := s.notificationRepo.GetByBatchID(ctx, batchID)
	if err != nil {
		return nil, err
	}

	// Convert to response format
	notificationResponses := make([]model.NotificationResponse, len(notifications))
	for i, n := range notifications {
		notificationResponses[i] = n.ToResponse()
	}

	response := batch.ToBatchResponse(notificationResponses)
	return &response, nil
}
