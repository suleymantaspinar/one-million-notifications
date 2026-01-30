package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/notifications-management-api/internal/model"
)

// cleanupTimeout is the timeout for cleanup operations (DB updates) that must complete
// even if the request context is cancelled.
const cleanupTimeout = 10 * time.Second

// =============================================================================
// Interfaces for Dependency Injection
// =============================================================================

// NotificationHandler defines the interface for handling notifications.
type NotificationHandler interface {
	// Process handles the full processing pipeline.
	Process(ctx context.Context, msg *NotificationMessage) error
	// Channel returns the notification channel this handler processes.
	Channel() model.NotificationChannel
}

// NotificationSender is the Strategy interface for sending notifications.
// Each notification type (SMS, Email, Push) implements this interface.
type NotificationSender interface {
	// Send sends the notification to the external provider.
	Send(ctx context.Context, msg *NotificationMessage) (*ProviderResponse, error)
	// Channel returns the notification channel.
	Channel() model.NotificationChannel
	// Name returns the sender name for logging.
	Name() string
}

// IdempotencyChecker checks if a message has already been processed.
type IdempotencyChecker interface {
	IsProcessed(ctx context.Context, messageID string) (bool, error)
}

// RateLimiterInterface provides rate limiting functionality.
type RateLimiterInterface interface {
	Wait(ctx context.Context) error
}

// RetryerInterface provides retry functionality.
type RetryerInterface interface {
	Do(ctx context.Context, operation string, fn RetryableFunc) RetryResult
}

// NotificationRepository updates notification status in the database.
type NotificationRepository interface {
	UpdateStatus(ctx context.Context, messageID uuid.UUID, status model.NotificationStatus) error
}

// DLQProducer interface for publishing to dead letter queue.
type DLQProducer interface {
	PublishToDLQ(ctx context.Context, msg *NotificationMessage, err error, attempts int) error
}

// =============================================================================
// Data Transfer Objects
// =============================================================================

// NotificationMessage represents a message consumed from Kafka.
type NotificationMessage struct {
	NotificationID string `json:"notificationId"`
	Recipient      string `json:"recipient"`
	Channel        string `json:"channel"`
	Content        string `json:"content"`
	Priority       string `json:"priority"`
	Timestamp      string `json:"timestamp"`
}

// ProviderResponse represents the response from the external notification provider.
type ProviderResponse struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// ProviderError represents an error from the notification provider.
type ProviderError struct {
	StatusCode int
	Body       string
}

func (e *ProviderError) Error() string {
	return fmt.Sprintf("provider returned status %d: %s", e.StatusCode, e.Body)
}

// IsRetryable returns true if the error is retryable (5xx errors).
func (e *ProviderError) IsRetryable() bool {
	return e.StatusCode >= 500 && e.StatusCode < 600
}

// ParseMessage parses a Kafka message value into a NotificationMessage.
func ParseMessage(value []byte) (*NotificationMessage, error) {
	var msg NotificationMessage
	if err := json.Unmarshal(value, &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	if msg.NotificationID == "" {
		return nil, errors.New("notification ID is required")
	}

	return &msg, nil
}

// =============================================================================
// Template Pattern: BaseNotificationHandler
// =============================================================================

// BaseNotificationHandler implements the Template Method pattern.
// It defines the skeleton of the notification processing algorithm,
// delegating the actual sending to the injected NotificationSender strategy.
type BaseNotificationHandler struct {
	sender           NotificationSender     // Strategy for sending
	idempotency      IdempotencyChecker     // Idempotency checking
	rateLimiter      RateLimiterInterface   // Rate limiting
	retryer          RetryerInterface       // Retry logic
	notificationRepo NotificationRepository // DB operations
	dlqProducer      DLQProducer            // Dead letter queue
	logger           *slog.Logger           // Logging
}

// HandlerConfig holds configuration for creating a BaseNotificationHandler.
type HandlerConfig struct {
	Sender           NotificationSender
	Idempotency      IdempotencyChecker
	RateLimiter      RateLimiterInterface
	Retryer          RetryerInterface
	NotificationRepo NotificationRepository
	DLQProducer      DLQProducer
	Logger           *slog.Logger
}

// NewBaseNotificationHandler creates a new BaseNotificationHandler with injected dependencies.
func NewBaseNotificationHandler(cfg HandlerConfig) *BaseNotificationHandler {
	return &BaseNotificationHandler{
		sender:           cfg.Sender,
		idempotency:      cfg.Idempotency,
		rateLimiter:      cfg.RateLimiter,
		retryer:          cfg.Retryer,
		notificationRepo: cfg.NotificationRepo,
		dlqProducer:      cfg.DLQProducer,
		logger:           cfg.Logger,
	}
}

// Channel returns the notification channel from the injected sender strategy.
func (h *BaseNotificationHandler) Channel() model.NotificationChannel {
	return h.sender.Channel()
}

// Process is the Template Method that defines the notification processing algorithm.
// Steps:
// 1. Check idempotency
// 2. Apply rate limiting
// 3. Send with retry (delegates to strategy)
// 4. Handle success/failure
func (h *BaseNotificationHandler) Process(ctx context.Context, msg *NotificationMessage) error {
	startTime := time.Now()
	senderName := h.sender.Name()

	// Step 1: Check idempotency - skip if already processed
	if skip := h.checkIdempotency(ctx, msg, senderName); skip {
		return nil
	}

	// Step 2: Apply rate limiting
	if err := h.applyRateLimiting(ctx); err != nil {
		return err
	}

	// Step 3: Send with retry (delegates to the strategy)
	result := h.sendWithRetry(ctx, msg, senderName)

	duration := time.Since(startTime)

	// Step 4: Handle result
	if result.Success() {
		h.onSuccess(msg, senderName, result.Attempts, duration)
		return nil
	}

	h.onFailure(msg, senderName, result, duration)
	return result.LastErr
}

// checkIdempotency checks if the message was already processed.
// Returns true if processing should be skipped.
func (h *BaseNotificationHandler) checkIdempotency(ctx context.Context, msg *NotificationMessage, senderName string) bool {
	processed, err := h.idempotency.IsProcessed(ctx, msg.NotificationID)
	if err != nil {
		h.logger.Warn("idempotency check failed, proceeding",
			slog.String("sender", senderName),
			slog.String("notification_id", msg.NotificationID),
			slog.String("error", err.Error()),
		)
		return false
	}

	if processed {
		h.logger.Info("notification already processed, skipping",
			slog.String("sender", senderName),
			slog.String("notification_id", msg.NotificationID),
		)
		return true
	}

	return false
}

// applyRateLimiting waits for rate limiter approval.
func (h *BaseNotificationHandler) applyRateLimiting(ctx context.Context) error {
	if err := h.rateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiting wait failed: %w", err)
	}
	return nil
}

// sendWithRetry sends the notification using the strategy with retry logic.
func (h *BaseNotificationHandler) sendWithRetry(ctx context.Context, msg *NotificationMessage, senderName string) RetryResult {
	return h.retryer.Do(ctx, "send_"+senderName, func(ctx context.Context) error {
		_, err := h.sender.Send(ctx, msg)
		return err
	})
}

// onSuccess handles successful notification delivery.
func (h *BaseNotificationHandler) onSuccess(msg *NotificationMessage, senderName string, attempts int, duration time.Duration) {
	// Use background context for cleanup operations
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()

	// Update DB status to sent
	notificationID, err := uuid.Parse(msg.NotificationID)
	if err == nil {
		if err := h.notificationRepo.UpdateStatus(ctx, notificationID, model.StatusSent); err != nil {
			h.logger.Error("failed to update notification status",
				slog.String("sender", senderName),
				slog.String("notification_id", msg.NotificationID),
				slog.String("error", err.Error()),
			)
		}
	}

	h.logger.Info("notification sent successfully",
		slog.String("sender", senderName),
		slog.String("notification_id", msg.NotificationID),
		slog.String("recipient", msg.Recipient),
		slog.Int("attempts", attempts),
		slog.Duration("duration", duration),
	)
}

// onFailure handles failed notification delivery.
func (h *BaseNotificationHandler) onFailure(msg *NotificationMessage, senderName string, result RetryResult, duration time.Duration) {
	h.logger.Error("notification failed after retries",
		slog.String("sender", senderName),
		slog.String("notification_id", msg.NotificationID),
		slog.Int("attempts", result.Attempts),
		slog.Duration("duration", duration),
		slog.String("error", result.LastErr.Error()),
	)

	// Use background context for cleanup operations
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()

	// Update DB status to failed
	notificationID, err := uuid.Parse(msg.NotificationID)
	if err == nil {
		if err := h.notificationRepo.UpdateStatus(ctx, notificationID, model.StatusFailed); err != nil {
			h.logger.Error("failed to update notification status to failed",
				slog.String("sender", senderName),
				slog.String("notification_id", msg.NotificationID),
				slog.String("error", err.Error()),
			)
		}
	}

	// Send to DLQ
	if h.dlqProducer != nil {
		if err := h.dlqProducer.PublishToDLQ(ctx, msg, result.LastErr, result.Attempts); err != nil {
			h.logger.Error("failed to publish to DLQ",
				slog.String("sender", senderName),
				slog.String("notification_id", msg.NotificationID),
				slog.String("error", err.Error()),
			)
		}
	}
}
