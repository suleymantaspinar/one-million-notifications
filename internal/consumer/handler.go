package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/notifications-management-api/internal/model"
	"github.com/notifications-management-api/internal/redis"
	"github.com/notifications-management-api/internal/repository"
)

// cleanupTimeout is the timeout for cleanup operations (DB updates, Redis) that must complete
// even if the request context is cancelled.
const cleanupTimeout = 10 * time.Second

// NotificationHandler defines the interface for handling notifications.
type NotificationHandler interface {
	// Process handles the full processing pipeline: idempotency, rate limiting, retry, send, DB update, DLQ.
	Process(ctx context.Context, msg *NotificationMessage) error
	// Channel returns the notification channel this handler processes.
	Channel() model.NotificationChannel
}

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

// DLQProducer interface for publishing to dead letter queue.
type DLQProducer interface {
	PublishToDLQ(ctx context.Context, msg *NotificationMessage, err error, attempts int) error
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

// HandlerDependencies holds shared dependencies for all handlers.
type HandlerDependencies struct {
	NotificationRepo *repository.NotificationRepository
	IdempotencyStore *redis.IdempotencyStore
	RateLimiter      *RateLimiter
	Retryer          *Retryer
	DLQProducer      DLQProducer
	Logger           *slog.Logger
}

// ============================================================================
// SMS Handler
// ============================================================================

// SMSRequest represents the request body for SMS provider.
type SMSRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// SMSHandler handles SMS notifications.
type SMSHandler struct {
	webhookURL string
	httpClient *http.Client
	deps       *HandlerDependencies
}

// NewSMSHandler creates a new SMS notification handler.
func NewSMSHandler(webhookURL string, httpTimeout time.Duration, deps *HandlerDependencies) *SMSHandler {
	return &SMSHandler{
		webhookURL: webhookURL,
		httpClient: &http.Client{Timeout: httpTimeout},
		deps:       deps,
	}
}

// Channel returns the notification channel.
func (h *SMSHandler) Channel() model.NotificationChannel {
	return model.ChannelSMS
}

// Process handles the full processing pipeline for SMS.
func (h *SMSHandler) Process(ctx context.Context, msg *NotificationMessage) error {
	startTime := time.Now()
	logger := h.deps.Logger

	// 1. Check idempotency - skip if already processed
	processed, err := h.deps.IdempotencyStore.IsProcessed(ctx, msg.NotificationID)
	if err != nil {
		logger.Warn("idempotency check failed, proceeding",
			slog.String("notification_id", msg.NotificationID),
			slog.String("error", err.Error()),
		)
	} else if processed {
		logger.Info("SMS already processed, skipping",
			slog.String("notification_id", msg.NotificationID),
		)
		return nil
	}

	// Try to acquire processing lock
	acquired, err := h.deps.IdempotencyStore.TryAcquire(ctx, msg.NotificationID)
	if err != nil {
		logger.Warn("failed to acquire lock, proceeding",
			slog.String("notification_id", msg.NotificationID),
			slog.String("error", err.Error()),
		)
	} else if !acquired {
		logger.Info("SMS being processed by another worker, skipping",
			slog.String("notification_id", msg.NotificationID),
		)
		return nil
	}

	// Track if we need to release the lock (only if we acquired it and processing fails before completion)
	lockAcquired := acquired
	completed := false

	// Ensure lock is released if we exit without completing
	defer func() {
		if lockAcquired && !completed {
			// Use background context for cleanup since request ctx may be cancelled
			cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			defer cancel()
			if err := h.deps.IdempotencyStore.ReleaseLock(cleanupCtx, msg.NotificationID); err != nil {
				logger.Warn("failed to release lock",
					slog.String("notification_id", msg.NotificationID),
					slog.String("error", err.Error()),
				)
			}
		}
	}()

	// 2. Apply rate limiting
	if err := h.deps.RateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiting wait failed: %w", err)
	}

	// 3. Send with retry
	result := h.deps.Retryer.Do(ctx, "send_sms", func(ctx context.Context) error {
		_, err := h.send(ctx, msg)
		return err
	})

	duration := time.Since(startTime)

	// 4. Handle result (use background context for cleanup operations)
	if result.Success() {
		h.handleSuccess(msg, result.Attempts, duration)
		completed = true
		return nil
	}

	h.handleFailure(msg, result, duration)
	completed = true // Mark completed so we don't release lock (it's already updated to failed status)
	return result.LastErr
}

// send sends the SMS to the external provider.
func (h *SMSHandler) send(ctx context.Context, msg *NotificationMessage) (*ProviderResponse, error) {
	reqBody := SMSRequest{
		To:      msg.Recipient,
		Channel: "sms",
		Content: msg.Content,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal SMS request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.webhookURL, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create SMS request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	h.deps.Logger.Debug("sending SMS",
		slog.String("notification_id", msg.NotificationID),
		slog.String("recipient", msg.Recipient),
	)

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send SMS: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &ProviderError{StatusCode: resp.StatusCode, Body: string(body)}
	}

	var providerResp ProviderResponse
	if err := json.Unmarshal(body, &providerResp); err != nil {
		if resp.StatusCode == http.StatusOK {
			return &ProviderResponse{
				MessageID: msg.NotificationID,
				Status:    "accepted",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
			}, nil
		}
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &providerResp, nil
}

func (h *SMSHandler) handleSuccess(msg *NotificationMessage, attempts int, duration time.Duration) {
	logger := h.deps.Logger

	// Use background context for cleanup operations
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()

	// Update DB status to sent
	notificationID, err := uuid.Parse(msg.NotificationID)
	if err == nil {
		if err := h.deps.NotificationRepo.UpdateStatus(ctx, notificationID, model.StatusSent); err != nil {
			logger.Error("failed to update SMS status", slog.String("notification_id", msg.NotificationID), slog.String("error", err.Error()))
		}
	}

	// Mark as processed in Redis (this also serves as releasing the lock by changing status)
	if err := h.deps.IdempotencyStore.MarkProcessed(ctx, msg.NotificationID, "sent"); err != nil {
		logger.Warn("failed to mark SMS as processed", slog.String("notification_id", msg.NotificationID), slog.String("error", err.Error()))
	}

	logger.Info("SMS sent successfully",
		slog.String("notification_id", msg.NotificationID),
		slog.String("recipient", msg.Recipient),
		slog.Int("attempts", attempts),
		slog.Duration("duration", duration),
	)
}

func (h *SMSHandler) handleFailure(msg *NotificationMessage, result RetryResult, duration time.Duration) {
	logger := h.deps.Logger

	logger.Error("SMS failed after retries",
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
		if err := h.deps.NotificationRepo.UpdateStatus(ctx, notificationID, model.StatusFailed); err != nil {
			logger.Error("failed to update SMS status to failed", slog.String("notification_id", msg.NotificationID), slog.String("error", err.Error()))
		}
	}

	// Mark as failed in Redis
	if err := h.deps.IdempotencyStore.MarkProcessed(ctx, msg.NotificationID, "failed"); err != nil {
		logger.Warn("failed to mark SMS as failed", slog.String("notification_id", msg.NotificationID), slog.String("error", err.Error()))
	}

	// Send to DLQ
	if h.deps.DLQProducer != nil {
		if err := h.deps.DLQProducer.PublishToDLQ(ctx, msg, result.LastErr, result.Attempts); err != nil {
			logger.Error("failed to publish SMS to DLQ", slog.String("notification_id", msg.NotificationID), slog.String("error", err.Error()))
		}
	}
}

// ============================================================================
// Email Handler
// ============================================================================

// EmailRequest represents the request body for Email provider.
type EmailRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// EmailHandler handles Email notifications.
type EmailHandler struct {
	webhookURL string
	httpClient *http.Client
	deps       *HandlerDependencies
}

// NewEmailHandler creates a new Email notification handler.
func NewEmailHandler(webhookURL string, httpTimeout time.Duration, deps *HandlerDependencies) *EmailHandler {
	return &EmailHandler{
		webhookURL: webhookURL,
		httpClient: &http.Client{Timeout: httpTimeout},
		deps:       deps,
	}
}

// Channel returns the notification channel.
func (h *EmailHandler) Channel() model.NotificationChannel {
	return model.ChannelEmail
}

// Process handles the full processing pipeline for Email.
func (h *EmailHandler) Process(ctx context.Context, msg *NotificationMessage) error {
	startTime := time.Now()
	logger := h.deps.Logger

	// 1. Check idempotency
	processed, err := h.deps.IdempotencyStore.IsProcessed(ctx, msg.NotificationID)
	if err != nil {
		logger.Warn("idempotency check failed, proceeding",
			slog.String("notification_id", msg.NotificationID),
			slog.String("error", err.Error()),
		)
	} else if processed {
		logger.Info("Email already processed, skipping",
			slog.String("notification_id", msg.NotificationID),
		)
		return nil
	}

	// Try to acquire processing lock
	acquired, err := h.deps.IdempotencyStore.TryAcquire(ctx, msg.NotificationID)
	if err != nil {
		logger.Warn("failed to acquire lock, proceeding",
			slog.String("notification_id", msg.NotificationID),
			slog.String("error", err.Error()),
		)
	} else if !acquired {
		logger.Info("Email being processed by another worker, skipping",
			slog.String("notification_id", msg.NotificationID),
		)
		return nil
	}

	// Track if we need to release the lock
	lockAcquired := acquired
	completed := false

	defer func() {
		if lockAcquired && !completed {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			defer cancel()
			if err := h.deps.IdempotencyStore.ReleaseLock(cleanupCtx, msg.NotificationID); err != nil {
				logger.Warn("failed to release lock",
					slog.String("notification_id", msg.NotificationID),
					slog.String("error", err.Error()),
				)
			}
		}
	}()

	// 2. Apply rate limiting
	if err := h.deps.RateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiting wait failed: %w", err)
	}

	// 3. Send with retry
	result := h.deps.Retryer.Do(ctx, "send_email", func(ctx context.Context) error {
		_, err := h.send(ctx, msg)
		return err
	})

	duration := time.Since(startTime)

	// 4. Handle result
	if result.Success() {
		h.handleSuccess(msg, result.Attempts, duration)
		completed = true
		return nil
	}

	h.handleFailure(msg, result, duration)
	completed = true
	return result.LastErr
}

// send sends the Email to the external provider.
func (h *EmailHandler) send(ctx context.Context, msg *NotificationMessage) (*ProviderResponse, error) {
	reqBody := EmailRequest{
		To:      msg.Recipient,
		Channel: "email",
		Content: msg.Content,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Email request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.webhookURL, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create Email request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	h.deps.Logger.Debug("sending Email",
		slog.String("notification_id", msg.NotificationID),
		slog.String("recipient", msg.Recipient),
	)

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send Email: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &ProviderError{StatusCode: resp.StatusCode, Body: string(body)}
	}

	var providerResp ProviderResponse
	if err := json.Unmarshal(body, &providerResp); err != nil {
		if resp.StatusCode == http.StatusOK {
			return &ProviderResponse{
				MessageID: msg.NotificationID,
				Status:    "accepted",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
			}, nil
		}
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &providerResp, nil
}

func (h *EmailHandler) handleSuccess(msg *NotificationMessage, attempts int, duration time.Duration) {
	logger := h.deps.Logger

	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()

	notificationID, err := uuid.Parse(msg.NotificationID)
	if err == nil {
		if err := h.deps.NotificationRepo.UpdateStatus(ctx, notificationID, model.StatusSent); err != nil {
			logger.Error("failed to update Email status", slog.String("notification_id", msg.NotificationID), slog.String("error", err.Error()))
		}
	}

	if err := h.deps.IdempotencyStore.MarkProcessed(ctx, msg.NotificationID, "sent"); err != nil {
		logger.Warn("failed to mark Email as processed", slog.String("notification_id", msg.NotificationID), slog.String("error", err.Error()))
	}

	logger.Info("Email sent successfully",
		slog.String("notification_id", msg.NotificationID),
		slog.String("recipient", msg.Recipient),
		slog.Int("attempts", attempts),
		slog.Duration("duration", duration),
	)
}

func (h *EmailHandler) handleFailure(msg *NotificationMessage, result RetryResult, duration time.Duration) {
	logger := h.deps.Logger

	logger.Error("Email failed after retries",
		slog.String("notification_id", msg.NotificationID),
		slog.Int("attempts", result.Attempts),
		slog.Duration("duration", duration),
		slog.String("error", result.LastErr.Error()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()

	notificationID, err := uuid.Parse(msg.NotificationID)
	if err == nil {
		if err := h.deps.NotificationRepo.UpdateStatus(ctx, notificationID, model.StatusFailed); err != nil {
			logger.Error("failed to update Email status to failed", slog.String("notification_id", msg.NotificationID), slog.String("error", err.Error()))
		}
	}

	if err := h.deps.IdempotencyStore.MarkProcessed(ctx, msg.NotificationID, "failed"); err != nil {
		logger.Warn("failed to mark Email as failed", slog.String("notification_id", msg.NotificationID), slog.String("error", err.Error()))
	}

	if h.deps.DLQProducer != nil {
		if err := h.deps.DLQProducer.PublishToDLQ(ctx, msg, result.LastErr, result.Attempts); err != nil {
			logger.Error("failed to publish Email to DLQ", slog.String("notification_id", msg.NotificationID), slog.String("error", err.Error()))
		}
	}
}

// ============================================================================
// Push Handler
// ============================================================================

// PushRequest represents the request body for Push provider.
type PushRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// PushHandler handles Push notifications.
type PushHandler struct {
	webhookURL string
	httpClient *http.Client
	deps       *HandlerDependencies
}

// NewPushHandler creates a new Push notification handler.
func NewPushHandler(webhookURL string, httpTimeout time.Duration, deps *HandlerDependencies) *PushHandler {
	return &PushHandler{
		webhookURL: webhookURL,
		httpClient: &http.Client{Timeout: httpTimeout},
		deps:       deps,
	}
}

// Channel returns the notification channel.
func (h *PushHandler) Channel() model.NotificationChannel {
	return model.ChannelPush
}

// Process handles the full processing pipeline for Push.
func (h *PushHandler) Process(ctx context.Context, msg *NotificationMessage) error {
	startTime := time.Now()
	logger := h.deps.Logger

	// 1. Check idempotency
	processed, err := h.deps.IdempotencyStore.IsProcessed(ctx, msg.NotificationID)
	if err != nil {
		logger.Warn("idempotency check failed, proceeding",
			slog.String("notification_id", msg.NotificationID),
			slog.String("error", err.Error()),
		)
	} else if processed {
		logger.Info("Push already processed, skipping",
			slog.String("notification_id", msg.NotificationID),
		)
		return nil
	}

	// Try to acquire processing lock
	acquired, err := h.deps.IdempotencyStore.TryAcquire(ctx, msg.NotificationID)
	if err != nil {
		logger.Warn("failed to acquire lock, proceeding",
			slog.String("notification_id", msg.NotificationID),
			slog.String("error", err.Error()),
		)
	} else if !acquired {
		logger.Info("Push being processed by another worker, skipping",
			slog.String("notification_id", msg.NotificationID),
		)
		return nil
	}

	// Track if we need to release the lock
	lockAcquired := acquired
	completed := false

	defer func() {
		if lockAcquired && !completed {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			defer cancel()
			if err := h.deps.IdempotencyStore.ReleaseLock(cleanupCtx, msg.NotificationID); err != nil {
				logger.Warn("failed to release lock",
					slog.String("notification_id", msg.NotificationID),
					slog.String("error", err.Error()),
				)
			}
		}
	}()

	// 2. Apply rate limiting
	if err := h.deps.RateLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiting wait failed: %w", err)
	}

	// 3. Send with retry
	result := h.deps.Retryer.Do(ctx, "send_push", func(ctx context.Context) error {
		_, err := h.send(ctx, msg)
		return err
	})

	duration := time.Since(startTime)

	// 4. Handle result
	if result.Success() {
		h.handleSuccess(msg, result.Attempts, duration)
		completed = true
		return nil
	}

	h.handleFailure(msg, result, duration)
	completed = true
	return result.LastErr
}

// send sends the Push notification to the external provider.
func (h *PushHandler) send(ctx context.Context, msg *NotificationMessage) (*ProviderResponse, error) {
	reqBody := PushRequest{
		To:      msg.Recipient,
		Channel: "push",
		Content: msg.Content,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Push request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.webhookURL, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create Push request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	h.deps.Logger.Debug("sending Push",
		slog.String("notification_id", msg.NotificationID),
		slog.String("recipient", msg.Recipient),
	)

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send Push: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &ProviderError{StatusCode: resp.StatusCode, Body: string(body)}
	}

	var providerResp ProviderResponse
	if err := json.Unmarshal(body, &providerResp); err != nil {
		if resp.StatusCode == http.StatusOK {
			return &ProviderResponse{
				MessageID: msg.NotificationID,
				Status:    "accepted",
				Timestamp: time.Now().UTC().Format(time.RFC3339),
			}, nil
		}
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &providerResp, nil
}

func (h *PushHandler) handleSuccess(msg *NotificationMessage, attempts int, duration time.Duration) {
	logger := h.deps.Logger

	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()

	notificationID, err := uuid.Parse(msg.NotificationID)
	if err == nil {
		if err := h.deps.NotificationRepo.UpdateStatus(ctx, notificationID, model.StatusSent); err != nil {
			logger.Error("failed to update Push status", slog.String("notification_id", msg.NotificationID), slog.String("error", err.Error()))
		}
	}

	if err := h.deps.IdempotencyStore.MarkProcessed(ctx, msg.NotificationID, "sent"); err != nil {
		logger.Warn("failed to mark Push as processed", slog.String("notification_id", msg.NotificationID), slog.String("error", err.Error()))
	}

	logger.Info("Push sent successfully",
		slog.String("notification_id", msg.NotificationID),
		slog.String("recipient", msg.Recipient),
		slog.Int("attempts", attempts),
		slog.Duration("duration", duration),
	)
}

func (h *PushHandler) handleFailure(msg *NotificationMessage, result RetryResult, duration time.Duration) {
	logger := h.deps.Logger

	logger.Error("Push failed after retries",
		slog.String("notification_id", msg.NotificationID),
		slog.Int("attempts", result.Attempts),
		slog.Duration("duration", duration),
		slog.String("error", result.LastErr.Error()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()

	notificationID, err := uuid.Parse(msg.NotificationID)
	if err == nil {
		if err := h.deps.NotificationRepo.UpdateStatus(ctx, notificationID, model.StatusFailed); err != nil {
			logger.Error("failed to update Push status to failed", slog.String("notification_id", msg.NotificationID), slog.String("error", err.Error()))
		}
	}

	if err := h.deps.IdempotencyStore.MarkProcessed(ctx, msg.NotificationID, "failed"); err != nil {
		logger.Warn("failed to mark Push as failed", slog.String("notification_id", msg.NotificationID), slog.String("error", err.Error()))
	}

	if h.deps.DLQProducer != nil {
		if err := h.deps.DLQProducer.PublishToDLQ(ctx, msg, result.LastErr, result.Attempts); err != nil {
			logger.Error("failed to publish Push to DLQ", slog.String("notification_id", msg.NotificationID), slog.String("error", err.Error()))
		}
	}
}
