package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/notifications-management-api/internal/model"
)

// EmailRequest represents the request body for Email provider.
type EmailRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// EmailSender implements the NotificationSender strategy for Email notifications.
type EmailSender struct {
	webhookURL string
	httpClient *http.Client
	logger     *slog.Logger
}

// NewEmailSender creates a new Email sender strategy.
func NewEmailSender(webhookURL string, httpTimeout time.Duration, logger *slog.Logger) *EmailSender {
	return &EmailSender{
		webhookURL: webhookURL,
		httpClient: &http.Client{Timeout: httpTimeout},
		logger:     logger,
	}
}

// Channel returns the notification channel.
func (s *EmailSender) Channel() model.NotificationChannel {
	return model.ChannelEmail
}

// Name returns the sender name for logging.
func (s *EmailSender) Name() string {
	return "email"
}

// Send sends the Email to the external provider.
func (s *EmailSender) Send(ctx context.Context, msg *NotificationMessage) (*ProviderResponse, error) {
	reqBody := EmailRequest{
		To:      msg.Recipient,
		Channel: "email",
		Content: msg.Content,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Email request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create Email request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	s.logger.Debug("sending Email",
		slog.String("notification_id", msg.NotificationID),
		slog.String("recipient", msg.Recipient),
	)

	resp, err := s.httpClient.Do(req)
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

// NewEmailHandler creates a complete Email notification handler using the template pattern.
func NewEmailHandler(cfg HandlerConfig) *BaseNotificationHandler {
	return NewBaseNotificationHandler(cfg)
}
