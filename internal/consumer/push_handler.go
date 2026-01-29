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

// PushRequest represents the request body for Push provider.
type PushRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// PushSender implements the NotificationSender strategy for Push notifications.
type PushSender struct {
	webhookURL string
	httpClient *http.Client
	logger     *slog.Logger
}

// NewPushSender creates a new Push sender strategy.
func NewPushSender(webhookURL string, httpTimeout time.Duration, logger *slog.Logger) *PushSender {
	return &PushSender{
		webhookURL: webhookURL,
		httpClient: &http.Client{Timeout: httpTimeout},
		logger:     logger,
	}
}

// Channel returns the notification channel.
func (s *PushSender) Channel() model.NotificationChannel {
	return model.ChannelPush
}

// Name returns the sender name for logging.
func (s *PushSender) Name() string {
	return "push"
}

// Send sends the Push notification to the external provider.
func (s *PushSender) Send(ctx context.Context, msg *NotificationMessage) (*ProviderResponse, error) {
	reqBody := PushRequest{
		To:      msg.Recipient,
		Channel: "push",
		Content: msg.Content,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Push request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create Push request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	s.logger.Debug("sending Push",
		slog.String("notification_id", msg.NotificationID),
		slog.String("recipient", msg.Recipient),
	)

	resp, err := s.httpClient.Do(req)
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

// NewPushHandler creates a complete Push notification handler using the template pattern.
func NewPushHandler(cfg HandlerConfig) *BaseNotificationHandler {
	return NewBaseNotificationHandler(cfg)
}
