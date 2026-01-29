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

// SMSRequest represents the request body for SMS provider.
type SMSRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// SMSSender implements the NotificationSender strategy for SMS notifications.
type SMSSender struct {
	webhookURL string
	httpClient *http.Client
	logger     *slog.Logger
}

// NewSMSSender creates a new SMS sender strategy.
func NewSMSSender(webhookURL string, httpTimeout time.Duration, logger *slog.Logger) *SMSSender {
	return &SMSSender{
		webhookURL: webhookURL,
		httpClient: &http.Client{Timeout: httpTimeout},
		logger:     logger,
	}
}

// Channel returns the notification channel.
func (s *SMSSender) Channel() model.NotificationChannel {
	return model.ChannelSMS
}

// Name returns the sender name for logging.
func (s *SMSSender) Name() string {
	return "sms"
}

// Send sends the SMS to the external provider.
func (s *SMSSender) Send(ctx context.Context, msg *NotificationMessage) (*ProviderResponse, error) {
	reqBody := SMSRequest{
		To:      msg.Recipient,
		Channel: "sms",
		Content: msg.Content,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal SMS request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create SMS request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	s.logger.Debug("sending SMS",
		slog.String("notification_id", msg.NotificationID),
		slog.String("recipient", msg.Recipient),
	)

	resp, err := s.httpClient.Do(req)
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

// NewSMSHandler creates a complete SMS notification handler using the template pattern.
func NewSMSHandler(cfg HandlerConfig) *BaseNotificationHandler {
	return NewBaseNotificationHandler(cfg)
}
