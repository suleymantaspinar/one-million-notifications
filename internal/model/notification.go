package model

import (
	"time"

	"github.com/google/uuid"
)

// NotificationChannel represents the delivery channel for a notification.
type NotificationChannel string

const (
	ChannelEmail NotificationChannel = "email"
	ChannelSMS   NotificationChannel = "sms"
	ChannelPush  NotificationChannel = "push"
)

// NotificationPriority represents the priority level of a notification.
type NotificationPriority string

const (
	PriorityLow    NotificationPriority = "low"
	PriorityNormal NotificationPriority = "normal"
	PriorityHigh   NotificationPriority = "high"
)

// NotificationStatus represents the current status of a notification.
type NotificationStatus string

const (
	StatusPending   NotificationStatus = "pending"
	StatusSent      NotificationStatus = "sent"
	StatusFailed    NotificationStatus = "failed"
	StatusCancelled NotificationStatus = "cancelled"
)

// BatchStatus represents the current status of a batch.
type BatchStatus string

const (
	BatchStatusProcessing BatchStatus = "processing"
	BatchStatusCompleted  BatchStatus = "completed"
	BatchStatusFailed     BatchStatus = "failed"
)

// OutboxEventStatus represents the current status of an outbox event.
type OutboxEventStatus string

const (
	OutboxStatusReady  OutboxEventStatus = "ready"
	OutboxStatusSent   OutboxEventStatus = "sent"
	OutboxStatusFailed OutboxEventStatus = "failed"
)

// Notification represents a notification entity.
type Notification struct {
	MessageID uuid.UUID            `json:"messageId"`
	BatchID   *uuid.UUID           `json:"batchId,omitempty"`
	Recipient string               `json:"recipient"`
	Channel   NotificationChannel  `json:"channel"`
	Content   string               `json:"content"`
	Priority  NotificationPriority `json:"priority"`
	Status    NotificationStatus   `json:"status"`
	CreatedAt time.Time            `json:"createdAt"`
	UpdatedAt time.Time            `json:"updatedAt"`
}

// Batch represents a batch of notifications.
type Batch struct {
	BatchID      uuid.UUID   `json:"batchId"`
	TotalCount   int         `json:"totalCount"`
	SuccessCount int         `json:"successCount"`
	FailureCount int         `json:"failureCount"`
	Status       BatchStatus `json:"status"`
	CreatedAt    time.Time   `json:"createdAt"`
}

// OutboxEvent represents an event in the transactional outbox.
type OutboxEvent struct {
	ID             uuid.UUID            `json:"id"`
	NotificationID uuid.UUID            `json:"notificationId"`
	Recipient      string               `json:"recipient"`
	Channel        NotificationChannel  `json:"channel"`
	Content        string               `json:"content"`
	Priority       NotificationPriority `json:"priority"`
	Status         OutboxEventStatus    `json:"status"`
	RetryCount     int                  `json:"retryCount"`
	CreatedAt      time.Time            `json:"createdAt"`
	ProcessedAt    *time.Time           `json:"processedAt,omitempty"`
}

// CreateNotificationRequest represents a request to create a notification.
type CreateNotificationRequest struct {
	To       string               `json:"to"`
	Channel  NotificationChannel  `json:"channel"`
	Content  string               `json:"content"`
	Priority NotificationPriority `json:"priority,omitempty"`
}

// CreateNotificationBatchRequest represents a request to create multiple notifications.
type CreateNotificationBatchRequest struct {
	Notifications []CreateNotificationRequest `json:"notifications"`
}

// NotificationResponse represents the API response for a notification.
type NotificationResponse struct {
	MessageID uuid.UUID            `json:"messageId"`
	To        string               `json:"to"`
	Channel   NotificationChannel  `json:"channel"`
	Content   string               `json:"content"`
	Priority  NotificationPriority `json:"priority"`
	Status    NotificationStatus   `json:"status"`
	CreatedAt time.Time            `json:"createdAt"`
	UpdatedAt time.Time            `json:"updatedAt"`
}

// BatchResponse represents the API response for a batch.
type BatchResponse struct {
	BatchID       uuid.UUID              `json:"batchId"`
	TotalCount    int                    `json:"totalCount"`
	SuccessCount  int                    `json:"successCount"`
	FailureCount  int                    `json:"failureCount"`
	Status        BatchStatus            `json:"status"`
	Notifications []NotificationResponse `json:"notifications,omitempty"`
	CreatedAt     time.Time              `json:"createdAt"`
}

// NotificationListResponse represents the API response for listing notifications.
type NotificationListResponse struct {
	Data       []NotificationResponse `json:"data"`
	Pagination Pagination             `json:"pagination"`
}

// Pagination represents pagination metadata.
type Pagination struct {
	Page       int `json:"page"`
	PageSize   int `json:"pageSize"`
	TotalPages int `json:"totalPages"`
	TotalCount int `json:"totalCount"`
}

// NotificationFilter represents filters for listing notifications.
type NotificationFilter struct {
	Status    *NotificationStatus
	Channel   *NotificationChannel
	StartDate *time.Time
	EndDate   *time.Time
	Page      int
	PageSize  int
}

// ErrorResponse represents an API error response.
type ErrorResponse struct {
	Code      string     `json:"code"`
	Message   string     `json:"message"`
	MessageID *uuid.UUID `json:"messageId,omitempty"`
}

// ToResponse converts a Notification to NotificationResponse.
func (n *Notification) ToResponse() NotificationResponse {
	return NotificationResponse{
		MessageID: n.MessageID,
		To:        n.Recipient,
		Channel:   n.Channel,
		Content:   n.Content,
		Priority:  n.Priority,
		Status:    n.Status,
		CreatedAt: n.CreatedAt,
		UpdatedAt: n.UpdatedAt,
	}
}

// ToBatchResponse converts a Batch to BatchResponse.
func (b *Batch) ToBatchResponse(notifications []NotificationResponse) BatchResponse {
	return BatchResponse{
		BatchID:       b.BatchID,
		TotalCount:    b.TotalCount,
		SuccessCount:  b.SuccessCount,
		FailureCount:  b.FailureCount,
		Status:        b.Status,
		Notifications: notifications,
		CreatedAt:     b.CreatedAt,
	}
}

// Validate validates the CreateNotificationRequest.
func (r *CreateNotificationRequest) Validate() error {
	if r.To == "" {
		return ErrInvalidRecipient
	}
	if r.Channel == "" {
		return ErrInvalidChannel
	}
	if r.Channel != ChannelEmail && r.Channel != ChannelSMS && r.Channel != ChannelPush {
		return ErrInvalidChannel
	}
	if r.Content == "" {
		return ErrInvalidContent
	}
	if len(r.Content) > 1000 {
		return ErrContentTooLong
	}
	if r.Priority == "" {
		r.Priority = PriorityNormal
	}
	if r.Priority != PriorityLow && r.Priority != PriorityNormal && r.Priority != PriorityHigh {
		return ErrInvalidPriority
	}
	return nil
}
