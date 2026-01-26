package model

import "errors"

// Domain errors.
var (
	ErrNotificationNotFound = errors.New("notification not found")
	ErrBatchNotFound        = errors.New("batch not found")
	ErrInvalidRecipient     = errors.New("recipient is required")
	ErrInvalidChannel       = errors.New("invalid channel: must be email, sms, or push")
	ErrInvalidContent       = errors.New("content is required")
	ErrContentTooLong       = errors.New("content must not exceed 1000 characters")
	ErrInvalidPriority      = errors.New("invalid priority: must be low, normal, or high")
	ErrCannotCancel         = errors.New("only pending notifications can be cancelled")
	ErrEmptyBatch           = errors.New("batch must contain at least one notification")
	ErrBatchTooLarge        = errors.New("batch must not exceed 1000 notifications")
)
