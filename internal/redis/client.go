package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config holds Redis configuration.
type Config struct {
	Host         string
	Port         int
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Client wraps Redis client functionality.
type Client struct {
	client *redis.Client
	logger *slog.Logger
}

// NewClient creates a new Redis client.
func NewClient(cfg Config, logger *slog.Logger) (*Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	logger.Info("connected to Redis",
		slog.String("addr", fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)),
	)

	return &Client{
		client: client,
		logger: logger,
	}, nil
}

// Close closes the Redis connection.
func (c *Client) Close() error {
	return c.client.Close()
}

// SetNX sets a key-value pair only if the key doesn't exist (for idempotency).
// Returns true if the key was set, false if it already existed.
func (c *Client) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	result, err := c.client.SetNX(ctx, key, value, expiration).Result()
	if err != nil {
		return false, fmt.Errorf("failed to set key %s: %w", key, err)
	}
	return result, nil
}

// Get gets a value by key.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	result, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to get key %s: %w", key, err)
	}
	return result, nil
}

// Exists checks if a key exists.
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	result, err := c.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check existence of key %s: %w", key, err)
	}
	return result > 0, nil
}

// Delete deletes a key.
func (c *Client) Delete(ctx context.Context, key string) error {
	if err := c.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("failed to delete key %s: %w", key, err)
	}
	return nil
}

// Health checks if Redis is healthy.
func (c *Client) Health(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// IdempotencyStore provides idempotency checking using Redis.
type IdempotencyStore struct {
	client       *Client
	keyPrefix    string
	ttl          time.Duration
	logger       *slog.Logger
}

// NewIdempotencyStore creates a new idempotency store.
func NewIdempotencyStore(client *Client, keyPrefix string, ttl time.Duration, logger *slog.Logger) *IdempotencyStore {
	return &IdempotencyStore{
		client:    client,
		keyPrefix: keyPrefix,
		ttl:       ttl,
		logger:    logger,
	}
}

// buildKey builds the Redis key for a message ID.
func (s *IdempotencyStore) buildKey(messageID string) string {
	return fmt.Sprintf("%s:%s", s.keyPrefix, messageID)
}

// IsProcessed checks if a message has already been processed.
func (s *IdempotencyStore) IsProcessed(ctx context.Context, messageID string) (bool, error) {
	key := s.buildKey(messageID)
	exists, err := s.client.Exists(ctx, key)
	if err != nil {
		s.logger.Error("failed to check idempotency",
			slog.String("message_id", messageID),
			slog.String("error", err.Error()),
		)
		return false, err
	}
	return exists, nil
}

// MarkProcessed marks a message as processed.
func (s *IdempotencyStore) MarkProcessed(ctx context.Context, messageID string, status string) error {
	key := s.buildKey(messageID)
	success, err := s.client.SetNX(ctx, key, status, s.ttl)
	if err != nil {
		s.logger.Error("failed to mark message as processed",
			slog.String("message_id", messageID),
			slog.String("error", err.Error()),
		)
		return err
	}
	
	if !success {
		s.logger.Debug("message already marked as processed",
			slog.String("message_id", messageID),
		)
	}
	
	return nil
}

// TryAcquire attempts to acquire a lock for processing a message.
// Returns true if the lock was acquired (message not yet processed).
// The lock has a short TTL to prevent blocking if the process crashes.
func (s *IdempotencyStore) TryAcquire(ctx context.Context, messageID string) (bool, error) {
	key := s.buildLockKey(messageID)
	// Use a shorter TTL for locks (e.g., 5 minutes) to prevent long blocking
	lockTTL := 5 * time.Minute
	if s.ttl < lockTTL {
		lockTTL = s.ttl
	}
	success, err := s.client.SetNX(ctx, key, "processing", lockTTL)
	if err != nil {
		return false, err
	}
	return success, nil
}

// ReleaseLock releases the processing lock for a message.
// Should be called when processing fails before completion.
func (s *IdempotencyStore) ReleaseLock(ctx context.Context, messageID string) error {
	key := s.buildLockKey(messageID)
	return s.client.Delete(ctx, key)
}

// buildLockKey builds the Redis key for a message lock.
func (s *IdempotencyStore) buildLockKey(messageID string) string {
	return fmt.Sprintf("%s:lock:%s", s.keyPrefix, messageID)
}

// GetStatus returns the processing status of a message.
func (s *IdempotencyStore) GetStatus(ctx context.Context, messageID string) (string, error) {
	key := s.buildKey(messageID)
	return s.client.Get(ctx, key)
}
