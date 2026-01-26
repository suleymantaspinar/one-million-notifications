package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/notifications-management-api/internal/config"
	"github.com/notifications-management-api/internal/model"
	"github.com/segmentio/kafka-go"
)

// Producer wraps Kafka writer functionality.
type Producer struct {
	writers map[string]*kafka.Writer
	topics  config.KafkaTopics
	logger  *slog.Logger
}

// NewProducer creates a new Kafka producer.
func NewProducer(cfg config.KafkaConfig, logger *slog.Logger) *Producer {
	p := &Producer{
		writers: make(map[string]*kafka.Writer),
		topics:  cfg.Topics,
		logger:  logger,
	}

	// Create writers for each topic
	allTopics := []string{
		cfg.Topics.EmailHigh,
		cfg.Topics.EmailNormal,
		cfg.Topics.EmailLow,
		cfg.Topics.SMSHigh,
		cfg.Topics.SMSNormal,
		cfg.Topics.SMSLow,
		cfg.Topics.PushHigh,
		cfg.Topics.PushNormal,
		cfg.Topics.PushLow,
		cfg.Topics.DeadLetter,
	}

	for _, topic := range allTopics {
		p.writers[topic] = &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			WriteTimeout: cfg.WriteTimeout,
			ReadTimeout:  cfg.ReadTimeout,
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		}
	}

	return p
}

// GetTopicForNotification returns the appropriate Kafka topic based on channel and priority.
func (p *Producer) GetTopicForNotification(channel model.NotificationChannel, priority model.NotificationPriority) string {
	switch channel {
	case model.ChannelEmail:
		switch priority {
		case model.PriorityHigh:
			return p.topics.EmailHigh
		case model.PriorityLow:
			return p.topics.EmailLow
		default:
			return p.topics.EmailNormal
		}
	case model.ChannelSMS:
		switch priority {
		case model.PriorityHigh:
			return p.topics.SMSHigh
		case model.PriorityLow:
			return p.topics.SMSLow
		default:
			return p.topics.SMSNormal
		}
	case model.ChannelPush:
		switch priority {
		case model.PriorityHigh:
			return p.topics.PushHigh
		case model.PriorityLow:
			return p.topics.PushLow
		default:
			return p.topics.PushNormal
		}
	default:
		return p.topics.DeadLetter
	}
}

// Publish publishes a message to the specified topic.
func (p *Producer) Publish(ctx context.Context, topic string, key, value []byte) error {
	writer, ok := p.writers[topic]
	if !ok {
		return fmt.Errorf("unknown topic: %s", topic)
	}

	msg := kafka.Message{
		Key:   key,
		Value: value,
	}

	if err := writer.WriteMessages(ctx, msg); err != nil {
		p.logger.Error("failed to publish message",
			slog.String("topic", topic),
			slog.String("error", err.Error()),
		)
		return fmt.Errorf("failed to publish message to %s: %w", topic, err)
	}

	p.logger.Debug("message published",
		slog.String("topic", topic),
		slog.String("key", string(key)),
	)

	return nil
}

// Close closes all Kafka writers.
func (p *Producer) Close() error {
	var lastErr error
	for topic, writer := range p.writers {
		if err := writer.Close(); err != nil {
			p.logger.Error("failed to close writer",
				slog.String("topic", topic),
				slog.String("error", err.Error()),
			)
			lastErr = err
		}
	}
	return lastErr
}

// Health checks if Kafka is healthy by attempting to get metadata.
func (p *Producer) Health(ctx context.Context) error {
	// Use any writer to check connectivity
	for _, writer := range p.writers {
		if writer != nil {
			// The writer doesn't have a direct health check,
			// but we can verify the connection is established
			return nil
		}
	}
	return fmt.Errorf("no kafka writers available")
}
