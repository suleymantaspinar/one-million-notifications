package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/notifications-management-api/internal/config"
	"github.com/notifications-management-api/internal/model"
	"github.com/segmentio/kafka-go"
)

// KafkaConsumer consumes messages from Kafka topics.
type KafkaConsumer struct {
	readers       map[string]*kafka.Reader
	poolManager   *WorkerPoolManager
	logger        *slog.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	cfg           config.KafkaConfig
	consumerGroup string
}

// NewKafkaConsumer creates a new Kafka consumer.
func NewKafkaConsumer(
	cfg config.KafkaConfig,
	consumerGroup string,
	poolManager *WorkerPoolManager,
	logger *slog.Logger,
) *KafkaConsumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &KafkaConsumer{
		readers:       make(map[string]*kafka.Reader),
		poolManager:   poolManager,
		logger:        logger,
		ctx:           ctx,
		cancel:        cancel,
		cfg:           cfg,
		consumerGroup: consumerGroup,
	}
}

// topicToChannel maps a topic name to a notification channel.
func topicToChannel(topic string, topics config.KafkaTopics) model.NotificationChannel {
	switch topic {
	case topics.EmailHigh, topics.EmailNormal, topics.EmailLow:
		return model.ChannelEmail
	case topics.SMSHigh, topics.SMSNormal, topics.SMSLow:
		return model.ChannelSMS
	case topics.PushHigh, topics.PushNormal, topics.PushLow:
		return model.ChannelPush
	default:
		return ""
	}
}

// Subscribe subscribes to all notification topics.
func (c *KafkaConsumer) Subscribe() error {
	topics := []string{
		c.cfg.Topics.EmailHigh,
		c.cfg.Topics.EmailNormal,
		c.cfg.Topics.EmailLow,
		c.cfg.Topics.SMSHigh,
		c.cfg.Topics.SMSNormal,
		c.cfg.Topics.SMSLow,
		c.cfg.Topics.PushHigh,
		c.cfg.Topics.PushNormal,
		c.cfg.Topics.PushLow,
	}

	for _, topic := range topics {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:        c.cfg.Brokers,
			Topic:          topic,
			GroupID:        c.consumerGroup,
			MinBytes:       10e3,        // 10KB
			MaxBytes:       10e6,        // 10MB
			MaxWait:        time.Second, // Maximum wait time for batch
			ReadBackoffMin: 100 * time.Millisecond,
			ReadBackoffMax: time.Second,
			CommitInterval: time.Second, // Auto-commit interval
			StartOffset:    kafka.FirstOffset,
		})

		c.readers[topic] = reader
		c.logger.Info("subscribed to topic",
			slog.String("topic", topic),
			slog.String("consumer_group", c.consumerGroup),
		)
	}

	return nil
}

// Start starts consuming messages from all subscribed topics.
func (c *KafkaConsumer) Start() {
	for topic, reader := range c.readers {
		c.wg.Add(1)
		go c.consumeTopic(topic, reader)
	}
}

// consumeTopic consumes messages from a single topic.
func (c *KafkaConsumer) consumeTopic(topic string, reader *kafka.Reader) {
	defer c.wg.Done()

	channel := topicToChannel(topic, c.cfg.Topics)
	if channel == "" {
		c.logger.Error("unknown topic, cannot determine channel",
			slog.String("topic", topic),
		)
		return
	}

	pool, ok := c.poolManager.GetPool(channel)
	if !ok {
		c.logger.Error("no worker pool for channel",
			slog.String("topic", topic),
			slog.String("channel", string(channel)),
		)
		return
	}

	c.logger.Info("starting topic consumer",
		slog.String("topic", topic),
		slog.String("channel", string(channel)),
	)

	for {
		select {
		case <-c.ctx.Done():
			c.logger.Info("stopping topic consumer",
				slog.String("topic", topic),
			)
			return
		default:
			// Fetch message with context
			msg, err := reader.FetchMessage(c.ctx)
			if err != nil {
				if c.ctx.Err() != nil {
					// Context cancelled, normal shutdown
					return
				}
				c.logger.Error("failed to fetch message",
					slog.String("topic", topic),
					slog.String("error", err.Error()),
				)
				continue
			}

			// Parse message
			notifMsg, err := ParseMessage(msg.Value)
			if err != nil {
				c.logger.Error("failed to parse message",
					slog.String("topic", topic),
					slog.Int64("offset", msg.Offset),
					slog.String("error", err.Error()),
				)
				// Commit invalid message to prevent blocking
				if err := reader.CommitMessages(c.ctx, msg); err != nil {
					c.logger.Error("failed to commit invalid message",
						slog.String("topic", topic),
						slog.Int64("offset", msg.Offset),
						slog.String("error", err.Error()),
					)
				}
				continue
			}

			// Create job
			job := &Job{
				Message:   notifMsg,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Topic:     topic,
			}

			// Submit to worker pool with backpressure handling
			submitted := false
			for !submitted {
				select {
				case <-c.ctx.Done():
					return
				default:
					if pool.SubmitWithTimeout(job, 5*time.Second) {
						submitted = true
					} else {
						c.logger.Warn("worker pool busy, retrying submission",
							slog.String("topic", topic),
							slog.String("notification_id", notifMsg.NotificationID),
						)
					}
				}
			}

			// Commit offset after successful submission
			if err := reader.CommitMessages(c.ctx, msg); err != nil {
				c.logger.Error("failed to commit message",
					slog.String("topic", topic),
					slog.Int64("offset", msg.Offset),
					slog.String("error", err.Error()),
				)
			}
		}
	}
}

// Stop stops the consumer gracefully.
func (c *KafkaConsumer) Stop() {
	c.logger.Info("stopping Kafka consumer...")
	c.cancel()
}

// Wait waits for all consumer goroutines to finish.
func (c *KafkaConsumer) Wait() {
	c.wg.Wait()
}

// Close closes all Kafka readers.
func (c *KafkaConsumer) Close() error {
	var lastErr error
	for topic, reader := range c.readers {
		if err := reader.Close(); err != nil {
			c.logger.Error("failed to close reader",
				slog.String("topic", topic),
				slog.String("error", err.Error()),
			)
			lastErr = err
		}
	}
	return lastErr
}

// DLQMessage represents a message sent to the dead letter queue.
type DLQMessage struct {
	OriginalMessage *NotificationMessage `json:"originalMessage"`
	Error           string               `json:"error"`
	Attempts        int                  `json:"attempts"`
	FailedAt        string               `json:"failedAt"`
}

// KafkaDLQProducer publishes failed messages to the dead letter queue.
type KafkaDLQProducer struct {
	writer *kafka.Writer
	logger *slog.Logger
}

// NewKafkaDLQProducer creates a new DLQ producer.
func NewKafkaDLQProducer(brokers []string, dlqTopic string, logger *slog.Logger) *KafkaDLQProducer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        dlqTopic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	return &KafkaDLQProducer{
		writer: writer,
		logger: logger,
	}
}

// PublishToDLQ publishes a failed message to the DLQ.
func (p *KafkaDLQProducer) PublishToDLQ(ctx context.Context, msg *NotificationMessage, err error, attempts int) error {
	dlqMsg := DLQMessage{
		OriginalMessage: msg,
		Error:           err.Error(),
		Attempts:        attempts,
		FailedAt:        time.Now().UTC().Format(time.RFC3339),
	}

	value, marshalErr := json.Marshal(dlqMsg)
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal DLQ message: %w", marshalErr)
	}

	kafkaMsg := kafka.Message{
		Key:   []byte(msg.NotificationID),
		Value: value,
	}

	if writeErr := p.writer.WriteMessages(ctx, kafkaMsg); writeErr != nil {
		p.logger.Error("failed to publish to DLQ",
			slog.String("notification_id", msg.NotificationID),
			slog.String("error", writeErr.Error()),
		)
		return fmt.Errorf("failed to publish to DLQ: %w", writeErr)
	}

	p.logger.Info("message published to DLQ",
		slog.String("notification_id", msg.NotificationID),
		slog.Int("attempts", attempts),
	)

	return nil
}

// Close closes the DLQ producer.
func (p *KafkaDLQProducer) Close() error {
	return p.writer.Close()
}
