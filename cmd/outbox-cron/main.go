package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/notifications-management-api/internal/config"
	"github.com/notifications-management-api/internal/database"
	"github.com/notifications-management-api/internal/kafka"
	"github.com/notifications-management-api/internal/model"
	"github.com/notifications-management-api/internal/repository"
)

// OutboxProcessor processes outbox events and publishes them to Kafka.
type OutboxProcessor struct {
	outboxRepo    *repository.OutboxRepository
	kafkaProducer *kafka.Producer
	cfg           config.OutboxConfig
	logger        *slog.Logger
}

// NewOutboxProcessor creates a new OutboxProcessor.
func NewOutboxProcessor(
	outboxRepo *repository.OutboxRepository,
	kafkaProducer *kafka.Producer,
	cfg config.OutboxConfig,
	logger *slog.Logger,
) *OutboxProcessor {
	return &OutboxProcessor{
		outboxRepo:    outboxRepo,
		kafkaProducer: kafkaProducer,
		cfg:           cfg,
		logger:        logger,
	}
}

// KafkaMessage represents the message structure sent to Kafka.
type KafkaMessage struct {
	NotificationID string `json:"notificationId"`
	Recipient      string `json:"recipient"`
	Channel        string `json:"channel"`
	Content        string `json:"content"`
	Priority       string `json:"priority"`
	Timestamp      string `json:"timestamp"`
}

// Run starts the outbox processor loop.
func (p *OutboxProcessor) Run(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	p.logger.Info("outbox processor started",
		slog.Duration("poll_interval", p.cfg.PollInterval),
		slog.Int("batch_size", p.cfg.BatchSize),
	)

	// Process immediately on startup
	p.processEvents(ctx)

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("outbox processor shutting down")
			return
		case <-ticker.C:
			p.processEvents(ctx)
		}
	}
}

// processEvents fetches and processes outbox events.
func (p *OutboxProcessor) processEvents(ctx context.Context) {
	events, err := p.outboxRepo.GetUnprocessed(ctx, p.cfg.BatchSize)
	if err != nil {
		p.logger.Error("failed to fetch outbox events",
			slog.String("error", err.Error()),
		)
		return
	}

	if len(events) == 0 {
		p.logger.Debug("no outbox events to process")
		return
	}

	p.logger.Info("processing outbox events",
		slog.Int("count", len(events)),
	)

	var successCount, failCount int

	for _, event := range events {
		if err := p.publishEvent(ctx, event); err != nil {
			p.logger.Error("failed to publish event, marking as failed",
				slog.String("event_id", event.ID.String()),
				slog.String("notification_id", event.NotificationID.String()),
				slog.String("error", err.Error()),
			)

			// Mark as failed immediately (no retry)
			if err := p.outboxRepo.MarkFailed(ctx, event.ID); err != nil {
				if errors.Is(err, repository.ErrOutboxEventNotFound) {
					p.logger.Warn("outbox event not found when marking as failed",
						slog.String("event_id", event.ID.String()),
					)
				} else {
					p.logger.Error("failed to mark event as failed",
						slog.String("event_id", event.ID.String()),
						slog.String("error", err.Error()),
					)
				}
			}

			failCount++
			continue
		}

		// Mark as processed (sent/published)
		if err := p.outboxRepo.MarkProcessed(ctx, event.ID); err != nil {
			if errors.Is(err, repository.ErrOutboxEventNotFound) {
				p.logger.Warn("outbox event not found when marking as processed",
					slog.String("event_id", event.ID.String()),
				)
			} else {
				p.logger.Error("failed to mark event as processed",
					slog.String("event_id", event.ID.String()),
					slog.String("error", err.Error()),
				)
			}
			failCount++
			continue
		}

		p.logger.Info("event published successfully",
			slog.String("event_id", event.ID.String()),
			slog.String("notification_id", event.NotificationID.String()),
			slog.String("channel", string(event.Channel)),
			slog.String("priority", string(event.Priority)),
		)

		successCount++
	}

	p.logger.Info("batch processing completed",
		slog.Int("total", len(events)),
		slog.Int("success", successCount),
		slog.Int("failed", failCount),
	)
}

// publishEvent publishes a single outbox event to Kafka.
func (p *OutboxProcessor) publishEvent(ctx context.Context, event model.OutboxEvent) error {
	topic := p.kafkaProducer.GetTopicForNotification(event.Channel, event.Priority)

	msg := KafkaMessage{
		NotificationID: event.NotificationID.String(),
		Recipient:      event.Recipient,
		Channel:        string(event.Channel),
		Content:        event.Content,
		Priority:       string(event.Priority),
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
	}

	value, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return p.kafkaProducer.Publish(ctx, topic, []byte(event.NotificationID.String()), value)
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("starting outbox cron service")

	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to database
	db, err := database.NewPostgresDB(ctx, cfg.Database)
	if err != nil {
		logger.Error("failed to connect to database", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer db.Close()
	logger.Info("connected to PostgreSQL")

	// Initialize Kafka producer
	kafkaProducer := kafka.NewProducer(cfg.Kafka, logger)
	defer kafkaProducer.Close()
	logger.Info("initialized Kafka producer")

	// Initialize repository
	outboxRepo := repository.NewOutboxRepository(db.Pool)

	// Create processor
	processor := NewOutboxProcessor(outboxRepo, kafkaProducer, cfg.Outbox, logger)

	// Run processor in background
	go processor.Run(ctx)

	// Wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down outbox cron service...")
	cancel()

	// Give some time for graceful shutdown
	time.Sleep(2 * time.Second)
	logger.Info("outbox cron service stopped")
}
