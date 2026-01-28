package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/notifications-management-api/internal/config"
	"github.com/notifications-management-api/internal/consumer"
	"github.com/notifications-management-api/internal/database"
	"github.com/notifications-management-api/internal/model"
	"github.com/notifications-management-api/internal/redis"
	"github.com/notifications-management-api/internal/repository"
)

func main() {
	// Initialize logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("starting notification consumer service")

	// Load configuration
	cfg := config.Load()

	// Create root context for graceful shutdown
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

	// Connect to Redis
	redisClient, err := redis.NewClient(redis.Config{
		Host:         cfg.Redis.Host,
		Port:         cfg.Redis.Port,
		Password:     cfg.Redis.Password,
		DB:           cfg.Redis.DB,
		PoolSize:     cfg.Redis.PoolSize,
		MinIdleConns: cfg.Redis.MinIdleConns,
		DialTimeout:  cfg.Redis.DialTimeout,
		ReadTimeout:  cfg.Redis.ReadTimeout,
		WriteTimeout: cfg.Redis.WriteTimeout,
	}, logger)
	if err != nil {
		logger.Error("failed to connect to Redis", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer redisClient.Close()
	logger.Info("connected to Redis")

	// Initialize repositories
	notificationRepo := repository.NewNotificationRepository(db.Pool)

	// Initialize idempotency store
	idempotencyStore := redis.NewIdempotencyStore(
		redisClient,
		"notification:processed",
		cfg.Consumer.IdempotencyTTL,
		logger,
	)

	// Initialize DLQ producer
	dlqProducer := consumer.NewKafkaDLQProducer(
		cfg.Kafka.Brokers,
		cfg.Kafka.Topics.DeadLetter,
		logger,
	)
	defer dlqProducer.Close()
	logger.Info("initialized DLQ producer")

	// Initialize retry configuration
	retryConfig := consumer.RetryConfig{
		MaxRetries:     cfg.Consumer.MaxRetries,
		InitialBackoff: cfg.Consumer.InitialBackoff,
		MaxBackoff:     cfg.Consumer.MaxBackoff,
		Multiplier:     cfg.Consumer.BackoffMultiplier,
	}

	// Create rate limiters for each channel
	emailRateLimiter := consumer.NewRateLimiter(cfg.Consumer.EmailRateMax, cfg.Consumer.RateLimitInterval, logger)
	smsRateLimiter := consumer.NewRateLimiter(cfg.Consumer.SMSRateMax, cfg.Consumer.RateLimitInterval, logger)
	pushRateLimiter := consumer.NewRateLimiter(cfg.Consumer.PushRateMax, cfg.Consumer.RateLimitInterval, logger)

	logger.Info("rate limiters configured",
		slog.Int("email_max", cfg.Consumer.EmailRateMax),
		slog.Int("sms_max", cfg.Consumer.SMSRateMax),
		slog.Int("push_max", cfg.Consumer.PushRateMax),
		slog.Duration("interval", cfg.Consumer.RateLimitInterval),
	)

	// Create handler dependencies for each channel
	emailDeps := &consumer.HandlerDependencies{
		NotificationRepo: notificationRepo,
		IdempotencyStore: idempotencyStore,
		RateLimiter:      emailRateLimiter,
		Retryer:          consumer.NewRetryer(retryConfig, logger),
		DLQProducer:      dlqProducer,
		Logger:           logger,
	}

	smsDeps := &consumer.HandlerDependencies{
		NotificationRepo: notificationRepo,
		IdempotencyStore: idempotencyStore,
		RateLimiter:      smsRateLimiter,
		Retryer:          consumer.NewRetryer(retryConfig, logger),
		DLQProducer:      dlqProducer,
		Logger:           logger,
	}

	pushDeps := &consumer.HandlerDependencies{
		NotificationRepo: notificationRepo,
		IdempotencyStore: idempotencyStore,
		RateLimiter:      pushRateLimiter,
		Retryer:          consumer.NewRetryer(retryConfig, logger),
		DLQProducer:      dlqProducer,
		Logger:           logger,
	}

	// Create handlers for each channel
	emailHandler := consumer.NewEmailHandler(cfg.Consumer.WebhookURL, cfg.Consumer.HTTPTimeout, emailDeps)
	smsHandler := consumer.NewSMSHandler(cfg.Consumer.WebhookURL, cfg.Consumer.HTTPTimeout, smsDeps)
	pushHandler := consumer.NewPushHandler(cfg.Consumer.WebhookURL, cfg.Consumer.HTTPTimeout, pushDeps)

	// Initialize worker pool manager (Fan-Out Pattern)
	poolManager := consumer.NewWorkerPoolManager(logger)

	// Create worker pools for each channel
	emailPool := consumer.NewWorkerPool(
		"email-pool",
		model.ChannelEmail,
		cfg.Consumer.EmailWorkers,
		cfg.Consumer.QueueSize,
		emailHandler,
		logger,
	)

	smsPool := consumer.NewWorkerPool(
		"sms-pool",
		model.ChannelSMS,
		cfg.Consumer.SMSWorkers,
		cfg.Consumer.QueueSize,
		smsHandler,
		logger,
	)

	pushPool := consumer.NewWorkerPool(
		"push-pool",
		model.ChannelPush,
		cfg.Consumer.PushWorkers,
		cfg.Consumer.QueueSize,
		pushHandler,
		logger,
	)

	poolManager.AddPool(model.ChannelEmail, emailPool)
	poolManager.AddPool(model.ChannelSMS, smsPool)
	poolManager.AddPool(model.ChannelPush, pushPool)

	// Start all worker pools
	poolManager.StartAll()
	logger.Info("worker pools started",
		slog.Int("email_workers", cfg.Consumer.EmailWorkers),
		slog.Int("sms_workers", cfg.Consumer.SMSWorkers),
		slog.Int("push_workers", cfg.Consumer.PushWorkers),
	)

	// Initialize Kafka consumer
	kafkaConsumer := consumer.NewKafkaConsumer(
		cfg.Kafka,
		cfg.Consumer.ConsumerGroup,
		poolManager,
		logger,
	)

	// Subscribe to topics
	if err := kafkaConsumer.Subscribe(); err != nil {
		logger.Error("failed to subscribe to Kafka topics", slog.String("error", err.Error()))
		os.Exit(1)
	}

	// Start consuming
	kafkaConsumer.Start()
	logger.Info("Kafka consumer started",
		slog.String("consumer_group", cfg.Consumer.ConsumerGroup),
	)

	// Wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("notification consumer is running, press Ctrl+C to stop")

	<-quit
	logger.Info("shutdown signal received, initiating graceful shutdown...")

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Consumer.ShutdownTimeout)
	defer shutdownCancel()

	// Phase 1: Stop accepting new messages
	logger.Info("phase 1: stopping Kafka consumer...")
	kafkaConsumer.Stop()

	// Wait for consumer to stop with timeout
	consumerDone := make(chan struct{})
	go func() {
		kafkaConsumer.Wait()
		close(consumerDone)
	}()

	select {
	case <-consumerDone:
		logger.Info("Kafka consumer stopped")
	case <-shutdownCtx.Done():
		logger.Warn("timeout waiting for Kafka consumer to stop")
	}

	// Phase 2: Drain in-flight messages
	logger.Info("phase 2: draining worker pools...")

	// Stop worker pools (close job channels)
	poolManager.StopAll()

	// Wait for workers to finish with remaining timeout
	deadline, ok := shutdownCtx.Deadline()
	if ok {
		remainingTimeout := time.Until(deadline)
		if remainingTimeout > 0 {
			poolManager.DrainAllWithTimeout(remainingTimeout)
		}
	}

	// Phase 3: Close connections
	logger.Info("phase 3: closing connections...")

	if err := kafkaConsumer.Close(); err != nil {
		logger.Error("error closing Kafka consumer", slog.String("error", err.Error()))
	}

	// Cancel main context
	cancel()

	logger.Info("notification consumer stopped gracefully")
}
