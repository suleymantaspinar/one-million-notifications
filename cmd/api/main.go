package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/notifications-management-api/internal/config"
	"github.com/notifications-management-api/internal/database"
	"github.com/notifications-management-api/internal/handler"
	"github.com/notifications-management-api/internal/kafka"
	"github.com/notifications-management-api/internal/repository"
	"github.com/notifications-management-api/internal/service"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("starting notification management API")

	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := database.NewPostgresDB(ctx, cfg.Database)
	if err != nil {
		logger.Error("failed to connect to database", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer db.Close()
	logger.Info("connected to PostgreSQL")

	kafkaProducer := kafka.NewProducer(cfg.Kafka, logger)
	defer kafkaProducer.Close()
	logger.Info("initialized Kafka producer")

	notificationRepo := repository.NewNotificationRepository(db.Pool)
	batchRepo := repository.NewBatchRepository(db.Pool)
	outboxRepo := repository.NewOutboxRepository(db.Pool)

	notificationService := service.NewNotificationService(
		db,
		notificationRepo,
		batchRepo,
		outboxRepo,
		kafkaProducer,
		logger,
	)

	notificationHandler := handler.NewNotificationHandler(notificationService, logger)
	healthHandler := handler.NewHealthHandler(db, kafkaProducer)
	swaggerHandler := handler.NewSwaggerHandler()

	r := chi.NewRouter()

	r.Use(handler.RecoveryMiddleware(logger))
	r.Use(handler.RequestIDMiddleware)
	r.Use(handler.LoggingMiddleware(logger))
	r.Use(handler.CORSMiddleware)

	r.Route("/v1", func(r chi.Router) {
		notificationHandler.RegisterRoutes(r)
	})
	healthHandler.RegisterRoutes(r)
	swaggerHandler.RegisterRoutes(r)

	server := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler:      r,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	go func() {
		logger.Info("starting HTTP server", slog.String("addr", server.Addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", slog.String("error", err.Error()))
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down server...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", slog.String("error", err.Error()))
	}

	logger.Info("server stopped")
}
