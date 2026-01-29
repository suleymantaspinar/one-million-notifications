package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all application configuration.
type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	Kafka    KafkaConfig
	Outbox   OutboxConfig
	Consumer ConsumerConfig
}

// OutboxConfig holds outbox cron configuration.
type OutboxConfig struct {
	PollInterval time.Duration
	BatchSize    int
	MaxRetries   int
}

// ConsumerConfig holds notification consumer configuration.
type ConsumerConfig struct {
	// Consumer group ID
	ConsumerGroup string

	// Webhook URL for external notification provider
	WebhookURL string

	// HTTP timeout for webhook requests
	HTTPTimeout time.Duration

	// Worker pool configuration per channel
	EmailWorkers int
	SMSWorkers   int
	PushWorkers  int

	// Queue size per worker pool
	QueueSize int

	// Rate limiting: maximum messages per second per channel
	RateLimitPerSecond int

	// Retry configuration
	MaxRetries        int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64

	// Idempotency TTL (how long to remember processed messages)
	IdempotencyTTL time.Duration

	// Graceful shutdown timeout
	ShutdownTimeout time.Duration
}


// ServerConfig holds HTTP server configuration.
type ServerConfig struct {
	Host         string
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// DatabaseConfig holds PostgreSQL configuration.
type DatabaseConfig struct {
	Host            string
	Port            int
	User            string
	Password        string
	Database        string
	SSLMode         string
	MaxConns        int
	MinConns        int
	MaxConnLifetime time.Duration
}

// KafkaConfig holds Kafka configuration.
type KafkaConfig struct {
	Brokers      []string
	Topics       KafkaTopics
	WriteTimeout time.Duration
	ReadTimeout  time.Duration
}

// KafkaTopics holds all Kafka topic names.
type KafkaTopics struct {
	EmailHigh    string
	EmailNormal  string
	EmailLow     string
	SMSHigh      string
	SMSNormal    string
	SMSLow       string
	PushHigh     string
	PushNormal   string
	PushLow      string
	DeadLetter   string
}

// Load loads configuration from environment variables.
func Load() *Config {
	return &Config{
		Server: ServerConfig{
			Host:         getEnv("SERVER_HOST", "0.0.0.0"),
			Port:         getEnvAsInt("SERVER_PORT", 8080),
			ReadTimeout:  getEnvAsDuration("SERVER_READ_TIMEOUT", 10*time.Second),
			WriteTimeout: getEnvAsDuration("SERVER_WRITE_TIMEOUT", 10*time.Second),
		},
		Database: DatabaseConfig{
			Host:            getEnv("DB_HOST", "localhost"),
			Port:            getEnvAsInt("DB_PORT", 5432),
			User:            getEnv("DB_USER", "postgres"),
			Password:        getEnv("DB_PASSWORD", "postgres"),
			Database:        getEnv("DB_NAME", "notifications"),
			SSLMode:         getEnv("DB_SSL_MODE", "disable"),
			MaxConns:        getEnvAsInt("DB_MAX_CONNS", 25),
			MinConns:        getEnvAsInt("DB_MIN_CONNS", 5),
			MaxConnLifetime: getEnvAsDuration("DB_MAX_CONN_LIFETIME", time.Hour),
		},
		Kafka: KafkaConfig{
			Brokers:      getEnvAsStringSlice("KAFKA_BROKERS", "localhost:9092"),
			WriteTimeout: getEnvAsDuration("KAFKA_WRITE_TIMEOUT", 10*time.Second),
			ReadTimeout:  getEnvAsDuration("KAFKA_READ_TIMEOUT", 10*time.Second),
			Topics: KafkaTopics{
				EmailHigh:    getEnv("KAFKA_TOPIC_EMAIL_HIGH", "notifications.email.high"),
				EmailNormal:  getEnv("KAFKA_TOPIC_EMAIL_NORMAL", "notifications.email.normal"),
				EmailLow:     getEnv("KAFKA_TOPIC_EMAIL_LOW", "notifications.email.low"),
				SMSHigh:      getEnv("KAFKA_TOPIC_SMS_HIGH", "notifications.sms.high"),
				SMSNormal:    getEnv("KAFKA_TOPIC_SMS_NORMAL", "notifications.sms.normal"),
				SMSLow:       getEnv("KAFKA_TOPIC_SMS_LOW", "notifications.sms.low"),
				PushHigh:     getEnv("KAFKA_TOPIC_PUSH_HIGH", "notifications.push.high"),
				PushNormal:   getEnv("KAFKA_TOPIC_PUSH_NORMAL", "notifications.push.normal"),
				PushLow:      getEnv("KAFKA_TOPIC_PUSH_LOW", "notifications.push.low"),
				DeadLetter:   getEnv("KAFKA_TOPIC_DLQ", "notifications.dlq"),
			},
		},
		Outbox: OutboxConfig{
			PollInterval: getEnvAsDuration("OUTBOX_POLL_INTERVAL", 5*time.Second),
			BatchSize:    getEnvAsInt("OUTBOX_BATCH_SIZE", 100),
			MaxRetries:   getEnvAsInt("OUTBOX_MAX_RETRIES", 3),
		},
		Consumer: ConsumerConfig{
			ConsumerGroup:      getEnv("CONSUMER_GROUP", "notification-consumer"),
			WebhookURL:         getEnv("WEBHOOK_URL", "https://webhook.site/eb377dd8-129d-4fdd-9a83-e5f83314141d"),
			HTTPTimeout:        getEnvAsDuration("HTTP_TIMEOUT", 30*time.Second),
			EmailWorkers:       getEnvAsInt("EMAIL_WORKERS", 5),
			SMSWorkers:         getEnvAsInt("SMS_WORKERS", 5),
			PushWorkers:        getEnvAsInt("PUSH_WORKERS", 5),
			QueueSize:          getEnvAsInt("QUEUE_SIZE", 100),
			RateLimitPerSecond: getEnvAsInt("RATE_LIMIT_PER_SECOND", 100), // 100 msg/sec per channel
			MaxRetries:         getEnvAsInt("MAX_RETRIES", 3),
			InitialBackoff:     getEnvAsDuration("INITIAL_BACKOFF", 1*time.Second),
			MaxBackoff:         getEnvAsDuration("MAX_BACKOFF", 30*time.Second),
			BackoffMultiplier:  getEnvAsFloat("BACKOFF_MULTIPLIER", 2.0),
			IdempotencyTTL:     getEnvAsDuration("IDEMPOTENCY_TTL", 24*time.Hour),
			ShutdownTimeout:    getEnvAsDuration("SHUTDOWN_TIMEOUT", 30*time.Second),
		},
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func getEnvAsFloat(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
			return floatValue
		}
	}
	return defaultValue
}

func getEnvAsStringSlice(key string, defaultValue string) []string {
	if value := os.Getenv(key); value != "" {
		return strings.Split(value, ",")
	}
	return strings.Split(defaultValue, ",")
}
