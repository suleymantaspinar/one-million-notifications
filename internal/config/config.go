package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all application configuration.
type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	Kafka    KafkaConfig
	Outbox   OutboxConfig
}

// OutboxConfig holds outbox cron configuration.
type OutboxConfig struct {
	PollInterval time.Duration
	BatchSize    int
	MaxRetries   int
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
			Brokers:      []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
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
