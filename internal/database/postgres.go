package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/notifications-management-api/internal/config"
)

// DBTX interface for both pool and transaction.
type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

// PostgresDB wraps the pgx pool.
type PostgresDB struct {
	Pool *pgxpool.Pool
}

// NewPostgresDB creates a new PostgreSQL connection pool.
func NewPostgresDB(ctx context.Context, cfg config.DatabaseConfig) (*PostgresDB, error) {
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
		cfg.SSLMode,
	)

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	poolConfig.MaxConns = int32(cfg.MaxConns)
	poolConfig.MinConns = int32(cfg.MinConns)
	poolConfig.MaxConnLifetime = cfg.MaxConnLifetime

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &PostgresDB{Pool: pool}, nil
}

// Close closes the database connection pool.
func (db *PostgresDB) Close() {
	if db.Pool != nil {
		db.Pool.Close()
	}
}

// Health checks if the database is healthy.
func (db *PostgresDB) Health(ctx context.Context) error {
	return db.Pool.Ping(ctx)
}

// BeginTx starts a new transaction.
func (db *PostgresDB) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return db.Pool.Begin(ctx)
}
