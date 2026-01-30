package consumer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/notifications-management-api/internal/model"
)

// WorkerPool manages a pool of workers for processing notifications.
type WorkerPool struct {
	name       string
	channel    model.NotificationChannel
	numWorkers int
	jobs       chan *Job
	handler    NotificationHandler
	logger     *slog.Logger
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

// Job represents a single notification processing job.
type Job struct {
	Message   *NotificationMessage
	Partition int
	Offset    int64
	Topic     string
}

// NewWorkerPool creates a new worker pool for a specific notification channel.
func NewWorkerPool(
	name string,
	channel model.NotificationChannel,
	numWorkers int,
	queueSize int,
	handler NotificationHandler,
	logger *slog.Logger,
) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerPool{
		name:       name,
		channel:    channel,
		numWorkers: numWorkers,
		jobs:       make(chan *Job, queueSize),
		handler:    handler,
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start starts all workers in the pool.
func (p *WorkerPool) Start() {
	p.logger.Info("starting worker pool",
		slog.String("pool", p.name),
		slog.String("channel", string(p.channel)),
		slog.Int("workers", p.numWorkers),
	)

	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

// SubmitWithTimeout submits a job with a timeout.
func (p *WorkerPool) SubmitWithTimeout(job *Job, timeout time.Duration) bool {
	select {
	case <-p.ctx.Done():
		return false
	case p.jobs <- job:
		return true
	case <-time.After(timeout):
		p.logger.Warn("worker pool submit timed out",
			slog.String("pool", p.name),
			slog.String("notification_id", job.Message.NotificationID),
		)
		return false
	}
}

// Stop stops all workers gracefully.
func (p *WorkerPool) Stop() {
	p.logger.Info("stopping worker pool",
		slog.String("pool", p.name),
	)
	p.cancel()
	close(p.jobs)
}

// Wait waits for all workers to finish.
func (p *WorkerPool) Wait() {
	p.wg.Wait()
	p.logger.Info("worker pool stopped",
		slog.String("pool", p.name),
	)
}

// DrainWithTimeout drains remaining jobs with a timeout.
func (p *WorkerPool) DrainWithTimeout(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		p.logger.Info("worker pool drained successfully",
			slog.String("pool", p.name),
		)
	case <-time.After(timeout):
		p.logger.Warn("worker pool drain timed out",
			slog.String("pool", p.name),
			slog.Duration("timeout", timeout),
		)
	}
}

// worker is a single worker goroutine.
func (p *WorkerPool) worker(id int) {
	defer p.wg.Done()

	p.logger.Debug("worker started",
		slog.String("pool", p.name),
		slog.Int("worker_id", id),
	)

	for {
		select {
		case <-p.ctx.Done():
			// Context cancelled, but still process remaining jobs
			p.drainRemainingJobs(id)
			return
		case job, ok := <-p.jobs:
			if !ok {
				// Channel closed
				p.logger.Debug("worker stopping, channel closed",
					slog.String("pool", p.name),
					slog.Int("worker_id", id),
				)
				return
			}
			p.processJob(id, job)
		}
	}
}

// drainRemainingJobs processes any remaining jobs after context cancellation.
func (p *WorkerPool) drainRemainingJobs(workerID int) {
	for job := range p.jobs {
		p.processJob(workerID, job)
	}
}

// processJob processes a single job.
func (p *WorkerPool) processJob(workerID int, job *Job) {
	startTime := time.Now()

	p.logger.Debug("processing job",
		slog.String("pool", p.name),
		slog.Int("worker_id", workerID),
		slog.String("notification_id", job.Message.NotificationID),
	)

	// Create a context with timeout for processing
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Call handler.Process directly - it handles everything
	err := p.handler.Process(ctx, job.Message)

	duration := time.Since(startTime)

	if err != nil {
		p.logger.Error("job processing failed",
			slog.String("pool", p.name),
			slog.Int("worker_id", workerID),
			slog.String("notification_id", job.Message.NotificationID),
			slog.Duration("duration", duration),
			slog.String("error", err.Error()),
		)
		return
	}

	p.logger.Debug("job processed successfully",
		slog.String("pool", p.name),
		slog.Int("worker_id", workerID),
		slog.String("notification_id", job.Message.NotificationID),
		slog.Duration("duration", duration),
	)
}

// WorkerPoolManager manages multiple worker pools (fan-out pattern).
type WorkerPoolManager struct {
	pools  map[model.NotificationChannel]*WorkerPool
	logger *slog.Logger
	mu     sync.RWMutex
}

// NewWorkerPoolManager creates a new worker pool manager.
func NewWorkerPoolManager(logger *slog.Logger) *WorkerPoolManager {
	return &WorkerPoolManager{
		pools:  make(map[model.NotificationChannel]*WorkerPool),
		logger: logger,
	}
}

// AddPool adds a worker pool for a specific channel.
func (m *WorkerPoolManager) AddPool(channel model.NotificationChannel, pool *WorkerPool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pools[channel] = pool
}

// GetPool returns the worker pool for a channel.
func (m *WorkerPoolManager) GetPool(channel model.NotificationChannel) (*WorkerPool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	pool, ok := m.pools[channel]
	return pool, ok
}

// StartAll starts all worker pools.
func (m *WorkerPoolManager) StartAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, pool := range m.pools {
		pool.Start()
	}
}

// StopAll stops all worker pools.
func (m *WorkerPoolManager) StopAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, pool := range m.pools {
		pool.Stop()
	}
}

// DrainAllWithTimeout drains all worker pools with a timeout.
func (m *WorkerPoolManager) DrainAllWithTimeout(timeout time.Duration) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var wg sync.WaitGroup
	for _, pool := range m.pools {
		wg.Add(1)
		go func(p *WorkerPool) {
			defer wg.Done()
			p.DrainWithTimeout(timeout)
		}(pool)
	}
	wg.Wait()
}
