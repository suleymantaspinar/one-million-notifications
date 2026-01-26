package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

// HealthChecker defines the interface for health checking.
type HealthChecker interface {
	Health(ctx context.Context) error
}

// HealthHandler handles health check endpoints.
type HealthHandler struct {
	db    HealthChecker
	kafka HealthChecker
}

// NewHealthHandler creates a new HealthHandler.
func NewHealthHandler(db, kafka HealthChecker) *HealthHandler {
	return &HealthHandler{
		db:    db,
		kafka: kafka,
	}
}

// HealthResponse represents the health check response.
type HealthResponse struct {
	Status     string            `json:"status"`
	Components map[string]string `json:"components"`
	Timestamp  time.Time         `json:"timestamp"`
}

// RegisterRoutes registers the health routes.
func (h *HealthHandler) RegisterRoutes(r chi.Router) {
	r.Get("/health", h.Health)
	r.Get("/health/live", h.Liveness)
	r.Get("/health/ready", h.Readiness)
}

// Health handles GET /health - comprehensive health check.
func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	response := HealthResponse{
		Status:     "healthy",
		Components: make(map[string]string),
		Timestamp:  time.Now(),
	}

	// Check database
	if err := h.db.Health(ctx); err != nil {
		response.Status = "unhealthy"
		response.Components["database"] = "unhealthy: " + err.Error()
	} else {
		response.Components["database"] = "healthy"
	}

	// Check Kafka
	if err := h.kafka.Health(ctx); err != nil {
		response.Status = "unhealthy"
		response.Components["kafka"] = "unhealthy: " + err.Error()
	} else {
		response.Components["kafka"] = "healthy"
	}

	status := http.StatusOK
	if response.Status == "unhealthy" {
		status = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(response)
}

// Liveness handles GET /health/live - basic liveness probe.
func (h *HealthHandler) Liveness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "alive"})
}

// Readiness handles GET /health/ready - readiness probe.
func (h *HealthHandler) Readiness(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Check if dependencies are ready
	if err := h.db.Health(ctx); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"status": "not ready", "reason": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
}
