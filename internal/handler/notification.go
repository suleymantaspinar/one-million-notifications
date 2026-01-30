package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/notifications-management-api/internal/model"
	"github.com/notifications-management-api/internal/service"
)

type NotificationHandler struct {
	service *service.NotificationService
	logger  *slog.Logger
}

func NewNotificationHandler(service *service.NotificationService, logger *slog.Logger) *NotificationHandler {
	return &NotificationHandler{
		service: service,
		logger:  logger,
	}
}

func (h *NotificationHandler) RegisterRoutes(r chi.Router) {
	r.Route("/notifications", func(r chi.Router) {
		r.Post("/", h.CreateNotification)
		r.Get("/", h.ListNotifications)
		r.Post("/batch", h.CreateNotificationBatch)
		r.Get("/{messageId}", h.GetNotificationByID)
		r.Post("/{messageId}/cancel", h.CancelNotification)
		r.Get("/batches/{batchId}", h.GetBatchByID)
	})
}

func (h *NotificationHandler) CreateNotification(w http.ResponseWriter, r *http.Request) {
	var req model.CreateNotificationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "INVALID_JSON", "Invalid JSON payload")
		return
	}

	response, err := h.service.Create(r.Context(), req)
	if err != nil {
		h.handleServiceError(w, err)
		return
	}

	h.respondJSON(w, http.StatusCreated, response)
}

func (h *NotificationHandler) ListNotifications(w http.ResponseWriter, r *http.Request) {
	filter := model.NotificationFilter{
		Page:     1,
		PageSize: 20,
	}

	if status := r.URL.Query().Get("status"); status != "" {
		s := model.NotificationStatus(status)
		filter.Status = &s
	}
	if channel := r.URL.Query().Get("channel"); channel != "" {
		c := model.NotificationChannel(channel)
		filter.Channel = &c
	}
	if startDate := r.URL.Query().Get("startDate"); startDate != "" {
		if t, err := time.Parse(time.RFC3339, startDate); err == nil {
			filter.StartDate = &t
		}
	}
	if endDate := r.URL.Query().Get("endDate"); endDate != "" {
		if t, err := time.Parse(time.RFC3339, endDate); err == nil {
			filter.EndDate = &t
		}
	}
	if page := r.URL.Query().Get("page"); page != "" {
		if p, err := strconv.Atoi(page); err == nil && p > 0 {
			filter.Page = p
		}
	}
	if pageSize := r.URL.Query().Get("pageSize"); pageSize != "" {
		if ps, err := strconv.Atoi(pageSize); err == nil && ps > 0 && ps <= 100 {
			filter.PageSize = ps
		}
	}

	response, err := h.service.List(r.Context(), filter)
	if err != nil {
		h.handleServiceError(w, err)
		return
	}

	h.respondJSON(w, http.StatusOK, response)
}

func (h *NotificationHandler) CreateNotificationBatch(w http.ResponseWriter, r *http.Request) {
	var req model.CreateNotificationBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.respondError(w, http.StatusBadRequest, "INVALID_JSON", "Invalid JSON payload")
		return
	}

	response, err := h.service.CreateBatch(r.Context(), req)
	if err != nil {
		h.handleServiceError(w, err)
		return
	}

	h.respondJSON(w, http.StatusCreated, response)
}

func (h *NotificationHandler) GetNotificationByID(w http.ResponseWriter, r *http.Request) {
	messageIDStr := chi.URLParam(r, "messageId")
	messageID, err := uuid.Parse(messageIDStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "INVALID_ID", "Invalid message ID format")
		return
	}

	response, err := h.service.GetByID(r.Context(), messageID)
	if err != nil {
		h.handleServiceError(w, err)
		return
	}

	h.respondJSON(w, http.StatusOK, response)
}

func (h *NotificationHandler) CancelNotification(w http.ResponseWriter, r *http.Request) {
	messageIDStr := chi.URLParam(r, "messageId")
	messageID, err := uuid.Parse(messageIDStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "INVALID_ID", "Invalid message ID format")
		return
	}

	response, err := h.service.Cancel(r.Context(), messageID)
	if err != nil {
		h.handleServiceError(w, err)
		return
	}

	h.respondJSON(w, http.StatusOK, response)
}

func (h *NotificationHandler) GetBatchByID(w http.ResponseWriter, r *http.Request) {
	batchIDStr := chi.URLParam(r, "batchId")
	batchID, err := uuid.Parse(batchIDStr)
	if err != nil {
		h.respondError(w, http.StatusBadRequest, "INVALID_ID", "Invalid batch ID format")
		return
	}

	response, err := h.service.GetBatchByID(r.Context(), batchID)
	if err != nil {
		h.handleServiceError(w, err)
		return
	}

	h.respondJSON(w, http.StatusOK, response)
}

func (h *NotificationHandler) handleServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, model.ErrNotificationNotFound):
		h.respondError(w, http.StatusNotFound, "NOT_FOUND", "Notification not found")
	case errors.Is(err, model.ErrBatchNotFound):
		h.respondError(w, http.StatusNotFound, "NOT_FOUND", "Batch not found")
	case errors.Is(err, model.ErrInvalidRecipient):
		h.respondError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
	case errors.Is(err, model.ErrInvalidChannel):
		h.respondError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
	case errors.Is(err, model.ErrInvalidContent):
		h.respondError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
	case errors.Is(err, model.ErrContentTooLong):
		h.respondError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
	case errors.Is(err, model.ErrInvalidPriority):
		h.respondError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
	case errors.Is(err, model.ErrCannotCancel):
		h.respondError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
	case errors.Is(err, model.ErrEmptyBatch):
		h.respondError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
	case errors.Is(err, model.ErrBatchTooLarge):
		h.respondError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
	default:
		h.logger.Error("internal server error", slog.String("error", err.Error()))
		h.respondError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "An internal error occurred")
	}
}

func (h *NotificationHandler) respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		h.logger.Error("failed to encode response", slog.String("error", err.Error()))
	}
}

func (h *NotificationHandler) respondError(w http.ResponseWriter, status int, code, message string) {
	h.respondJSON(w, status, model.ErrorResponse{
		Code:    code,
		Message: message,
	})
}
