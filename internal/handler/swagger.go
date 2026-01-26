package handler

import (
	_ "embed"
	"html/template"
	"net/http"

	"github.com/go-chi/chi/v5"
)

//go:embed swagger_ui.html
var swaggerUIHTML string

//go:embed openapi.yaml
var openapiSpec []byte

// SwaggerHandler serves the Swagger UI and OpenAPI spec.
type SwaggerHandler struct{}

// NewSwaggerHandler creates a new SwaggerHandler.
func NewSwaggerHandler() *SwaggerHandler {
	return &SwaggerHandler{}
}

// RegisterRoutes registers the swagger routes.
func (h *SwaggerHandler) RegisterRoutes(r chi.Router) {
	r.Get("/docs", h.SwaggerUI)
	r.Get("/docs/openapi.yaml", h.OpenAPISpec)
}

// SwaggerUI serves the Swagger UI HTML page.
func (h *SwaggerHandler) SwaggerUI(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.New("swagger").Parse(swaggerUIHTML)
	if err != nil {
		http.Error(w, "Failed to load Swagger UI", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	tmpl.Execute(w, nil)
}

// OpenAPISpec serves the OpenAPI specification file.
func (h *SwaggerHandler) OpenAPISpec(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-yaml")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(openapiSpec)
}
