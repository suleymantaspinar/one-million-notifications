.PHONY: build run test lint clean docker-build docker-up docker-down migrate

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOMOD=$(GOCMD) mod
BINARY_NAME=api
BUILD_DIR=./build

# Build the application
build:
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/api

# Run the application locally
run:
	$(GOCMD) run ./cmd/api

# Run tests
test:
	$(GOTEST) -v -race ./...

# Run tests with coverage
test-coverage:
	$(GOTEST) -v -race -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

# Run linter
lint:
	golangci-lint run ./...

# Format code
fmt:
	gofmt -s -w .

# Download dependencies
deps:
	$(GOMOD) download
	$(GOMOD) tidy

# Clean build artifacts
clean:
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html

# Build Docker image
docker-build:
	docker build -t notifications-api:latest .

# Start all services with Docker Compose
docker-up:
	docker-compose up -d

# Stop all services
docker-down:
	docker-compose down

# View logs
docker-logs:
	docker-compose logs -f

# Restart API service
docker-restart-api:
	docker-compose restart api

# Run database migrations (requires migrate tool)
migrate-up:
	migrate -path ./migrations -database "postgres://postgres:postgres@localhost:5432/notifications?sslmode=disable" up

migrate-down:
	migrate -path ./migrations -database "postgres://postgres:postgres@localhost:5432/notifications?sslmode=disable" down

# Development helpers
dev-setup: deps
	@echo "Installing development tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Health check
health:
	curl -s http://localhost:8080/health | jq .

# Open API documentation
docs:
	@echo "Opening Swagger UI at http://localhost:8080/docs"
	@open http://localhost:8080/docs 2>/dev/null || xdg-open http://localhost:8080/docs 2>/dev/null || echo "Visit http://localhost:8080/docs"

# Test API endpoints
test-create:
	curl -X POST http://localhost:8080/v1/notifications \
		-H "Content-Type: application/json" \
		-d '{"to": "user@example.com", "channel": "email", "content": "Test notification", "priority": "normal"}' | jq .

test-list:
	curl -s "http://localhost:8080/v1/notifications?page=1&pageSize=10" | jq .

test-batch:
	curl -X POST http://localhost:8080/v1/notifications/batch \
		-H "Content-Type: application/json" \
		-d '{"notifications": [{"to": "user1@example.com", "channel": "email", "content": "Batch notification 1"}, {"to": "user2@example.com", "channel": "sms", "content": "Batch notification 2"}]}' | jq .
