# 📬 Notification Service

A scalable, event-driven notification service built with Go, Kafka, and Docker. Supports multi-channel notifications (email, SMS, push) with priority-based processing and reliable delivery guarantees.

## ✨ Features

- **Multi-Channel Support**: Email, SMS, and Push notifications
- **Priority-Based Processing**: High, normal, and low priority queues
- **Reliable Delivery**: Outbox pattern ensures no message loss
- **Scalable Architecture**: Kafka-based event streaming
- **Health Monitoring**: Liveness and readiness probes
- **Idempotency**: Prevents duplicate notification processing
- **Dead Letter Queue**: Automatic handling of failed messages

---

## 🚀 Quick Start

### 1️⃣ Prerequisites

- Docker & Docker Compose
- Go 1.21+ (for local development)

### 2️⃣ Create Environment File

```bash
cp .env.example .env
```

### 3️⃣ Run the Project Locally

```bash
docker compose up --build
```

### 4️⃣ Verify Services

| Service | URL |
|---------|-----|
| API | http://localhost:8080 |
| Swagger Docs | http://localhost:8080/docs |
| Health (Liveness) | http://localhost:8080/health/live |
| Health (Readiness) | http://localhost:8080/health/ready |
| Redpanda Console | http://localhost:8081 |

---

## 🔌 API Documentation

### Swagger UI

Interactive API documentation is available at:

```
GET /docs
```

### Create Notification

**Endpoint**: `POST /v1/notifications`

**Request Body**:
```json
{
  "to": "user@example.com",
  "channel": "email",
  "priority": "high",
  "content": "Your order has shipped!"
}
```

**Response**:
```json
{
  "id": "uuid",
  "status": "queued"
}
```

---

## ❤️ Health Endpoints

| Endpoint | Description |
|----------|-------------|
| `/health/live` | Checks if the service is running (liveness probe) |
| `/health/ready` | Checks if dependencies (DB, Kafka) are ready |

---

## 📨 Kafka Topics

| Topic | Purpose |
|-------|---------|
| `notifications.email.high` | High priority email notifications |
| `notifications.email.normal` | Normal priority email notifications |
| `notifications.email.low` | Low priority email notifications |
| `notifications.sms.high` | High priority SMS notifications |
| `notifications.sms.normal` | Normal priority SMS notifications |
| `notifications.sms.low` | Low priority SMS notifications |
| `notifications.push.high` | High priority push notifications |
| `notifications.push.normal` | Normal priority push notifications |
| `notifications.push.low` | Low priority push notifications |
| `notifications.dlq` | Dead Letter Queue for failed messages |

---

## 🔄 Processing Model

### Rate Limiting
Maximum messages per second per channel.

### Retry Strategy
- Exponential backoff
- Maximum retry attempts
- Messages moved to DLQ on failure

### Idempotency
Prevents duplicate notification processing.

---

## 📊 Observability (Planned)

- Metrics (queue depth, success/failure rates)
- Logging
- Distributed tracing

---

## 🧪 Testing

Run Tests:

```bash
go test ./...
```

---

## 📁 Project Structure

```
/cmd                  # Application entry points
/internal             # Private application code
  /api                # REST API handlers
  /consumer           # Kafka consumers
  /outbox             # Outbox pattern implementation
  /db                 # Database layer
  /kafka              # Kafka client
/migrations           # Database migrations
/docker               # Docker configurations
```

---

## 🚀 Scaling

- Increase consumer replicas
- Partition Kafka topics
- Tune worker pool sizes

---

## 🛑 Failure Handling

- **Retries**: Automatic retry with exponential backoff
- **Backoff**: Configurable backoff strategy
- **DLQ Strategy**: Failed messages route to Dead Letter Queue

---

## 🔐 Configuration

Environment variables are documented in:

```
.env.example
```

---

## 🧠 Design Decisions

- **Outbox Pattern**: Ensures reliable event publishing
- **Kafka**: Provides scalable event streaming
- **Idempotency**: Prevents duplicate processing
- **Worker Pool per Channel**: Allows independent scaling

---

## 📌 Future Improvements

- [ ] Circuit breakers
- [ ] Per-tenant rate limits
- [ ] UI dashboard
- [ ] Metrics export
- [ ] Github Actions Integrations
- [ ] %100 Test Coverage
---
