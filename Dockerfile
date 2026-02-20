# Build Stage
FROM golang:1.24-bookworm AS builder

WORKDIR /app

# Install build dependencies for DuckDB (C++)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libc6-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the application with CGO enabled
RUN CGO_ENABLED=1 GOOS=linux go build -o main cmd/server/main.go

# Runtime Stage
FROM debian:bookworm-slim

WORKDIR /app

# Install ca-certificates for HTTPS (Google Cloud API)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/main .

# Expose port
EXPOSE 80

# Command to run (expecting .env or env vars config)
CMD ["./main"]
