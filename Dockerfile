# syntax=docker/dockerfile:1

# Build Stage
FROM golang:1.24-bookworm AS builder

ARG COMMIT_SHA=unknown

WORKDIR /app

# Install build dependencies for DuckDB (C++)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# Build the application with CGO enabled
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 GOOS=linux go build -o main cmd/server/main.go

# Runtime Stage
FROM debian:bookworm-slim

ARG COMMIT_SHA=unknown
WORKDIR /app

# Install ca-certificates for HTTPS (Google Cloud API)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

LABEL watchlog.project-repo="satheeshds/paperless-document-processor"
LABEL watchlog.commit-sha="${COMMIT_SHA}"
LABEL watchlog.monitor="true"
LABEL watchlog.service="paperless-document-processor"

# Copy binary from builder
COPY --from=builder /app/main .

# Expose port
EXPOSE 80

# Command to run (expecting .env or env vars config)
CMD ["./main"]
