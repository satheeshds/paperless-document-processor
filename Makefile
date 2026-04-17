APP_NAME ?= paperless-document-processor
IMAGE ?= satheeshds/$(APP_NAME)
TAG ?= latest
DOCKER ?= docker
COMPOSE ?= docker compose
ENV_FILE ?= .env
COMMIT_SHA ?= $(shell git rev-parse --short HEAD)

export DOCKER_BUILDKIT ?= 1

.PHONY: help image push compose-up compose-down build test

help: ## List available targets
	@grep -E '^[a-zA-Z0-9_-]+:.*##' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*## "}; {printf "%-15s %s\n", $$1, $$2}'

image: ## Build the Docker image
	$(DOCKER) build --build-arg COMMIT_SHA=$(COMMIT_SHA) -t $(IMAGE):$(TAG) .

push: ## Push the Docker image
	$(DOCKER) push $(IMAGE):$(TAG)

compose-up: ## Start the stack with docker compose
	$(COMPOSE) --env-file $(ENV_FILE) up -d --build

compose-down: ## Stop the stack
	$(COMPOSE) down

build: ## Build the Go binary
	go build ./cmd/server

test: ## Run Go tests
	go test ./...
