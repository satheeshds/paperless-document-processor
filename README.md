# Paperless-ngx Document Processor

This Go service integrates Paperless-ngx with Google Document AI to provide superior OCR and metadata extraction for your invoices.

## Test Coverage

Unit tests are provided for all major packages. Run the full suite and generate a coverage report with:

```bash
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out   # open an HTML coverage report in your browser
```

Current coverage by package:

| Package | Coverage |
|---|---|
| `config` | 100% |
| `pkg/tika` | ~91% |
| `pkg/accounting` | ~82% |
| `pkg/libreoffice` | ~82% |
| `pkg/excel` | ~86% |
| `pkg/paperless` | ~78% |
| `pkg/docai` | ~40% |
| `cmd/server` | ~17% |
| **Overall** | **~44%** |

> `pkg/storage` functions that require a live DuckDB connection are exercised through integration testing; the pure helper `marshalOrderedRows` is unit-tested directly.

## Features

- **Google Document AI OCR**: Uses Google's powerful AI models to extract text and entities (Date, Total, Supplier, invoice line items).
- **Automatic Metadata Updates**: Updates the document in Paperless with:
    - Extracted Content (OCR text)
    - Correspondent (Supplier Name)
    - Custom Fields (e.g., Invoice Date, Total Amount)
- **Raw Data Storage**: Saves the full Google Document AI response and extracted metadata to a local DuckDB database (`duck.db`).
- **Dynamic Configuration**: Automatically maps extracted entities to Paperless Custom Fields by name.

## Setup

### 1. Prerequisites

- **Go**: Ensure Go is installed (1.20+).
- **Google Cloud Project**:
    - Enable Document AI API.
    - Create a Processor (Invoice Parser recommended).
    - Create a Service Account and download the JSON key.
- **Paperless-ngx**:
    - Create an API Token.
    - Ensure Custom Fields exist with names like "Invoice Date", "Total", "Net Amount", "Currency", "Invoice Number".

### 2. Configuration

1.  Copy `.env.example` to `.env`.
2.  Fill in the values:

```env
# Paperless
PAPERLESS_URL=http://your-paperless:8000
PAPERLESS_TOKEN=your-token

# Google Cloud
GOOGLE_CLOUD_PROJECT=your-project-id
GOOGLE_CLOUD_LOCATION=us # or us-central1
DOCUMENT_AI_PROCESSOR_ID=your-processor-id
GOOGLE_APPLICATION_CREDENTIALS=path/to/key.json
```

### 3. Running the Service

#### Option A: Docker (Recommended)

1.  Ensure you have your `service-account.json` key in the project root.
2.  Update `.env` or `docker-compose.yml` with your config.
3.  Run:

```bash
docker-compose up -d
```
(This will pull the image `satheeshds/paperless-document-processor` from Docker Hub)

##### Using the Makefile

Common Docker commands are available via `make`:

```bash
# Build the Docker image locally (override TAG if needed)
make image TAG=latest

# Start the stack with docker compose (uses .env by default)
make compose-up

# Stop the stack
make compose-down
```

#### Option B: Standalone

You can run the service using the provided script:

```bash
.\start.bat
```

Or manually:

```bash
go run cmd/server/main.go
```

The service will start on port `8080`.

### 4. Paperless-ngx Configuration

Configure a **Webhook** in Paperless-ngx to trigger this service when a document is added.

- **Trigger**: Document Added
- **URL**: `http://<machine-ip>:8080/webhook`
- **Method**: POST
