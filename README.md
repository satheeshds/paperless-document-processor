# Paperless-ngx Document Processor

This Go service integrates Paperless-ngx with Google Document AI to provide superior OCR and metadata extraction for your invoices.

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
PAPERLESS_USERNAME=your-username
PAPERLESS_PASSWORD=your-password

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

## Training pipeline architecture (ID-35)

For the training data flow (Paperless → GCS → Document AI), see [docs/training-pipeline.md](docs/training-pipeline.md). It now includes:
- **Current simple path**: directly upload each Paperless document to a dated folder in a GCS bucket.
- **Scalable path**: Pub/Sub-based ingestion, Cloud Run/Functions import, Dataset API, and scheduled training/promotion.
