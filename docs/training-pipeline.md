# Training pipeline architecture (ID-35)

## Goal
Automatically ship documents that arrive in Paperless to Google Cloud for continuous Document AI training without manual uploads. The flow must be reliable, auditable, and able to evolve as new processor versions are trained.

## Simple current approach (requested)
- **Goal now**: Just drop every new Paperless document into a GCS bucket using a predictable folder structure; no Pub/Sub, datasets, or training automation yet.
- **Suggested bucket**: `gs://paperless-training-raw`
- **Folder pattern**: `gs://paperless-training-raw/{yyyy}/{mm}/{dd}/{paperless_id}/{original_file_name}`
  - Example: `gs://paperless-training-raw/2026/03/20/1234/invoice-1234.pdf`
  - Place any sidecar JSON (e.g., tags/custom fields) as `metadata.json` in the same folder.
- **Rough steps** (service side):
  1) On webhook, download the document from Paperless.
  2) Detect MIME type; convert to PDF if needed (reuse LibreOffice/Tika if available).
  3) Upload to the bucket path above (idempotent overwrite OK).
  4) Optionally upload `metadata.json` containing Paperless tags/correspondent to aid later labeling.
- This keeps the path forward for later automation while satisfying the immediate need to stage files in GCS.

## Components (future scalable design)
- **Paperless webhook** – already configured to call the Go service when a document is added.
- **Document processor service (this repo)** – on webhook receipt, publishes a small message to Pub/Sub instead of synchronously handling training uploads.
- **Pub/Sub topic `docai-training-ingest`** – decouples Paperless events from training ingestion; messages carry document ID, filename, tags/labels, and a signed download URL.
- **Cloud Run job `training-dispatcher`** (subscriber) – downloads the document using the signed URL, normalizes the mime type, and writes payloads to Cloud Storage:
  - `gs://paperless-raw/<yyyy>/<mm>/<dd>/<docId>/source.pdf`
  - `gs://paperless-raw/.../metadata.json` (Paperless tags, correspondent, upload timestamps)
- **Cloud Storage event → Cloud Functions / Cloud Run `dataset-importer`** – on new `source.pdf`, calls Document AI Dataset API (`ImportDocuments`) to register the document and optional split (80/20 train/val) using the sibling metadata file for labels.
- **Document AI Dataset + Processor** – stores labeled examples and trains new processor versions when triggered.
- **Cloud Scheduler + Workflows** – nightly job to:
  1) check dataset size/label completeness,
  2) trigger `TrainProcessorVersion`,
  3) wait for completion, run a small evaluation set, and
  4) promote the new version by updating the serving processor/alias used by this service.
- **Audit storage (BigQuery/Firestore)** – lightweight table recording ingestion status, dataset ID, processor version, and errors for observability.

## End-to-end flow
1. **Document created in Paperless** → webhook hits this service.
2. **Publish to Pub/Sub** – message includes `document_id`, `original_file_name`, `tags`, `download_url`, `ingested_at`.
3. **Training dispatcher consumes** – downloads the file, converts to PDF if needed, writes `source.pdf` + `metadata.json` to `gs://paperless-raw/...`.
4. **Storage trigger fires importer** – importer calls:
   - `projects/{project}/locations/{location}/datasets/{dataset}/importDocuments`
   - `gcs_prefix`: `gs://paperless-raw/.../` (imports both PDF and metadata for labels)
5. **Labeling** – if Paperless tags can map directly to processor labels (e.g., supplier, doc type), the importer sets `labels` in the import request. Otherwise, the dataset remains “unlabeled” and can be labeled later in the Document AI UI.
6. **Training cadence** – Cloud Scheduler triggers Workflow nightly; if a threshold (e.g., 200 new labeled docs) is met, it launches `TrainProcessorVersion` and waits for completion.
7. **Promotion** – Workflow updates the serving alias to the new version after a quick regression check. The Go service reads the processor ID/alias from configuration so no redeploy is needed.

## Bucket layout
- `gs://paperless-raw/<date>/<docId>/source.pdf` – immutable raw uploads.
- `gs://paperless-raw/<date>/<docId>/metadata.json` – labels (Paperless tags), correspondent, checksum.
- Optional `gs://paperless-training-eval/` – fixed evaluation set used by Workflow before promoting a new version.

## Reliability and security
- At-least-once delivery via Pub/Sub; dispatcher uses idempotent writes keyed by `docId`.
- GCS object change notifications drive imports; failures are retried with DLQ Pub/Sub for manual inspection.
- Service accounts with least privilege: dispatcher (read Paperless, write GCS), importer (read GCS, call Document AI), workflow (train/promote).
- Checksums in metadata avoid re-importing corrupted downloads.

## Minimal changes required in this service
1) Add a Pub/Sub publish step on webhook receipt (non-blocking) with doc metadata.  
2) Emit a signed download URL (or temporary token) so Cloud Run job can fetch the file without Paperless credentials baked into the job.  
3) Keep the existing synchronous processing path for inference; training ingestion is side-channel and does not affect user latency.
