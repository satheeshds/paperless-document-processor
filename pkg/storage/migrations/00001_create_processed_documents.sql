-- +goose NO TRANSACTION

-- +goose Up
CREATE TABLE IF NOT EXISTS processed_documents (
    id INTEGER,
    paperless_id INTEGER NOT NULL,
    filename TEXT,
    supplier TEXT,
    date TEXT,
    total_amount REAL,
    raw_ocr_data TEXT,
    extracted_text TEXT,
    created_at TIMESTAMP
);

-- +goose Down
DROP TABLE IF EXISTS processed_documents;
