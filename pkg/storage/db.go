package storage

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

type DB struct {
	Conn *sql.DB
}

type ProcessedDocument struct {
	PaperlessID   int
	Filename      string
	Supplier      string
	Date          string
	TotalAmount   float64
	RawOCRData    string // JSON string
	ExtractedText string
	CreatedAt     time.Time
}

func InitDB(filepath string) (*DB, error) {
	slog.Info("Initializing database", "path", filepath)
	db, err := sql.Open("duckdb", filepath)
	if err != nil {
		slog.Error("Failed to open database", "path", filepath, "error", err)
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		slog.Error("Failed to ping database", "path", filepath, "error", err)
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Install and load excel extension
	_, err = db.Exec("INSTALL excel; LOAD excel;")
	if err != nil {
		slog.Warn("Failed to install/load excel extension", "error", err)
	}

	if err := createTables(db); err != nil {
		slog.Error("Failed to create tables", "error", err)
		return nil, fmt.Errorf("failed to create tables: %w", err)
	}

	slog.Info("Database initialized successfully")
	return &DB{Conn: db}, nil
}

func createTables(db *sql.DB) error {
	// 1. Try to create the sequence (Native DuckDB path)
	_, err := db.Exec("CREATE SEQUENCE IF NOT EXISTS seq_processed_documents_id;")

	var query string
	if err == nil {
		// Success! This is a native DuckDB database.
		slog.Debug("Creating tables using native DuckDB sequence")
		query = `
		CREATE TABLE IF NOT EXISTS processed_documents (
			id INTEGER PRIMARY KEY DEFAULT nextval('seq_processed_documents_id'),
			paperless_id INTEGER NOT NULL,
			filename TEXT,
			supplier TEXT,
			date TEXT,
			total_amount REAL,
			raw_ocr_data TEXT,
			extracted_text TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`
	} else if strings.Contains(err.Error(), "SQLite") || strings.Contains(strings.ToLower(err.Error()), "sqlite") {
		// This is a SQLite file being opened by DuckDB.
		slog.Warn("Database identified as SQLite, using SQLite-compatible schema")
		query = `
		CREATE TABLE IF NOT EXISTS processed_documents (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			paperless_id INTEGER NOT NULL,
			filename TEXT,
			supplier TEXT,
			date TEXT,
			total_amount REAL,
			raw_ocr_data TEXT,
			extracted_text TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);`
	} else {
		// Some other error
		return fmt.Errorf("failed to initialize sequence: %w", err)
	}

	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create processed_documents table: %w", err)
	}

	// Create index
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_paperless_id ON processed_documents(paperless_id);`)
	return err
}

func (d *DB) SaveDocument(doc *ProcessedDocument) error {
	slog.Debug("Saving processed document to DB", "paperless_id", doc.PaperlessID, "filename", doc.Filename)
	query := `
	INSERT INTO processed_documents (paperless_id, filename, supplier, date, total_amount, raw_ocr_data, extracted_text)
	VALUES (?, ?, ?, ?, ?, ?, ?)
	`
	_, err := d.Conn.Exec(query, doc.PaperlessID, doc.Filename, doc.Supplier, doc.Date, doc.TotalAmount, doc.RawOCRData, doc.ExtractedText)
	if err != nil {
		slog.Error("Failed to insert document into DB", "paperless_id", doc.PaperlessID, "error", err)
		return fmt.Errorf("failed to insert document: %w", err)
	}
	return nil
}

func (d *DB) Close() error {
	return d.Conn.Close()
}

// ReadExcel reads an Excel file using DuckDB's read_xlsx function and prints the output.
func (d *DB) ReadExcel(filePath string, options map[string]interface{}) error {
	slog.Info("Reading Excel file via DuckDB", "path", filePath, "options", options)

	optionStr := ""
	if len(options) > 0 {
		for k, v := range options {
			var valStr string
			switch t := v.(type) {
			case string:
				valStr = fmt.Sprintf("'%s'", t)
			case bool:
				valStr = fmt.Sprintf("%v", t)
			default:
				valStr = fmt.Sprintf("%v", v)
			}
			if optionStr == "" {
				optionStr = fmt.Sprintf(", %s=%s", k, valStr)
			} else {
				optionStr += fmt.Sprintf(", %s=%s", k, valStr)
			}
		}
	}

	query := fmt.Sprintf("SELECT * FROM read_xlsx('%s'%s)", filePath, optionStr)
	slog.Debug("Executing Excel read query", "query", query)

	rows, err := d.Conn.Query(query)
	if err != nil {
		slog.Error("Failed to query Excel file", "error", err)
		return fmt.Errorf("failed to query excel: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	fmt.Printf("Excel Content (%s):\n", filePath)
	fmt.Println(cols)

	count := 0
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return err
		}

		fmt.Println(columns)
		count++
	}

	slog.Info("Successfully read Excel file", "rows", count)
	return nil
}
