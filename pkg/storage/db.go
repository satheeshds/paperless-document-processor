package storage

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"paperless-document-processor/config"
	"paperless-document-processor/pkg/accounting"
	"paperless-document-processor/pkg/excel"

	"github.com/duckdb/duckdb-go/v2"
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

func (d *DB) IsDocumentProcessed(docID int) (bool, error) {
	query := `SELECT COUNT(1) FROM processed_documents WHERE paperless_id = ?;`
	slog.Debug("Executing check statement", "query", query, "docID", docID)
	var count int
	if err := d.Conn.QueryRow(query, docID).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to check document: %w", err)
	}
	return count > 0, nil
}

func (d *DB) Close() error {
	return d.Conn.Close()
}

// ProcessPlatformExcel reads an Excel file using DuckDB and stores it into a platform-specific table.
func (d *DB) ProcessPlatformExcel(docID int, filePath string, platform string, options config.PlatformConfig) error {
	slog.Info("Storing Excel file via DuckDB into platform table", "platform", platform, "path", filePath)

	for _, importConfig := range options.ImportConfigs {

		if importConfig.RelativeRange.RelativeConfigIndex > 0 {
			relativeOption := options.ImportConfigs[importConfig.RelativeRange.RelativeConfigIndex]
			relativeRangeEnd, err := d.GetRangeEnd(docID, platform, relativeOption)
			if err != nil {
				return fmt.Errorf("failed to get relative range end: %w", err)
			}
			currentRange, err := excel.NewRange(relativeOption.Range)
			if err != nil {
				return fmt.Errorf("failed to create current range: %w", err)
			}
			currentRange.Start.Row = relativeRangeEnd.End.Row + importConfig.RelativeRange.RowsOffset
			importConfig.Range = currentRange.String()
		}

		optionStr := importConfig.ToOptionString()

		tableName := importConfig.GetTableName(platform)

		// 1. Create table if not exists
		createStmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s AS SELECT %d as document_id, * FROM read_xlsx('%s', %s) LIMIT 0;`, tableName, docID, filePath, optionStr)
		slog.Debug("Executing create table statement", "query", createStmt)
		if _, err := d.Conn.Exec(createStmt); err != nil {
			return fmt.Errorf("failed to create platform table: %w", err)
		}

		// 3. Insert data (using BY NAME safely gracefully handles varying schema if supported, and normally duckdb ignores missing columns)
		insertStmt := fmt.Sprintf(`INSERT INTO %s BY NAME SELECT %d as document_id, * FROM read_xlsx('%s', %s);`, tableName, docID, filePath, optionStr)
		slog.Debug("Executing insert statement", "query", insertStmt)
		if _, err := d.Conn.Exec(insertStmt); err != nil {
			// Fallback to normal insert if BY NAME fails for older DuckDB versions
			fallbackStmt := fmt.Sprintf(`INSERT INTO %s SELECT %d as document_id, * FROM read_xlsx('%s', %s);`, tableName, docID, filePath, optionStr)
			if _, err2 := d.Conn.Exec(fallbackStmt); err2 != nil {
				return fmt.Errorf("failed to insert excel data: %w (fallback error: %v)", err, err2)
			}
		}
		slog.Info("Successfully stored Excel data into", "table", tableName)

	}

	return nil
}

func (d *DB) GetRangeEnd(docID int, platform string, option config.ImportConfig) (excel.Range, error) {
	rangeStart := option.Range
	rangeStartObj, err := excel.NewRange(rangeStart)
	if err != nil {
		return excel.Range{}, fmt.Errorf("failed to parse range: %w", err)
	}
	if rangeStartObj.End.Row > 0 {
		return rangeStartObj, nil
	}

	if rangeStart != "" {
		var rowCount int
		query := fmt.Sprintf("SELECT COUNT(1) FROM %s WHERE document_id = ?", option.GetTableName(platform))
		slog.Debug("Executing query to get range end", "query", query, "docID", docID)
		rows := d.Conn.QueryRow(query, docID)
		if rows.Err() != nil {
			return excel.Range{}, fmt.Errorf("failed to query platform table: %w", rows.Err())
		}
		rows.Scan(&rowCount)

		if !option.Header {
			rowCount--
		}

		lastCell := excel.Cell{
			Row:    rangeStartObj.Start.Row + rowCount,
			Column: rangeStartObj.End.Column,
		}
		rangeEndObj := excel.Range{
			Start: rangeStartObj.Start,
			End:   lastCell,
		}
		slog.Debug("Retrieved range end", "rangeEnd", rangeEndObj)
		return rangeEndObj, nil
	}
	return excel.Range{}, nil
}

// GetPlatformExcelRows retrieves the previously stored Excel rows from the platform table.
func (d *DB) GetPlatformExcelRows(docID int, platform string, options config.PlatformConfig) (accounting.PayoutInput, error) {
	var payoutInput duckdb.Composite[accounting.PayoutInput]
	for _, exportConfig := range options.ExportConfigs {
		if exportConfig.ReaderConfigs == nil || len(exportConfig.ReaderConfigs) == 0 {
			continue
		}
		var jsonMap duckdb.Composite[map[string]interface{}]
		tableName := exportConfig.GetTableName(platform)
		query := fmt.Sprintf("SELECT %s FROM %s WHERE document_id = ?", exportConfig.ToSelectExpresssions(), tableName)
		slog.Debug("Executing query to get platform table", "query", query, "docID", docID)
		rows := d.Conn.QueryRow(query, docID)
		if rows.Err() != nil {
			return accounting.PayoutInput{}, fmt.Errorf("failed to query platform table: %w", rows.Err())
		}
		rows.Scan(&jsonMap)
		slog.Debug("Retrieved platform table", "rows", jsonMap.Get())
		payoutInput.Scan(jsonMap.Get())
	}
	slog.Debug("Constructed payout input", "rows", payoutInput.Get())
	return payoutInput.Get(), nil
}
