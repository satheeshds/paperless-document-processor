package storage

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"paperless-document-processor/config"
	"paperless-document-processor/pkg/accounting"
	"paperless-document-processor/pkg/excel"
	"paperless-document-processor/pkg/libreoffice"

	"github.com/go-viper/mapstructure/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
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

// ValidateConfig logs the Nexus gateway configuration. All DB connections are
// opened per-request via OpenWithTenant (which rotates service-account
// credentials via the nexus-control API), so no static startup connection is
// required.
func ValidateConfig(cfg config.NexusConfig) {
	slog.Info("Nexus gateway configured", "host", cfg.Host, "port", cfg.Port, "control_url", cfg.ControlURL)
}

// serviceAccount holds the rotated credentials returned by the nexus-control API.
type serviceAccount struct {
	Username string `json:"service_id"`
	Password string `json:"service_api_key"`
}

// nexusHTTPClient is a shared HTTP client for nexus-control admin API calls.
var nexusHTTPClient = &http.Client{Timeout: 30 * time.Second}

// RotateTenantServiceAccount calls the nexus-control admin API to rotate the
// service account for the given tenant, returning fresh credentials.
// This matches the pattern in satheeshds/portal db/scheduler.go.
func RotateTenantServiceAccount(controlURL, adminKey, tenantID string) (*serviceAccount, error) {
	if controlURL == "" {
		return nil, fmt.Errorf("NEXUS_CONTROL_URL is not configured")
	}
	if adminKey == "" {
		return nil, fmt.Errorf("ADMIN_API_KEY is not configured")
	}

	endpoint := fmt.Sprintf("%s/api/v1/admin/tenants/%s/service-account/rotate", controlURL, tenantID)
	req, err := http.NewRequest(http.MethodPost, endpoint, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("failed to build rotate request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Admin-API-Key", adminKey)

	resp, err := nexusHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("rotate service account request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("rotate service account returned status %d (could not read body: %w)", resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("rotate service account returned status %d: %s", resp.StatusCode, string(body))
	}

	var sa serviceAccount
	if err := json.NewDecoder(resp.Body).Decode(&sa); err != nil {
		return nil, fmt.Errorf("failed to decode service account response: %w", err)
	}
	if sa.Username == "" || sa.Password == "" {
		return nil, fmt.Errorf("rotate service account response missing credentials for tenant %s", tenantID)
	}

	slog.Debug("rotated service account", "tenant_id", tenantID)
	return &sa, nil
}

// OpenWithTenant opens a single-connection DB scoped to the given tenant.
// It first rotates the service account for the tenant via the nexus-control API
// (using cfg.ControlURL and cfg.AdminAPIKey) to obtain fresh credentials, then
// opens a pgx connection using those credentials. This matches the pattern in
// satheeshds/portal db/db.go OpenWithCredentials + db/scheduler.go RotateTenantServiceAccount.
// Callers are responsible for calling Close when the request is complete.
func OpenWithTenant(cfg config.NexusConfig, tenantID string) (*DB, error) {
	creds, err := RotateTenantServiceAccount(cfg.ControlURL, cfg.AdminAPIKey, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to rotate service account for tenant %s: %w", tenantID, err)
	}

	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, creds.Username, creds.Password, cfg.DBName,
	)
	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse tenant DSN: %w", err)
	}
	connConfig.DefaultQueryExecMode = pgx.QueryExecModeExec

	db := stdlib.OpenDB(*connConfig)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	slog.Debug("Opened per-request tenant DB connection", "tenant_id", tenantID)
	return &DB{Conn: db}, nil
}

func createTables(db *sql.DB) error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS processed_documents (
		id INTEGER PRIMARY KEY,
		paperless_id INTEGER NOT NULL,
		filename TEXT,
		supplier TEXT,
		date TEXT,
		total_amount REAL,
		raw_ocr_data TEXT,
		extracted_text TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`)
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
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err := d.Conn.Exec(query, doc.PaperlessID, doc.Filename, doc.Supplier, doc.Date, doc.TotalAmount, doc.RawOCRData, doc.ExtractedText)
	if err != nil {
		slog.Error("Failed to insert document into DB", "paperless_id", doc.PaperlessID, "error", err)
		return fmt.Errorf("failed to insert document: %w", err)
	}
	return nil
}

func (d *DB) IsDocumentProcessed(docID int) (bool, error) {
	query := `SELECT COUNT(1) FROM processed_documents WHERE paperless_id = $1;`
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

// ProcessPlatformExcel reads an Excel file via the Nexus gateway (DuckDB) and
// stores it into a platform-specific table.
func (d *DB) ProcessPlatformExcel(docID int, filePath string, platform string, options config.PlatformConfig) error {
	slog.Info("Storing Excel file via Nexus gateway into platform table", "platform", platform, "path", filePath)

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

		// 2. Insert data (BY NAME handles varying schema gracefully; DuckDB ignores missing columns)
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
		query := fmt.Sprintf("SELECT COUNT(1) FROM %s WHERE document_id = $1", option.GetTableName(platform))
		slog.Debug("Executing query to get range end", "query", query, "docID", docID)
		rows := d.Conn.QueryRow(query, docID)
		if rows.Err() != nil {
			return excel.Range{}, fmt.Errorf("failed to query platform table: %w", rows.Err())
		}
		if err := rows.Scan(&rowCount); err != nil {
			return excel.Range{}, fmt.Errorf("failed to scan row count: %w", err)
		}

		if option.Header != nil && !*option.Header {
			rowCount--
		}
		if rowCount < 0 {
			rowCount = 0
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
// The struct expression is wrapped in to_json()::VARCHAR so that the result is
// returned as a standard JSON string through the Nexus gateway.
func (d *DB) GetPlatformExcelRows(docID int, platform string, options config.PlatformConfig) (accounting.PayoutInput, error) {
	var payoutInput accounting.PayoutInput
	for _, exportConfig := range options.ExportConfigs {
		if exportConfig.ReaderConfigs == nil || len(exportConfig.ReaderConfigs) == 0 {
			continue
		}
		tableName := exportConfig.GetTableName(platform)
		query := fmt.Sprintf("SELECT to_json(%s)::VARCHAR FROM %s WHERE document_id = $1", exportConfig.ToSelectExpresssions(), tableName)
		slog.Debug("Executing query to get platform table", "query", query, "docID", docID)
		row := d.Conn.QueryRow(query, docID)
		if row.Err() != nil {
			return accounting.PayoutInput{}, fmt.Errorf("failed to query platform table: %w", row.Err())
		}
		var jsonStr string
		if err := row.Scan(&jsonStr); err != nil {
			if err == sql.ErrNoRows {
				slog.Warn("GetPlatformExcelRows: no rows found", "table", tableName, "docID", docID)
				continue
			}
			return accounting.PayoutInput{}, fmt.Errorf("failed to scan platform table row: %w", err)
		}
		slog.Debug("Retrieved platform table JSON", "table", tableName, "json", jsonStr)
		var rowData map[string]interface{}
		if err := json.Unmarshal([]byte(jsonStr), &rowData); err != nil {
			return accounting.PayoutInput{}, fmt.Errorf("failed to parse JSON from platform table: %w", err)
		}
		dc := &mapstructure.DecoderConfig{
			Result:           &payoutInput,
			WeaklyTypedInput: true,
			DecodeHook:       mapstructure.StringToBasicTypeHookFunc(),
		}
		decoder, err := mapstructure.NewDecoder(dc)
		if err != nil {
			return accounting.PayoutInput{}, fmt.Errorf("failed to create mapstructure decoder: %w", err)
		}
		if err := decoder.Decode(rowData); err != nil {
			slog.Warn("GetPlatformExcelRows: partial decode error (some fields may be zero)", "table", tableName, "err", err)
		}
	}
	slog.Debug("Constructed payout input", "rows", payoutInput)
	return payoutInput, nil
}

// marshalOrderedRows encodes rows to JSON with object keys written in the order
// given by headers.  This ensures that read_json_auto creates DuckDB table
// columns in the same sequence as the original xlsx spreadsheet, enabling
// reliable column-index-based access in addition to name-based access.
//
// Keys present in headers but absent from a row are emitted as JSON null.
// Keys in a row that are not in headers are silently omitted.
func marshalOrderedRows(rows []map[string]interface{}, headers []string) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, row := range rows {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('{')
		for j, h := range headers {
			if j > 0 {
				buf.WriteByte(',')
			}
			keyBytes, err := json.Marshal(h)
			if err != nil {
				return nil, fmt.Errorf("marshalOrderedRows: key %q: %w", h, err)
			}
			buf.Write(keyBytes)
			buf.WriteByte(':')
			valBytes, err := json.Marshal(row[h]) // nil → JSON null when key absent
			if err != nil {
				return nil, fmt.Errorf("marshalOrderedRows: value for %q: %w", h, err)
			}
			buf.Write(valBytes)
		}
		buf.WriteByte('}')
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

// LoadRowsIntoTable creates (if necessary) a platform-specific DuckDB table from
// rows returned by the LibreOffice parser service and bulk-inserts the rows
// using DuckDB's read_json_auto table function — the same approach used for
// read_xlsx in the Nexus gateway path.
//
// All column types are inferred by DuckDB from the JSON data.  Export-config
// expressions should use TRY_CAST for numeric conversions where needed.
func (d *DB) LoadRowsIntoTable(docID int, tableName string, result *libreoffice.ParseResult) error {
	if result == nil || len(result.Rows) == 0 {
		slog.Warn("LoadRowsIntoTable: no rows to load", "table", tableName, "docID", docID)
		return nil
	}

	// Serialize rows to a temporary JSON file so DuckDB can read them via
	// read_json_auto — identical approach to how the Nexus path uses read_xlsx.
	// Use marshalOrderedRows when headers are available so that read_json_auto
	// creates DuckDB table columns in the original xlsx column sequence.
	var jsonBytes []byte
	var err error
	if len(result.Headers) > 0 {
		jsonBytes, err = marshalOrderedRows(result.Rows, result.Headers)
	} else {
		jsonBytes, err = json.Marshal(result.Rows)
	}
	if err != nil {
		return fmt.Errorf("LoadRowsIntoTable: failed to marshal rows to JSON: %w", err)
	}

	tmpFile, err := os.CreateTemp("", "lo-rows-*.json")
	if err != nil {
		return fmt.Errorf("LoadRowsIntoTable: failed to create temp JSON file: %w", err)
	}
	tmpPath := tmpFile.Name()

	if _, err := tmpFile.Write(jsonBytes); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("LoadRowsIntoTable: failed to write JSON: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("LoadRowsIntoTable: failed to close temp JSON file: %w", err)
	}
	defer os.Remove(tmpPath)

	// Escape single quotes in the path for safe SQL embedding (os.CreateTemp
	// produces safe names, but belt-and-suspenders for portability).
	safePath := strings.ReplaceAll(tmpPath, "'", "''")

	// 1. Create table schema (LIMIT 0 = structure only, no rows) using
	//    read_json_auto — mirrors the read_xlsx CREATE TABLE pattern.
	createStmt := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s AS SELECT %d AS document_id, * FROM read_json_auto('%s') LIMIT 0;`,
		tableName, docID, safePath,
	)
	slog.Debug("LoadRowsIntoTable: create table", "query", createStmt)
	if _, err := d.Conn.Exec(createStmt); err != nil {
		return fmt.Errorf("LoadRowsIntoTable: failed to create table %s: %w", tableName, err)
	}

	// 2. Bulk-insert all rows in a single statement.  BY NAME maps JSON columns
	//    to table columns by name so additional/missing columns don't cause errors.
	insertStmt := fmt.Sprintf(
		`INSERT INTO %s BY NAME SELECT %d AS document_id, * FROM read_json_auto('%s');`,
		tableName, docID, safePath,
	)
	slog.Debug("LoadRowsIntoTable: insert", "query", insertStmt)
	if _, err := d.Conn.Exec(insertStmt); err != nil {
		// Fallback without BY NAME for older DuckDB versions.
		slog.Warn("LoadRowsIntoTable: BY NAME insert failed, retrying without BY NAME", "err", err)
		fallbackStmt := fmt.Sprintf(
			`INSERT INTO %s SELECT %d AS document_id, * FROM read_json_auto('%s');`,
			tableName, docID, safePath,
		)
		if _, err2 := d.Conn.Exec(fallbackStmt); err2 != nil {
			return fmt.Errorf("LoadRowsIntoTable: failed to insert JSON data: %w (fallback: %v)", err, err2)
		}
	}

	slog.Info("LoadRowsIntoTable: loaded rows", "table", tableName, "count", len(result.Rows))
	return nil
}
