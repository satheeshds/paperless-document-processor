package storage

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"paperless-document-processor/config"
	"paperless-document-processor/pkg/excel"
	"paperless-document-processor/pkg/libreoffice"
	"paperless-document-processor/pkg/portal"

	"github.com/go-viper/mapstructure/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed migrations/*.sql
var embedMigrations embed.FS

func init() {
	// Silence goose's default logger; we use slog.
	goose.SetLogger(goose.NopLogger())
}

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

	endpoint := fmt.Sprintf("%s/api/v1/admin/tenants/%s/service-account/rotate", controlURL, url.PathEscape(tenantID))
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

// tenantEntry holds the minimal tenant data returned by the nexus-control
// list-tenants API (mirrors the portal's internal tenant struct).
type tenantEntry struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// listAllTenants calls GET /api/v1/admin/tenants on the nexus-control API and
// returns all registered tenants. This mirrors portal's listAllTenants in
// db/scheduler.go.
func listAllTenants(controlURL, adminKey string) ([]tenantEntry, error) {
	endpoint := controlURL + "/api/v1/admin/tenants"
	req, err := http.NewRequest(http.MethodGet, endpoint, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("failed to build list-tenants request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Admin-API-Key", adminKey)

	resp, err := nexusHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list-tenants request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("list tenants returned status %d (could not read body: %w)", resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("list tenants returned status %d: %s", resp.StatusCode, string(body))
	}

	var tenants []tenantEntry
	if err := json.NewDecoder(resp.Body).Decode(&tenants); err != nil {
		return nil, fmt.Errorf("failed to decode tenants response: %w", err)
	}
	return tenants, nil
}

// MigrateAllTenants lists every tenant from the nexus-control API, connects to
// each tenant's database using freshly rotated service-account credentials, and
// runs schema migrations. It mirrors portal's MigrateAllTenants in
// db/scheduler.go. Errors for individual tenants are logged but do not abort
// the loop so that a single bad tenant does not block the rest.
func MigrateAllTenants(cfg config.NexusConfig) error {
	tenants, err := listAllTenants(cfg.ControlURL, cfg.AdminAPIKey)
	if err != nil {
		return fmt.Errorf("MigrateAllTenants: failed to list tenants: %w", err)
	}

	slog.Info("running startup migrations for all tenants", "count", len(tenants))

	for _, t := range tenants {
		slog.Info("migrating tenant schema", "tenant_id", t.ID, "tenant_name", t.Name)

		creds, err := RotateTenantServiceAccount(cfg.ControlURL, cfg.AdminAPIKey, t.ID)
		if err != nil {
			slog.Error("failed to rotate service account for migration", "tenant_id", t.ID, "error", err)
			continue
		}

		db, err := openRawDB(cfg, creds)
		if err != nil {
			slog.Error("failed to open DB for migration", "tenant_id", t.ID, "error", err)
			continue
		}

		if err := MigrateDB(db); err != nil {
			slog.Error("migration failed", "tenant_id", t.ID, "error", err)
		} else {
			slog.Info("migration complete", "tenant_id", t.ID)
		}
		db.Close()
	}

	return nil
}

// openRawDB opens a *sql.DB using pre-obtained credentials (no rotation).
// Used by both MigrateAllTenants and OpenWithTenant.
func openRawDB(cfg config.NexusConfig, creds *serviceAccount) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, creds.Username, creds.Password, cfg.DBName,
	)
	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %w", err)
	}
	connConfig.DefaultQueryExecMode = pgx.QueryExecModeExec

	db := stdlib.OpenDB(*connConfig)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db, nil
}

// OpenWithTenant opens a single-connection DB scoped to the given tenant.
// It first rotates the service account for the tenant via the nexus-control API
// (using cfg.ControlURL and cfg.AdminAPIKey) to obtain fresh credentials, then
// opens a pgx connection using those credentials. MigrateDB is called after
// opening so that tenants created after startup automatically get their schema
// on the first request (idempotent — goose skips already-applied migrations).
// OpenWithTenant opens a per-request DB connection for the given tenant.
// It rotates the service account credentials and runs MigrateDB.
// Callers are responsible for calling Close when the request is complete.
func OpenWithTenant(cfg config.NexusConfig, tenantID string) (*DB, error) {
	db, _, err := GetTenantResources(cfg, tenantID, "")
	return db, err
}

// GetTenantResources rotates the service account for the given tenant
// exactly once, opens a per-request DB connection with those credentials, and
// (when portalURL is non-empty) also constructs a portal.Client using
// the same rotated service_id / service_api_key as HTTP Basic Auth credentials.
// This means the portal REST API and the Nexus gateway both use the same
// per-tenant service account, and credentials are only ever live for the
// duration of a single request.
// Callers are responsible for calling db.Close() when the request is complete.
func GetTenantResources(cfg config.NexusConfig, tenantID, portalURL string) (*DB, *portal.Client, error) {
	creds, err := RotateTenantServiceAccount(cfg.ControlURL, cfg.AdminAPIKey, tenantID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to rotate service account for tenant %s: %w", tenantID, err)
	}

	rawDB, err := openRawDB(cfg, creds)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open DB for tenant %s: %w", tenantID, err)
	}

	if err := MigrateDB(rawDB); err != nil {
		rawDB.Close()
		return nil, nil, fmt.Errorf("failed to migrate DB for tenant %s: %w", tenantID, err)
	}

	var acClient *portal.Client
	if portalURL != "" {
		acClient = portal.NewClient(portalURL, creds.Username, creds.Password)
	}

	slog.Debug("Opened per-request tenant DB connection", "tenant_id", tenantID)
	return &DB{Conn: rawDB}, acClient, nil
}

// createGooseVersionTable pre-creates the goose_db_version tracking table with
// DuckLake-compatible DDL before goose.NewProvider runs.  Goose normally
// creates this table itself using "id integer PRIMARY KEY GENERATED BY DEFAULT
// AS IDENTITY" (PostgreSQL-specific); DuckLake does not support PRIMARY KEY or
// GENERATED, but will auto-increment a plain INTEGER id column.  Because goose
// uses CREATE TABLE IF NOT EXISTS, it will skip creation when the table already
// exists, so this must be called first.
//
// Goose also inserts a zero-version baseline row as part of its own
// createVersionTable; since we bypass that, we seed version_id=0 ourselves so
// that goose.Provider.Up() can build its migration plan without failing with
// "missing zero version migration".
func createGooseVersionTable(db *sql.DB) error {
	const createSQL = `CREATE TABLE IF NOT EXISTS goose_db_version (
		id INTEGER,
		version_id BIGINT NOT NULL,
		is_applied BOOLEAN NOT NULL,
		tstamp TIMESTAMP
	);`
	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create goose_db_version table: %w", err)
	}

	// Seed the zero-version baseline row if it does not already exist.
	// Goose normally inserts this row during its own table creation; since we
	// created the table above, goose skips that step and we must seed it here.
	var count int
	if err := db.QueryRow("SELECT COUNT(1) FROM goose_db_version WHERE version_id = 0").Scan(&count); err != nil {
		return fmt.Errorf("failed to check goose zero version: %w", err)
	}
	if count == 0 {
		if _, err := db.Exec("INSERT INTO goose_db_version (version_id, is_applied) VALUES (0, true)"); err != nil {
			return fmt.Errorf("failed to seed goose zero version: %w", err)
		}
	}

	return nil
}

// ensureProcessedDocumentsTable creates the processed_documents table if it
// does not already exist.  This is the canonical DDL applied to every tenant
// database; it is always run after the goose migration pass as a safety net so
// that tables are created even when goose cannot apply migrations (e.g. when
// the Nexus gateway does not support DDL inside transactions).
func ensureProcessedDocumentsTable(db *sql.DB) error {
	const createSQL = `CREATE TABLE IF NOT EXISTS processed_documents (
		id INTEGER,
		paperless_id INTEGER NOT NULL,
		filename TEXT,
		supplier TEXT,
		date TEXT,
		total_amount REAL,
		raw_ocr_data TEXT,
		extracted_text TEXT,
		created_at TIMESTAMP
	);`
	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create processed_documents table: %w", err)
	}
	return nil
}

// MigrateDB applies all pending up-migrations to the provided database using
// embedded SQL files (pkg/storage/migrations/*.sql).  It is idempotent: goose
// records applied versions in a goose_db_version table and skips them on
// subsequent calls.
//
// After the goose pass, ensureProcessedDocumentsTable is always called as a
// safety net.  This covers two scenarios:
//  1. A brand-new tenant where goose has never run.
//  2. A tenant where goose's internal versioning (which uses PostgreSQL-specific
//     pg_tables / advisory-lock queries) does not work reliably against the
//     Nexus/DuckLake gateway — in that case goose logs a warning but the direct
//     DDL still creates the required tables.
func MigrateDB(db *sql.DB) error {
	// Pre-create the goose version table with DuckLake-compatible DDL.
	// Goose's DialectPostgres uses GENERATED BY DEFAULT AS IDENTITY which
	// DuckLake does not support.  We create the table first; goose then skips
	// its own CREATE TABLE IF NOT EXISTS.
	if err := createGooseVersionTable(db); err != nil {
		return fmt.Errorf("failed to initialize goose version table: %w", err)
	}

	migFS, err := fs.Sub(embedMigrations, "migrations")
	if err != nil {
		return fmt.Errorf("failed to create migration filesystem: %w", err)
	}

	provider, err := goose.NewProvider(goose.DialectPostgres, db, migFS)
	if err != nil {
		slog.Warn("goose provider creation failed, continuing with direct DDL", "error", err)
	} else {
		results, err := provider.Up(context.Background())
		if err != nil {
			slog.Warn("goose migration failed, continuing with direct DDL fallback", "error", err)
		} else {
			for _, r := range results {
				slog.Info("applied migration", "version", r.Source.Version, "duration", r.Duration)
			}
			if len(results) > 0 {
				slog.Info("database migrations complete", "applied", len(results))
			}
		}
	}

	// Always run the direct DDL safety net — CREATE TABLE IF NOT EXISTS is
	// idempotent so this is a no-op when the table already exists.
	if err := ensureProcessedDocumentsTable(db); err != nil {
		return fmt.Errorf("database migration failed: %w", err)
	}

	return nil
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
	query := `SELECT 1 FROM processed_documents WHERE paperless_id = $1 LIMIT 1`
	slog.Debug("Executing check statement", "query", query, "docID", docID)
	var exists int
	err := d.Conn.QueryRow(query, docID).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to check document: %w", err)
	}
	return true, nil
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

		tableName := pgx.Identifier{importConfig.GetTableName(platform)}.Sanitize()

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
		query := fmt.Sprintf("SELECT COUNT(1) FROM %s WHERE document_id = $1", pgx.Identifier{option.GetTableName(platform)}.Sanitize())
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
func (d *DB) GetPlatformExcelRows(docID int, platform string, options config.PlatformConfig) (portal.PayoutInput, error) {
	var payoutInput portal.PayoutInput
	for _, exportConfig := range options.ExportConfigs {
		if exportConfig.ReaderConfigs == nil || len(exportConfig.ReaderConfigs) == 0 {
			continue
		}
		tableName := pgx.Identifier{exportConfig.GetTableName(platform)}.Sanitize()
		query := fmt.Sprintf("SELECT to_json(%s)::VARCHAR FROM %s WHERE document_id = $1", exportConfig.ToSelectExpresssions(), tableName)
		slog.Debug("Executing query to get platform table", "query", query, "docID", docID)
		row := d.Conn.QueryRow(query, docID)
		if row.Err() != nil {
			return portal.PayoutInput{}, fmt.Errorf("failed to query platform table: %w", row.Err())
		}
		var jsonStr string
		if err := row.Scan(&jsonStr); err != nil {
			if err == sql.ErrNoRows {
				slog.Warn("GetPlatformExcelRows: no rows found", "table", tableName, "docID", docID)
				continue
			}
			return portal.PayoutInput{}, fmt.Errorf("failed to scan platform table row: %w", err)
		}
		slog.Debug("Retrieved platform table JSON", "table", tableName, "json", jsonStr)
		var rowData map[string]interface{}
		if err := json.Unmarshal([]byte(jsonStr), &rowData); err != nil {
			return portal.PayoutInput{}, fmt.Errorf("failed to parse JSON from platform table: %w", err)
		}
		dc := &mapstructure.DecoderConfig{
			Result:           &payoutInput,
			WeaklyTypedInput: true,
			DecodeHook:       mapstructure.StringToBasicTypeHookFunc(),
		}
		decoder, err := mapstructure.NewDecoder(dc)
		if err != nil {
			return portal.PayoutInput{}, fmt.Errorf("failed to create mapstructure decoder: %w", err)
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
	if result == nil {
		slog.Warn("LoadRowsIntoTable: no result", "table", tableName, "docID", docID)
		return nil
	}

	columns := make([]string, 0, len(result.Headers))
	if len(result.Headers) > 0 {
		columns = append(columns, result.Headers...)
	} else {
		seen := make(map[string]struct{})
		for _, row := range result.Rows {
			for k := range row {
				if _, ok := seen[k]; ok {
					continue
				}
				seen[k] = struct{}{}
				columns = append(columns, k)
			}
		}
		sort.Strings(columns)
	}
	if len(columns) == 0 {
		slog.Warn("LoadRowsIntoTable: rows contain no columns", "table", tableName, "docID", docID)
		return nil
	}

	safeTableName := pgx.Identifier{tableName}.Sanitize()

	createCols := make([]string, 0, len(columns)+1)
	createCols = append(createCols, "document_id INTEGER")
	for _, c := range columns {
		createCols = append(createCols, fmt.Sprintf("%s TEXT", pgx.Identifier{c}.Sanitize()))
	}

	// Create table with explicit TEXT columns — no file access required.
	// Run directly on d.Conn without a transaction: the Nexus gateway rejects
	// the keepalive ping that database/sql sends on Begin(), causing the first
	// statement after Begin() to fail with "empty query".
	createStmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", safeTableName, strings.Join(createCols, ", "))
	slog.Debug("LoadRowsIntoTable: create table", "query", createStmt)
	if _, err := d.Conn.Exec(createStmt); err != nil {
		return fmt.Errorf("LoadRowsIntoTable: failed to create table %s: %w", tableName, err)
	}

	// Reconcile schema for existing tables: CREATE TABLE IF NOT EXISTS does not
	// add columns when the table already exists, but inserts below always target
	// the current column set.
	existingCols := make(map[string]struct{}, len(columns)+1)
	columnQuery := `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = current_schema()
		  AND table_name = $1
	`
	rows, err := d.Conn.Query(columnQuery, tableName)
	if err != nil {
		return fmt.Errorf("LoadRowsIntoTable: failed to query existing columns for %s: %w", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return fmt.Errorf("LoadRowsIntoTable: failed to scan existing column for %s: %w", tableName, err)
		}
		existingCols[columnName] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("LoadRowsIntoTable: failed to read existing columns for %s: %w", tableName, err)
	}

	if _, ok := existingCols["document_id"]; !ok {
		alterStmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s INTEGER;", safeTableName, pgx.Identifier{"document_id"}.Sanitize())
		slog.Debug("LoadRowsIntoTable: alter table add column", "query", alterStmt)
		if _, err := d.Conn.Exec(alterStmt); err != nil {
			return fmt.Errorf("LoadRowsIntoTable: failed to add document_id column to %s: %w", tableName, err)
		}
	}
	for _, c := range columns {
		if _, ok := existingCols[c]; ok {
			continue
		}
		alterStmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s TEXT;", safeTableName, pgx.Identifier{c}.Sanitize())
		slog.Debug("LoadRowsIntoTable: alter table add column", "query", alterStmt)
		if _, err := d.Conn.Exec(alterStmt); err != nil {
			return fmt.Errorf("LoadRowsIntoTable: failed to add column %s to %s: %w", c, tableName, err)
		}
	}

	if len(result.Rows) == 0 {
		slog.Warn("LoadRowsIntoTable: no rows to insert", "table", tableName, "docID", docID)
		return nil
	}

	insertCols := make([]string, 0, len(columns)+1)
	insertCols = append(insertCols, pgx.Identifier{"document_id"}.Sanitize())
	for _, c := range columns {
		insertCols = append(insertCols, pgx.Identifier{c}.Sanitize())
	}
	const batchSize = 100
	valueCountPerRow := len(columns) + 1

	for start := 0; start < len(result.Rows); start += batchSize {
		end := start + batchSize
		if end > len(result.Rows) {
			end = len(result.Rows)
		}
		batchRows := result.Rows[start:end]

		args := make([]interface{}, 0, len(batchRows)*valueCountPerRow)
		valuePlaceholders := make([]string, 0, len(batchRows))
		argPos := 1

		for _, row := range batchRows {
			rowPlaceholders := make([]string, 0, valueCountPerRow)
			rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("$%d", argPos))
			args = append(args, docID)
			argPos++

			for _, c := range columns {
				rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("$%d", argPos))
				v, ok := row[c]
				if !ok || v == nil {
					args = append(args, nil)
				} else {
					switch vv := v.(type) {
					case string:
						args = append(args, vv)
					default:
						args = append(args, fmt.Sprint(vv))
					}
				}
				argPos++
			}
			valuePlaceholders = append(valuePlaceholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
		}

		insertStmt := fmt.Sprintf(
			"INSERT INTO %s (%s) VALUES %s;",
			safeTableName,
			strings.Join(insertCols, ", "),
			strings.Join(valuePlaceholders, ", "),
		)
		if _, err := d.Conn.Exec(insertStmt, args...); err != nil {
			return fmt.Errorf("LoadRowsIntoTable: failed to insert row batch starting at index %d: %w", start, err)
		}
	}

	slog.Info("LoadRowsIntoTable: loaded rows", "table", tableName, "count", len(result.Rows))
	return nil
}
