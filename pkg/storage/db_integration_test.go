package storage

import (
	"fmt"
	"testing"

	"paperless-document-processor/pkg/libreoffice"
)

// newTestDB creates an in-memory DuckDB instance for integration testing.
// Each call returns an independent database; the database is automatically
// closed when the test finishes via t.Cleanup.
func newTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := InitDB("")
	if err != nil {
		t.Fatalf("InitDB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// TestInitDB verifies that InitDB with an in-memory path succeeds and that
// the underlying connection is usable.
func TestInitDB(t *testing.T) {
	db := newTestDB(t)
	if db == nil {
		t.Fatal("expected non-nil DB")
	}
	if db.Conn == nil {
		t.Fatal("expected non-nil DB connection")
	}
	// The processed_documents table must exist after initialisation.
	var count int
	if err := db.Conn.QueryRow("SELECT COUNT(1) FROM processed_documents").Scan(&count); err != nil {
		t.Fatalf("processed_documents table not created: %v", err)
	}
}

// TestSaveDocument_And_IsDocumentProcessed tests the round-trip of saving a
// document and confirming it is recorded as processed.
func TestSaveDocument_And_IsDocumentProcessed(t *testing.T) {
	db := newTestDB(t)

	doc := &ProcessedDocument{
		PaperlessID:   42,
		Filename:      "invoice.pdf",
		Supplier:      "Acme Corp",
		Date:          "2024-01-15",
		TotalAmount:   1500.75,
		RawOCRData:    `{"key":"value"}`,
		ExtractedText: "Invoice from Acme Corp",
	}

	if err := db.SaveDocument(doc); err != nil {
		t.Fatalf("SaveDocument: %v", err)
	}

	processed, err := db.IsDocumentProcessed(42)
	if err != nil {
		t.Fatalf("IsDocumentProcessed: %v", err)
	}
	if !processed {
		t.Error("expected document 42 to be marked as processed")
	}
}

// TestIsDocumentProcessed_NotFound verifies that a document ID that was never
// saved returns false without an error.
func TestIsDocumentProcessed_NotFound(t *testing.T) {
	db := newTestDB(t)

	processed, err := db.IsDocumentProcessed(999)
	if err != nil {
		t.Fatalf("IsDocumentProcessed: %v", err)
	}
	if processed {
		t.Error("expected document 999 to not be processed")
	}
}

// TestSaveDocument_MultipleDocuments saves several distinct documents and
// confirms each one is individually retrievable.
func TestSaveDocument_MultipleDocuments(t *testing.T) {
	db := newTestDB(t)

	for i := 1; i <= 3; i++ {
		doc := &ProcessedDocument{
			PaperlessID: i,
			Filename:    fmt.Sprintf("doc%d.pdf", i),
			Supplier:    "Supplier",
			TotalAmount: float64(i) * 100,
		}
		if err := db.SaveDocument(doc); err != nil {
			t.Fatalf("SaveDocument(%d): %v", i, err)
		}
	}

	for i := 1; i <= 3; i++ {
		ok, err := db.IsDocumentProcessed(i)
		if err != nil {
			t.Fatalf("IsDocumentProcessed(%d): %v", i, err)
		}
		if !ok {
			t.Errorf("expected document %d to be processed", i)
		}
	}

	// Document that was never saved must not appear as processed.
	ok, err := db.IsDocumentProcessed(4)
	if err != nil {
		t.Fatalf("IsDocumentProcessed(4): %v", err)
	}
	if ok {
		t.Error("document 4 should not be processed")
	}
}

// TestLoadRowsIntoTable_WithRows verifies that rows supplied with an explicit
// header list are inserted and queryable by document_id.
func TestLoadRowsIntoTable_WithRows(t *testing.T) {
	db := newTestDB(t)

	result := &libreoffice.ParseResult{
		Headers: []string{"OrderID", "Amount", "Date"},
		Rows: []map[string]interface{}{
			{"OrderID": "ORD-001", "Amount": 150.0, "Date": "2024-01-01"},
			{"OrderID": "ORD-002", "Amount": 250.0, "Date": "2024-01-02"},
		},
	}

	if err := db.LoadRowsIntoTable(1, "test_payout_table", result); err != nil {
		t.Fatalf("LoadRowsIntoTable: %v", err)
	}

	var count int
	if err := db.Conn.QueryRow("SELECT COUNT(1) FROM test_payout_table WHERE document_id = 1").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

// TestLoadRowsIntoTable_Empty verifies that calling LoadRowsIntoTable with an
// empty Rows slice returns nil without creating a table.
func TestLoadRowsIntoTable_Empty(t *testing.T) {
	db := newTestDB(t)

	result := &libreoffice.ParseResult{
		Headers: []string{"Col1"},
		Rows:    []map[string]interface{}{},
	}

	if err := db.LoadRowsIntoTable(1, "test_empty_table", result); err != nil {
		t.Fatalf("LoadRowsIntoTable with empty rows should not error: %v", err)
	}

	// The table should not exist because no rows were provided.
	var ignored int
	err := db.Conn.QueryRow("SELECT COUNT(1) FROM test_empty_table").Scan(&ignored)
	if err == nil {
		t.Error("expected query on non-existent table to fail, but it succeeded")
	}
}

// TestLoadRowsIntoTable_NilResult verifies that a nil ParseResult is handled
// gracefully and returns nil without creating a table.
func TestLoadRowsIntoTable_NilResult(t *testing.T) {
	db := newTestDB(t)

	if err := db.LoadRowsIntoTable(1, "test_nil_table", nil); err != nil {
		t.Fatalf("LoadRowsIntoTable with nil result should not error: %v", err)
	}
}

// TestLoadRowsIntoTable_NoHeaders verifies that rows without an explicit
// header list are still inserted correctly.
func TestLoadRowsIntoTable_NoHeaders(t *testing.T) {
	db := newTestDB(t)

	result := &libreoffice.ParseResult{
		Headers: nil,
		Rows: []map[string]interface{}{
			{"Col1": "value1", "Col2": 42.0},
		},
	}

	if err := db.LoadRowsIntoTable(2, "test_noheader_table", result); err != nil {
		t.Fatalf("LoadRowsIntoTable without headers: %v", err)
	}

	var count int
	if err := db.Conn.QueryRow("SELECT COUNT(1) FROM test_noheader_table WHERE document_id = 2").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}
}

// TestLoadRowsIntoTable_AppendsSameTable verifies that two successive calls to
// LoadRowsIntoTable for the same table name accumulate rows from both calls.
func TestLoadRowsIntoTable_AppendsSameTable(t *testing.T) {
	db := newTestDB(t)

	result1 := &libreoffice.ParseResult{
		Headers: []string{"Name", "Value"},
		Rows:    []map[string]interface{}{{"Name": "doc1", "Value": 10.0}},
	}
	result2 := &libreoffice.ParseResult{
		Headers: []string{"Name", "Value"},
		Rows:    []map[string]interface{}{{"Name": "doc2", "Value": 20.0}},
	}

	if err := db.LoadRowsIntoTable(1, "shared_table", result1); err != nil {
		t.Fatalf("first LoadRowsIntoTable: %v", err)
	}
	if err := db.LoadRowsIntoTable(2, "shared_table", result2); err != nil {
		t.Fatalf("second LoadRowsIntoTable: %v", err)
	}

	var total int
	if err := db.Conn.QueryRow("SELECT COUNT(1) FROM shared_table").Scan(&total); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if total != 2 {
		t.Errorf("expected 2 total rows across both documents, got %d", total)
	}
}
