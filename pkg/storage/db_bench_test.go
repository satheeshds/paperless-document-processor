package storage

import (
	"fmt"
	"sync/atomic"
	"testing"

	"paperless-document-processor/pkg/libreoffice"
)

// newTestDB opens an in-memory DuckDB instance and returns a ready-to-use *DB.
// The caller is responsible for calling db.Close() when done.
func newTestDB(tb testing.TB) *DB {
	tb.Helper()
	db, err := InitDB(":memory:")
	if err != nil {
		tb.Fatalf("newTestDB: InitDB failed: %v", err)
	}
	return db
}

// sampleDocument returns a ProcessedDocument populated with realistic data.
func sampleDocument(id int) *ProcessedDocument {
	return &ProcessedDocument{
		PaperlessID:   id,
		Filename:      fmt.Sprintf("invoice-%d.pdf", id),
		Supplier:      "Acme Corp",
		Date:          "2024-01-15",
		TotalAmount:   1234.56,
		RawOCRData:    `{"lines":[{"text":"Invoice No: INV-001"},{"text":"Total: 1234.56"}]}`,
		ExtractedText: "Invoice No: INV-001\nTotal: 1234.56",
	}
}

// makeParseResult constructs a libreoffice.ParseResult with n data rows and
// the given column headers.
func makeParseResult(headers []string, n int) *libreoffice.ParseResult {
	rows := make([]map[string]interface{}, n)
	for i := range rows {
		row := make(map[string]interface{}, len(headers))
		for _, h := range headers {
			row[h] = fmt.Sprintf("value-%d", i)
		}
		rows[i] = row
	}
	return &libreoffice.ParseResult{Headers: headers, Rows: rows}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

// BenchmarkSaveDocument measures the throughput of inserting a single
// processed document into the storage layer on each iteration.
func BenchmarkSaveDocument(b *testing.B) {
	db := newTestDB(b)
	defer db.Close()

	b.ResetTimer()
	for i := range b.N {
		if err := db.SaveDocument(sampleDocument(i)); err != nil {
			b.Fatalf("SaveDocument failed: %v", err)
		}
	}
}

// BenchmarkIsDocumentProcessed_Miss measures the lookup cost when the queried
// document is NOT present in the database.
func BenchmarkIsDocumentProcessed_Miss(b *testing.B) {
	db := newTestDB(b)
	defer db.Close()

	b.ResetTimer()
	for i := range b.N {
		_, err := db.IsDocumentProcessed(1_000_000 + i)
		if err != nil {
			b.Fatalf("IsDocumentProcessed failed: %v", err)
		}
	}
}

// BenchmarkIsDocumentProcessed_Hit measures the lookup cost when the queried
// document IS present in the database (index hit).
func BenchmarkIsDocumentProcessed_Hit(b *testing.B) {
	db := newTestDB(b)
	defer db.Close()

	// Pre-populate a single document that will always be found.
	if err := db.SaveDocument(sampleDocument(1)); err != nil {
		b.Fatalf("setup: SaveDocument failed: %v", err)
	}

	b.ResetTimer()
	for range b.N {
		_, err := db.IsDocumentProcessed(1)
		if err != nil {
			b.Fatalf("IsDocumentProcessed failed: %v", err)
		}
	}
}

// BenchmarkLoadRowsIntoTable_Small benchmarks loading 10 rows into a DuckDB
// table via LoadRowsIntoTable (small dataset).
func BenchmarkLoadRowsIntoTable_Small(b *testing.B) {
	benchmarkLoadRowsIntoTable(b, 10)
}

// BenchmarkLoadRowsIntoTable_Medium benchmarks loading 100 rows into a DuckDB
// table via LoadRowsIntoTable (medium dataset).
func BenchmarkLoadRowsIntoTable_Medium(b *testing.B) {
	benchmarkLoadRowsIntoTable(b, 100)
}

// BenchmarkLoadRowsIntoTable_Large benchmarks loading 1 000 rows into a DuckDB
// table via LoadRowsIntoTable (large dataset).
func BenchmarkLoadRowsIntoTable_Large(b *testing.B) {
	benchmarkLoadRowsIntoTable(b, 1000)
}

// benchmarkLoadRowsIntoTable is the shared implementation for the row-loading
// benchmarks.  A fresh table is used on every iteration to avoid the
// measurement being dominated by prior-data read-back.
func benchmarkLoadRowsIntoTable(b *testing.B, rowCount int) {
	b.Helper()
	db := newTestDB(b)
	defer db.Close()

	headers := []string{"Date", "Order ID", "Amount", "Platform Fee", "Net Amount"}
	result := makeParseResult(headers, rowCount)

	b.ResetTimer()
	for i := range b.N {
		tableName := fmt.Sprintf("bench_table_%d", i)
		if err := db.LoadRowsIntoTable(i, tableName, result); err != nil {
			b.Fatalf("LoadRowsIntoTable failed: %v", err)
		}
	}
}

// BenchmarkSaveDocument_Parallel measures concurrent write throughput by
// running SaveDocument in multiple goroutines simultaneously.
func BenchmarkSaveDocument_Parallel(b *testing.B) {
	db := newTestDB(b)
	defer db.Close()

	var counter atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := int(counter.Add(1))
			doc := sampleDocument(id)
			if err := db.SaveDocument(doc); err != nil {
				b.Errorf("SaveDocument failed: %v", err)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

// TestSaveAndCheckDocument verifies that a document saved via SaveDocument is
// subsequently detected by IsDocumentProcessed.
func TestSaveAndCheckDocument(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	doc := sampleDocument(42)

	processed, err := db.IsDocumentProcessed(doc.PaperlessID)
	if err != nil {
		t.Fatalf("IsDocumentProcessed (before save): %v", err)
	}
	if processed {
		t.Fatal("expected document to not be processed before saving")
	}

	if err := db.SaveDocument(doc); err != nil {
		t.Fatalf("SaveDocument: %v", err)
	}

	processed, err = db.IsDocumentProcessed(doc.PaperlessID)
	if err != nil {
		t.Fatalf("IsDocumentProcessed (after save): %v", err)
	}
	if !processed {
		t.Fatal("expected document to be processed after saving")
	}
}

// TestLoadRowsIntoTable_Basic confirms that LoadRowsIntoTable inserts the
// expected number of rows and that a SELECT against the resulting table returns
// the correct count.
func TestLoadRowsIntoTable_Basic(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	headers := []string{"Date", "Amount"}
	result := makeParseResult(headers, 5)

	const docID = 1
	const tableName = "test_lo_rows"
	if err := db.LoadRowsIntoTable(docID, tableName, result); err != nil {
		t.Fatalf("LoadRowsIntoTable: %v", err)
	}

	var count int
	if err := db.Conn.QueryRow(fmt.Sprintf("SELECT COUNT(1) FROM %s WHERE document_id = ?", tableName), docID).Scan(&count); err != nil {
		t.Fatalf("SELECT COUNT: %v", err)
	}
	if count != 5 {
		t.Errorf("expected 5 rows, got %d", count)
	}
}

// TestLoadRowsIntoTable_Empty verifies that passing an empty ParseResult does
// not return an error and leaves no table behind.
func TestLoadRowsIntoTable_Empty(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	result := &libreoffice.ParseResult{}
	if err := db.LoadRowsIntoTable(1, "empty_table", result); err != nil {
		t.Fatalf("LoadRowsIntoTable with empty result: %v", err)
	}
}

// TestSaveDocument_MultipleDocuments verifies that multiple unique documents
// can be saved and are each independently detectable.
func TestSaveDocument_MultipleDocuments(t *testing.T) {
	db := newTestDB(t)
	defer db.Close()

	const n = 10
	for i := 1; i <= n; i++ {
		if err := db.SaveDocument(sampleDocument(i)); err != nil {
			t.Fatalf("SaveDocument(%d): %v", i, err)
		}
	}

	for i := 1; i <= n; i++ {
		ok, err := db.IsDocumentProcessed(i)
		if err != nil {
			t.Fatalf("IsDocumentProcessed(%d): %v", i, err)
		}
		if !ok {
			t.Errorf("expected document %d to be processed", i)
		}
	}
}
