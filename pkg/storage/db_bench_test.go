package storage_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"paperless-document-processor/pkg/storage"
)

// newBenchDB opens an in-memory DuckDB instance for benchmarking and registers
// cleanup to close the connection when the benchmark finishes.
func newBenchDB(b *testing.B) *storage.DB {
	b.Helper()
	db, err := storage.InitDB(":memory:")
	if err != nil {
		b.Fatalf("failed to initialise DB: %v", err)
	}
	b.Cleanup(func() { db.Close() })
	return db
}

// sampleDoc returns a ProcessedDocument populated with deterministic data for
// the given index i.  Using a distinct PaperlessID per iteration avoids
// accidental uniqueness conflicts across benchmark runs.
func sampleDoc(i int) *storage.ProcessedDocument {
	return &storage.ProcessedDocument{
		PaperlessID:   i,
		Filename:      fmt.Sprintf("invoice-%d.pdf", i),
		Supplier:      "Benchmark Supplier",
		Date:          "2026-01-01",
		TotalAmount:   float64(i) + 0.99,
		RawOCRData:    `{"supplier":"Benchmark Supplier","total":99.99}`,
		ExtractedText: "Order number 1234. Total amount due: 99.99.",
		CreatedAt:     time.Now(),
	}
}

// BenchmarkInitDB measures the cost of opening and initialising a fresh
// in-memory DuckDB database, including table and index creation.
func BenchmarkInitDB(b *testing.B) {
	for i := 0; i < b.N; i++ {
		db, err := storage.InitDB(":memory:")
		if err != nil {
			b.Fatalf("InitDB failed: %v", err)
		}
		db.Close()
	}
}

// BenchmarkSaveDocument measures sequential document inserts.
func BenchmarkSaveDocument(b *testing.B) {
	db := newBenchDB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.SaveDocument(sampleDoc(i)); err != nil {
			b.Fatalf("SaveDocument failed at iteration %d: %v", i, err)
		}
	}
}

// BenchmarkSaveDocument_Parallel measures concurrent document inserts across
// multiple goroutines.  A global counter ensures each goroutine uses a unique
// PaperlessID so there are no duplicate-key conflicts.
func BenchmarkSaveDocument_Parallel(b *testing.B) {
	db := newBenchDB(b)
	var counter atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := int(counter.Add(1))
			if err := db.SaveDocument(sampleDoc(id)); err != nil {
				b.Errorf("SaveDocument failed for id %d: %v", id, err)
			}
		}
	})
}

// BenchmarkIsDocumentProcessed_Hit measures the lookup path when the document
// is already in the database (cache-hit scenario).
func BenchmarkIsDocumentProcessed_Hit(b *testing.B) {
	db := newBenchDB(b)
	const preload = 1000
	for i := 0; i < preload; i++ {
		if err := db.SaveDocument(sampleDoc(i)); err != nil {
			b.Fatalf("setup: SaveDocument failed: %v", err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		found, err := db.IsDocumentProcessed(i % preload)
		if err != nil {
			b.Fatalf("IsDocumentProcessed failed: %v", err)
		}
		if !found {
			b.Fatalf("expected document %d to be present", i%preload)
		}
	}
}

// BenchmarkIsDocumentProcessed_Miss measures the lookup path when the document
// is absent (cache-miss / new-document scenario).
func BenchmarkIsDocumentProcessed_Miss(b *testing.B) {
	db := newBenchDB(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Use large IDs that were never inserted.
		found, err := db.IsDocumentProcessed(i + 1_000_000)
		if err != nil {
			b.Fatalf("IsDocumentProcessed failed: %v", err)
		}
		if found {
			b.Fatalf("expected document %d to be absent", i+1_000_000)
		}
	}
}

// BenchmarkMixedReadWrite exercises interleaved writes and reads to simulate
// a realistic workload where new documents arrive while existing ones are
// checked for duplicate processing.
func BenchmarkMixedReadWrite(b *testing.B) {
	db := newBenchDB(b)
	// Seed the database so reads have something to hit.
	const seed = 500
	for i := 0; i < seed; i++ {
		if err := db.SaveDocument(sampleDoc(i)); err != nil {
			b.Fatalf("seed: SaveDocument failed: %v", err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			// Write path: insert a new document.
			if err := db.SaveDocument(sampleDoc(seed + i)); err != nil {
				b.Fatalf("SaveDocument failed at iteration %d: %v", i, err)
			}
		} else {
			// Read path: look up a previously seeded document.
			if _, err := db.IsDocumentProcessed(i % seed); err != nil {
				b.Fatalf("IsDocumentProcessed failed at iteration %d: %v", i, err)
			}
		}
	}
}
