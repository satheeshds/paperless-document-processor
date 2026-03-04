package libreoffice

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParse_WrappedFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/parse" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("file_path") != "/data/documents/originals/invoice.xlsx" {
			t.Errorf("unexpected file_path: %s", q.Get("file_path"))
		}
		if q.Get("sheet_name") != "Order Level" {
			t.Errorf("unexpected sheet_name: %s", q.Get("sheet_name"))
		}
		if q.Get("has_header") != "true" {
			t.Errorf("unexpected has_header: %s", q.Get("has_header"))
		}
		if q.Get("as_table") != "true" {
			t.Errorf("unexpected as_table: %s", q.Get("as_table"))
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"headers": []string{"Date", "Amount", "Platform Fee"},
			"data": []map[string]interface{}{
				{"Date": "2022-01-01", "Amount": 100.0, "Platform Fee": 10.0},
				{"Date": "2022-01-02", "Amount": 200.0, "Platform Fee": 20.0},
			},
		})
	}))
	defer server.Close()

	client := NewClient(server.URL, "/data")
	result, err := client.Parse("documents/originals/invoice.xlsx", "Order Level", "A7:BH", true, true)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
	if len(result.Headers) != 3 {
		t.Errorf("expected 3 headers, got %d", len(result.Headers))
	}
	if result.Headers[0] != "Date" {
		t.Errorf("expected first header 'Date', got %s", result.Headers[0])
	}
}

func TestParse_PlainArrayFormat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]map[string]interface{}{
			{"Date": "2022-01-01", "Amount": 100.0},
			{"Date": "2022-01-02", "Amount": 200.0},
		})
	}))
	defer server.Close()

	client := NewClient(server.URL, "/data")
	result, err := client.Parse("documents/originals/invoice.xlsx", "", "", true, false)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestParse_ServiceError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "file not found", http.StatusNotFound)
	}))
	defer server.Close()

	client := NewClient(server.URL, "/data")
	_, err := client.Parse("documents/originals/missing.xlsx", "", "", true, false)
	if err == nil {
		t.Fatal("expected error for non-200 response")
	}
}

func TestParse_DataOnlyFormat_SanitizesColumnNames(t *testing.T) {
	// Simulates the actual LibreOffice service response: {"data": [...]} with no
	// "headers" key and xlsx column names that contain literal newlines.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// The key "Order level Payout\n(A) - (F) + (G)" contains a real newline.
		payload := `{"data":[` +
			`{"Order date":"2026-02-09 12:25:53","Order level Payout\n(A) - (F) + (G)":548.78,"Bank UTR":"ABC123"},` +
			`{"Order date":"2026-02-10 12:00:00","Order level Payout\n(A) - (F) + (G)":248.15,"Bank UTR":"ABC123"}` +
			`]}`
		w.Write([]byte(payload))
	}))
	defer server.Close()

	client := NewClient(server.URL, "/data")
	result, err := client.Parse("documents/originals/invoice.xlsx", "Order Level", "A7:BH", true, true)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
	// Newline in column name should be replaced with a space.
	const wantCol = "Order level Payout (A) - (F) + (G)"
	if _, ok := result.Rows[0][wantCol]; !ok {
		t.Errorf("expected sanitized column %q to be present in row, got keys: %v", wantCol, keys(result.Rows[0]))
	}
	// Original newlined key must not be present.
	if _, ok := result.Rows[0]["Order level Payout\n(A) - (F) + (G)"]; ok {
		t.Error("expected original newlined column name to be absent after sanitization")
	}
}

func keys(m map[string]interface{}) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

func TestParse_DataPathPrepended(t *testing.T) {
	var gotFilePath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotFilePath = r.URL.Query().Get("file_path")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]map[string]interface{}{})
	}))
	defer server.Close()

	client := NewClient(server.URL, "/mnt/media")
	client.Parse("documents/originals/test.xlsx", "", "", false, false)
	if gotFilePath != "/mnt/media/documents/originals/test.xlsx" {
		t.Errorf("expected /mnt/media/documents/originals/test.xlsx, got %s", gotFilePath)
	}
}
