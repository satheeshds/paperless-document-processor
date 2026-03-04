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
