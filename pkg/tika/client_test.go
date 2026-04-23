package tika

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParse_EmptyContent(t *testing.T) {
	client := NewClient("http://localhost:9998")
	_, err := client.Parse([]byte{})
	if err == nil {
		t.Fatal("expected error for empty content, got nil")
	}
}

func TestParse_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rmeta/xhtml" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		if r.Header.Get("Accept") != "application/json" {
			t.Errorf("expected Accept: application/json, got %s", r.Header.Get("Accept"))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]map[string]interface{}{
			{"X-TIKA:content": "<html><body>Invoice content</body></html>"},
		})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	content, err := client.Parse([]byte("fake xlsx bytes"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if content != "<html><body>Invoice content</body></html>" {
		t.Errorf("unexpected content: %s", content)
	}
}

func TestParse_NonOKStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unsupported media type", http.StatusUnsupportedMediaType)
	}))
	defer server.Close()

	client := NewClient(server.URL)
	_, err := client.Parse([]byte("data"))
	if err == nil {
		t.Fatal("expected error for non-200 status, got nil")
	}
}

func TestParse_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("not valid json"))
	}))
	defer server.Close()

	client := NewClient(server.URL)
	_, err := client.Parse([]byte("data"))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestParse_EmptyResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]map[string]interface{}{})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	_, err := client.Parse([]byte("data"))
	if err == nil {
		t.Fatal("expected error for empty results, got nil")
	}
}

func TestParse_MissingTikaContentKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]map[string]interface{}{
			{"Content-Type": "application/vnd.ms-excel"},
		})
	}))
	defer server.Close()

	client := NewClient(server.URL)
	_, err := client.Parse([]byte("data"))
	if err == nil {
		t.Fatal("expected error when X-TIKA:content is missing, got nil")
	}
}

func TestNewClient_TrimsTrailingSlash(t *testing.T) {
	client := NewClient("http://tika:9998/")
	if client.baseURL != "http://tika:9998" {
		t.Errorf("expected baseURL without trailing slash, got %s", client.baseURL)
	}
}
