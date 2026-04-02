package gateway_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"paperless-document-processor/pkg/gateway"
)

func TestRotateServiceAccount_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/api/v1/service-accounts/tenant-42/rotate" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "admin" || pass != "secret" {
			t.Errorf("unexpected basic auth: user=%s ok=%v", user, ok)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data": map[string]string{
				"username": "svc-tenant-42",
				"password": "rotated-pass",
			},
		})
	}))
	defer srv.Close()

	client := gateway.NewClient(srv.URL, "admin", "secret")
	creds, err := client.RotateServiceAccount("tenant-42")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if creds.Username != "svc-tenant-42" {
		t.Errorf("expected username svc-tenant-42, got %s", creds.Username)
	}
	if creds.Password != "rotated-pass" {
		t.Errorf("expected password rotated-pass, got %s", creds.Password)
	}
}

func TestRotateServiceAccount_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := gateway.NewClient(srv.URL, "admin", "secret")
	_, err := client.RotateServiceAccount("tenant-99")
	if err == nil {
		t.Fatal("expected error for 500 response, got nil")
	}
}

func TestRotateServiceAccount_ErrorField(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"data":  map[string]string{},
			"error": "tenant not found",
		})
	}))
	defer srv.Close()

	client := gateway.NewClient(srv.URL, "admin", "secret")
	_, err := client.RotateServiceAccount("tenant-unknown")
	if err == nil {
		t.Fatal("expected error when response contains error field, got nil")
	}
}
