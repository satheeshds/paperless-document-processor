package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"paperless-document-processor/pkg/accounting"
	"paperless-document-processor/pkg/docai"
	"paperless-document-processor/pkg/paperless"
	"paperless-document-processor/pkg/storage"
)

type receivedBillInput struct {
	BillNumber string                 `json:"bill_number"`
	Amount     float64                `json:"amount"`
	Items      []receivedBillLineItem `json:"items"`
}

type receivedBillLineItem struct {
	Description string  `json:"description"`
	Quantity    float64 `json:"quantity"`
	Unit        string  `json:"unit"`
	UnitPrice   float64 `json:"unit_price"`
	Amount      float64 `json:"amount"`
}

func TestCreateLocalBill_IncludesExtractedLineItems(t *testing.T) {
	var receivedBill receivedBillInput
	errCh := make(chan error, 2)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/contacts":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(accounting.Response[[]accounting.Contact]{
				Data: []accounting.Contact{{ID: 10, Name: "Acme Corp", Type: "vendor"}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/bills":
			if err := json.NewDecoder(r.Body).Decode(&receivedBill); err != nil {
				errCh <- err
				http.Error(w, "failed to decode bill input", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(accounting.Response[accounting.Bill]{
				Data: accounting.Bill{ID: 30, Amount: receivedBill.Amount},
			})
		default:
			errCh <- &unexpectedRequestError{method: r.Method, path: r.URL.Path}
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	s := &Server{accountingClient: accounting.NewClient(server.URL, "user", "pass")}
	s.createLocalBill(123, &docai.ExtractedData{
		ExampleDate: "2026-03-19",
		TotalAmount: "100.50",
		Supplier:    "Acme Corp",
		Entities: map[string]string{
			"invoice_id": "INV-123",
		},
		LineItems: []docai.LineItem{
			{
				Description: "Consulting services",
				Quantity:    "2",
				Unit:        "hours",
				UnitPrice:   "50.25",
				Amount:      "100.50",
			},
		},
	}, &paperless.Document{OriginalFileName: "invoice.pdf"}, BillRequest{DocURL: "http://paperless/doc/123"})

	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	if receivedBill.BillNumber != "INV-123" {
		t.Fatalf("expected bill number INV-123, got %s", receivedBill.BillNumber)
	}
	if len(receivedBill.Items) != 1 {
		t.Fatalf("expected 1 line item, got %d", len(receivedBill.Items))
	}

	item := receivedBill.Items[0]
	if item.Description != "Consulting services" {
		t.Errorf("expected description Consulting services, got %s", item.Description)
	}
	if item.Quantity != 2 {
		t.Errorf("expected quantity 2, got %v", item.Quantity)
	}
	if item.Unit != "hours" {
		t.Errorf("expected unit hours, got %s", item.Unit)
	}
	if item.UnitPrice != 50.25 {
		t.Errorf("expected unit price 50.25, got %v", item.UnitPrice)
	}
	if item.Amount != 100.50 {
		t.Errorf("expected amount 100.50, got %v", item.Amount)
	}
}

func TestCreateLocalBill_RoundsTotalAmountToTwoDecimals(t *testing.T) {
	var receivedBill receivedBillInput
	errCh := make(chan error, 2)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/contacts":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(accounting.Response[[]accounting.Contact]{
				Data: []accounting.Contact{{ID: 10, Name: "Acme Corp", Type: "vendor"}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/bills":
			if err := json.NewDecoder(r.Body).Decode(&receivedBill); err != nil {
				errCh <- err
				http.Error(w, "failed to decode bill input", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(accounting.Response[accounting.Bill]{
				Data: accounting.Bill{ID: 31, Amount: receivedBill.Amount},
			})
		default:
			errCh <- &unexpectedRequestError{method: r.Method, path: r.URL.Path}
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	s := &Server{accountingClient: accounting.NewClient(server.URL, "user", "pass")}
	s.createLocalBill(124, &docai.ExtractedData{
		ExampleDate: "2026-03-19",
		TotalAmount: "0.295",
		Supplier:    "Acme Corp",
		Entities: map[string]string{
			"invoice_id": "INV-124",
		},
	}, &paperless.Document{OriginalFileName: "invoice.pdf"}, BillRequest{DocURL: "http://paperless/doc/124"})

	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	if receivedBill.Amount != 0.30 {
		t.Fatalf("expected rounded amount 0.30, got %v", receivedBill.Amount)
	}
}

func TestBuildBillLineItems_FiltersEmptyDescription(t *testing.T) {
	items := buildBillLineItems([]docai.LineItem{
		{Description: "", Quantity: "1", Unit: "pcs", UnitPrice: "10.00", Amount: "10.00"},
		{Description: "   ", Quantity: "1", Unit: "pcs", UnitPrice: "10.00", Amount: "10.00"},
		{Description: "Valid item", Quantity: "2", Unit: "hrs", UnitPrice: "50.00", Amount: "100.00"},
	})
	if len(items) != 1 {
		t.Fatalf("expected 1 item after filtering empty description, got %d", len(items))
	}
	if items[0].Description != "Valid item" {
		t.Errorf("expected description 'Valid item', got %s", items[0].Description)
	}
}

func TestBuildBillLineItems_FiltersNonPositiveAmount(t *testing.T) {
	items := buildBillLineItems([]docai.LineItem{
		{Description: "Negative amount", Quantity: "1", Unit: "pcs", UnitPrice: "10.00", Amount: "-10.00"},
		{Description: "Valid item", Quantity: "1", Unit: "pcs", UnitPrice: "10.00", Amount: "10.00"},
	})
	if len(items) != 1 {
		t.Fatalf("expected 1 item after filtering non-positive amounts, got %d", len(items))
	}
	if items[0].Description != "Valid item" {
		t.Errorf("expected description 'Valid item', got %s", items[0].Description)
	}
}

func TestBuildBillLineItems_FiltersNonPositiveUnitPrice(t *testing.T) {
	items := buildBillLineItems([]docai.LineItem{
		{Description: "Zero unit price", Quantity: "1", Unit: "pcs", UnitPrice: "0", Amount: "10.00"},
		{Description: "Negative unit price", Quantity: "1", Unit: "pcs", UnitPrice: "-5.00", Amount: "10.00"},
		{Description: "Valid item", Quantity: "1", Unit: "pcs", UnitPrice: "10.00", Amount: "10.00"},
	})
	if len(items) != 1 {
		t.Fatalf("expected 1 item after filtering non-positive unit prices, got %d", len(items))
	}
	if items[0].Description != "Valid item" {
		t.Errorf("expected description 'Valid item', got %s", items[0].Description)
	}
}

func TestBuildBillLineItems_FiltersNonPositiveQuantity(t *testing.T) {
	items := buildBillLineItems([]docai.LineItem{
		{Description: "Zero quantity", Quantity: "0", Unit: "pcs", UnitPrice: "10.00", Amount: "10.00"},
		{Description: "Negative quantity", Quantity: "-1", Unit: "pcs", UnitPrice: "10.00", Amount: "10.00"},
		{Description: "Valid item", Quantity: "1", Unit: "pcs", UnitPrice: "10.00", Amount: "10.00"},
	})
	if len(items) != 1 {
		t.Fatalf("expected 1 item after filtering non-positive quantities, got %d", len(items))
	}
	if items[0].Description != "Valid item" {
		t.Errorf("expected description 'Valid item', got %s", items[0].Description)
	}
}

type unexpectedRequestError struct {
	method string
	path   string
}

func (e *unexpectedRequestError) Error() string {
	return "unexpected request: " + e.method + " " + e.path
}

func newTestDB(t *testing.T) *storage.DB {
	t.Helper()
	db, err := storage.InitDB(":memory:")
	if err != nil {
		t.Fatalf("failed to init test DB: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestHandleCreateServiceAccount_ReturnsPlainAPIKey(t *testing.T) {
	db := newTestDB(t)
	s := &Server{db: db}

	body, _ := json.Marshal(CreateServiceAccountRequest{Name: "ingestion-service"})
	req := httptest.NewRequest(http.MethodPost, "/service-accounts", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	s.handleCreateServiceAccount(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	var resp CreateServiceAccountResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.APIKey == "" {
		t.Fatal("expected non-empty api_key in response")
	}
	if len(resp.APIKey) < 32 {
		t.Errorf("expected api_key length >= 32, got %d", len(resp.APIKey))
	}
	if resp.Name != "ingestion-service" {
		t.Errorf("expected name ingestion-service, got %s", resp.Name)
	}
	if resp.ID == 0 {
		t.Error("expected non-zero id in response")
	}
	if resp.CreatedAt == "" {
		t.Error("expected non-empty created_at in response")
	}
}

func TestHandleCreateServiceAccount_MissingName(t *testing.T) {
	db := newTestDB(t)
	s := &Server{db: db}

	body, _ := json.Marshal(CreateServiceAccountRequest{Name: ""})
	req := httptest.NewRequest(http.MethodPost, "/service-accounts", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	s.handleCreateServiceAccount(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing name, got %d", w.Code)
	}
}

func TestHandleCreateServiceAccount_InvalidJSON(t *testing.T) {
	db := newTestDB(t)
	s := &Server{db: db}

	req := httptest.NewRequest(http.MethodPost, "/service-accounts", strings.NewReader("{invalid"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	s.handleCreateServiceAccount(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid JSON, got %d", w.Code)
	}
}

func TestGenerateAPIKey_IsUnique(t *testing.T) {
	key1, err1 := generateAPIKey()
	key2, err2 := generateAPIKey()
	if err1 != nil || err2 != nil {
		t.Fatalf("generateAPIKey error: %v / %v", err1, err2)
	}
	if key1 == key2 {
		t.Error("expected unique API keys, got identical values")
	}
}

func TestHashAPIKey_IsConsistent(t *testing.T) {
	key := "testkey123"
	h1 := hashAPIKey(key)
	h2 := hashAPIKey(key)
	if h1 != h2 {
		t.Errorf("expected consistent hash, got %s and %s", h1, h2)
	}
	if h1 == key {
		t.Error("hash should not equal the original key")
	}
}
