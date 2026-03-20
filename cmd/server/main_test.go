package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"paperless-document-processor/pkg/accounting"
	"paperless-document-processor/pkg/docai"
	"paperless-document-processor/pkg/paperless"
)

type receivedInvoiceInput struct {
	InvoiceNumber string                    `json:"invoice_number"`
	Amount        int                       `json:"amount"`
	Items         []receivedInvoiceLineItem `json:"items"`
}

type receivedInvoiceLineItem struct {
	Description string  `json:"description"`
	Quantity    float64 `json:"quantity"`
	Unit        string  `json:"unit"`
	UnitPrice   int     `json:"unit_price"`
	Amount      int     `json:"amount"`
}

func TestCreateLocalBill_IncludesExtractedLineItems(t *testing.T) {
	var receivedInvoice receivedInvoiceInput
	errCh := make(chan error, 2)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/contacts":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(accounting.Response[[]accounting.Contact]{
				Data: []accounting.Contact{{ID: 10, Name: "Acme Corp", Type: "vendor"}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/invoices":
			if err := json.NewDecoder(r.Body).Decode(&receivedInvoice); err != nil {
				errCh <- err
				http.Error(w, "failed to decode invoice input", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(accounting.Response[map[string]int]{
				Data: map[string]int{"id": 30},
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

	if receivedInvoice.InvoiceNumber != "INV-123" {
		t.Fatalf("expected invoice number INV-123, got %s", receivedInvoice.InvoiceNumber)
	}
	if len(receivedInvoice.Items) != 1 {
		t.Fatalf("expected 1 line item, got %d", len(receivedInvoice.Items))
	}

	item := receivedInvoice.Items[0]
	if item.Description != "Consulting services" {
		t.Errorf("expected description Consulting services, got %s", item.Description)
	}
	if item.Quantity != 2 {
		t.Errorf("expected quantity 2, got %v", item.Quantity)
	}
	if item.Unit != "hours" {
		t.Errorf("expected unit hours, got %s", item.Unit)
	}
	if item.UnitPrice != 5025 {
		t.Errorf("expected unit price 5025, got %d", item.UnitPrice)
	}
	if item.Amount != 10050 {
		t.Errorf("expected amount 10050, got %d", item.Amount)
	}
}

type unexpectedRequestError struct {
	method string
	path   string
}

func (e *unexpectedRequestError) Error() string {
	return "unexpected request: " + e.method + " " + e.path
}
