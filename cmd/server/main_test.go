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

func TestCreateLocalBill_IncludesExtractedLineItems(t *testing.T) {
	var receivedBill accounting.BillInput

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/contacts":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(accounting.Response[[]accounting.Contact]{
				Data: []accounting.Contact{{ID: 10, Name: "Acme Corp", Type: "vendor"}},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/bills":
			if err := json.NewDecoder(r.Body).Decode(&receivedBill); err != nil {
				t.Fatalf("failed to decode bill input: %v", err)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(accounting.Response[accounting.Bill]{
				Data: accounting.Bill{ID: 30, Amount: receivedBill.Amount},
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
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
				UnitPrice:   "50.25",
				Amount:      "100.50",
			},
		},
	}, &paperless.Document{OriginalFileName: "invoice.pdf"}, BillRequest{DocURL: "http://paperless/doc/123"})

	if receivedBill.BillNumber != "INV-123" {
		t.Fatalf("expected bill number INV-123, got %s", receivedBill.BillNumber)
	}
	if len(receivedBill.LineItems) != 1 {
		t.Fatalf("expected 1 line item, got %d", len(receivedBill.LineItems))
	}

	item := receivedBill.LineItems[0]
	if item.Description != "Consulting services" {
		t.Errorf("expected description Consulting services, got %s", item.Description)
	}
	if item.Quantity != 2 {
		t.Errorf("expected quantity 2, got %v", item.Quantity)
	}
	if item.UnitPrice != 5025 {
		t.Errorf("expected unit price 5025, got %d", item.UnitPrice)
	}
	if item.Amount != 10050 {
		t.Errorf("expected amount 10050, got %d", item.Amount)
	}
}
