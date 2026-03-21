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

type receivedBillInput struct {
	BillNumber string                 `json:"bill_number"`
	Amount     int                    `json:"amount"`
	Items      []receivedBillLineItem `json:"items"`
}

type receivedBillLineItem struct {
	Description string  `json:"description"`
	Quantity    float64 `json:"quantity"`
	Unit        string  `json:"unit"`
	UnitPrice   int     `json:"unit_price"`
	Amount      int     `json:"amount"`
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
	if item.UnitPrice != 5025 {
		t.Errorf("expected unit price 5025, got %d", item.UnitPrice)
	}
	if item.Amount != 10050 {
		t.Errorf("expected amount 10050, got %d", item.Amount)
	}
}

func TestCreateLocalBill_RoundsTotalAmountToPaise(t *testing.T) {
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
		TotalAmount: "0.29",
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

	if receivedBill.Amount != 29 {
		t.Fatalf("expected rounded amount 29, got %d", receivedBill.Amount)
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
