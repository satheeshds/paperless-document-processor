package accounting

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type recordedBillInput struct {
	ContactID  *int                   `json:"contact_id"`
	BillNumber string                 `json:"bill_number"`
	IssueDate  string                 `json:"issue_date"`
	DueDate    string                 `json:"due_date"`
	Amount     float64                `json:"amount"`
	Status     string                 `json:"status"`
	FileURL    string                 `json:"file_url"`
	Notes      string                 `json:"notes"`
	Items      []recordedBillLineItem `json:"items"`
}

type recordedBillLineItem struct {
	Description string  `json:"description"`
	Quantity    float64 `json:"quantity"`
	Unit        string  `json:"unit"`
	UnitPrice   float64 `json:"unit_price"`
	Amount      float64 `json:"amount"`
}

type recordedInvoiceInput struct {
	ContactID     *int                      `json:"contact_id"`
	InvoiceNumber string                    `json:"invoice_number"`
	IssueDate     string                    `json:"issue_date"`
	DueDate       string                    `json:"due_date"`
	Amount        float64                   `json:"amount"`
	Status        string                    `json:"status"`
	FileURL       string                    `json:"file_url"`
	Notes         string                    `json:"notes"`
	Items         []recordedInvoiceLineItem `json:"items"`
}

type recordedInvoiceLineItem struct {
	Description string  `json:"description"`
	Quantity    float64 `json:"quantity"`
	Unit        string  `json:"unit"`
	UnitPrice   float64 `json:"unit_price"`
	Amount      float64 `json:"amount"`
}

func TestGetOrCreateVendor_Existing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/api/v1/contacts" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response[[]Contact]{
				Data: []Contact{{ID: 10, Name: "Acme Corp", Type: "vendor"}},
			})
			return
		}
		t.Errorf("Unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	client := NewClient(server.URL, "user", "pass")
	id, err := client.GetOrCreateVendor("Acme Corp")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if id != 10 {
		t.Errorf("Expected ID 10, got %d", id)
	}
}

func TestGetOrCreateVendor_New(t *testing.T) {
	created := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/api/v1/contacts" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response[[]Contact]{Data: []Contact{}})
			return
		}
		if r.Method == "POST" && r.URL.Path == "/api/v1/contacts" {
			var input ContactInput
			json.NewDecoder(r.Body).Decode(&input)
			if input.Name != "New Corp" {
				t.Errorf("Expected name New Corp, got %s", input.Name)
			}
			created = true
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(Response[Contact]{Data: Contact{ID: 20, Name: "New Corp", Type: "vendor"}})
			return
		}
		t.Errorf("Unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	client := NewClient(server.URL, "user", "pass")
	id, err := client.GetOrCreateVendor("New Corp")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if id != 20 {
		t.Errorf("Expected ID 20, got %d", id)
	}
	if !created {
		t.Error("Vendor should have been created")
	}
}

func TestCreateBill(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/api/v1/bills" {
			var input recordedBillInput
			json.NewDecoder(r.Body).Decode(&input)
			if input.Amount != 100.50 {
				t.Errorf("Expected amount 100.50, got %v", input.Amount)
			}
			if input.BillNumber != "BILL-001" {
				t.Errorf("Expected bill number BILL-001, got %s", input.BillNumber)
			}
			if len(input.Items) != 1 {
				t.Errorf("Expected 1 line item, got %d", len(input.Items))
			}
			if len(input.Items) == 1 {
				if input.Items[0].Description != "Consulting services" {
					t.Errorf("Expected line item description Consulting services, got %s", input.Items[0].Description)
				}
				if input.Items[0].Unit != "hours" {
					t.Errorf("Expected line item unit hours, got %s", input.Items[0].Unit)
				}
				if input.Items[0].Amount != 100.50 {
					t.Errorf("Expected line item amount 100.50, got %v", input.Items[0].Amount)
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(Response[Bill]{Data: Bill{ID: 30, Amount: 100.50}})
			return
		}
		t.Errorf("Unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	client := NewClient(server.URL, "user", "pass")
	contactID := 10
	id, err := client.CreateBill(BillInput{
		ContactID:  &contactID,
		BillNumber: "BILL-001",
		Amount:     100.50,
		Items: []BillLineItem{
			{
				Description: "Consulting services",
				Quantity:    2,
				Unit:        "hours",
				UnitPrice:   50.25,
				Amount:      100.50,
			},
		},
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if id != 30 {
		t.Errorf("Expected ID 30, got %d", id)
	}
}

func TestCreateInvoice(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/api/v1/invoices" {
			var input recordedInvoiceInput
			json.NewDecoder(r.Body).Decode(&input)
			if input.Amount != 100.50 {
				t.Errorf("Expected amount 100.50, got %v", input.Amount)
			}
			if input.InvoiceNumber != "INV-001" {
				t.Errorf("Expected invoice number INV-001, got %s", input.InvoiceNumber)
			}
			if len(input.Items) != 1 {
				t.Errorf("Expected 1 item, got %d", len(input.Items))
			}
			if len(input.Items) == 1 {
				if input.Items[0].Description != "Consulting services" {
					t.Errorf("Expected item description Consulting services, got %s", input.Items[0].Description)
				}
				if input.Items[0].Unit != "hours" {
					t.Errorf("Expected item unit hours, got %s", input.Items[0].Unit)
				}
				if input.Items[0].Amount != 100.50 {
					t.Errorf("Expected item amount 100.50, got %v", input.Items[0].Amount)
				}
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(Response[Invoice]{Data: Invoice{ID: 31, Amount: 100.50}})
			return
		}
		t.Errorf("Unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	client := NewClient(server.URL, "user", "pass")
	contactID := 10
	id, err := client.CreateInvoice(InvoiceInput{
		ContactID:     &contactID,
		InvoiceNumber: "INV-001",
		Amount:        100.50,
		Items: []InvoiceLineItem{
			{
				Description: "Consulting services",
				Quantity:    2,
				Unit:        "hours",
				UnitPrice:   50.25,
				Amount:      100.50,
			},
		},
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if id != 31 {
		t.Errorf("Expected ID 31, got %d", id)
	}
}

func TestCreatePayout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/api/v1/payouts" {
			var input PayoutInput
			json.NewDecoder(r.Body).Decode(&input)
			if input.FinalPayoutAmt != 340000 {
				t.Errorf("Expected amount 340000, got %v", input.FinalPayoutAmt)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(Response[Payout]{Data: Payout{ID: 40, FinalPayoutAmt: 340000}})
			return
		}
		t.Errorf("Unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	client := NewClient(server.URL, "user", "pass")
	id, err := client.CreatePayout(PayoutInput{
		OutletName:     "Test Outlet",
		Platform:       "Swiggy",
		FinalPayoutAmt: 340000,
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if id != 40 {
		t.Errorf("Expected ID 40, got %d", id)
	}
}

func TestGetOrCreateBankAccount_Existing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/api/v1/accounts" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response[[]Account]{
				Data: []Account{{ID: 50, Name: "HDFC Bank", Type: "bank"}},
			})
			return
		}
		t.Errorf("Unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	client := NewClient(server.URL, "user", "pass")
	id, err := client.GetOrCreateBankAccount("HDFC Bank")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if id != 50 {
		t.Errorf("Expected ID 50, got %d", id)
	}
}

func TestGetOrCreateBankAccount_CaseInsensitive(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/api/v1/accounts" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response[[]Account]{
				Data: []Account{{ID: 51, Name: "ICICI Bank", Type: "bank"}},
			})
			return
		}
		t.Errorf("Unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	client := NewClient(server.URL, "user", "pass")
	// Lookup with different casing should still find the account.
	id, err := client.GetOrCreateBankAccount("icici bank")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if id != 51 {
		t.Errorf("Expected ID 51, got %d", id)
	}
}

func TestGetOrCreateBankAccount_New(t *testing.T) {
	created := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Path == "/api/v1/accounts" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response[[]Account]{Data: []Account{}})
			return
		}
		if r.Method == "POST" && r.URL.Path == "/api/v1/accounts" {
			var input AccountInput
			json.NewDecoder(r.Body).Decode(&input)
			if input.Name != "SBI Account" {
				t.Errorf("Expected name SBI Account, got %s", input.Name)
			}
			if input.Type != "bank" {
				t.Errorf("Expected type bank, got %s", input.Type)
			}
			created = true
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(Response[Account]{Data: Account{ID: 60, Name: "SBI Account", Type: "bank"}})
			return
		}
		t.Errorf("Unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	client := NewClient(server.URL, "user", "pass")
	id, err := client.GetOrCreateBankAccount("SBI Account")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if id != 60 {
		t.Errorf("Expected ID 60, got %d", id)
	}
	if !created {
		t.Error("Bank account should have been created")
	}
}

func TestGetOrCreateBankAccount_CreateErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(Response[[]Account]{Data: []Account{}})
			return
		}
		// Simulate a server error on create.
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer server.Close()

	client := NewClient(server.URL, "user", "pass")
	_, err := client.GetOrCreateBankAccount("Error Account")
	if err == nil {
		t.Fatal("Expected error for server error on create, got nil")
	}
}

func TestCreateTransaction_Success(t *testing.T) {
	date := "2026-03-01"
	desc := "Payout settlement"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/api/v1/transactions" {
			var input TransactionInput
			json.NewDecoder(r.Body).Decode(&input)
			if input.AccountID != 50 {
				t.Errorf("Expected account ID 50, got %d", input.AccountID)
			}
			if input.Type != "income" {
				t.Errorf("Expected type income, got %s", input.Type)
			}
			if input.Amount != 5000.00 {
				t.Errorf("Expected amount 5000.00, got %v", input.Amount)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(Response[Transaction]{
				Data: Transaction{ID: 100, AccountID: 50, Type: "income", Amount: 5000.00},
			})
			return
		}
		t.Errorf("Unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	client := NewClient(server.URL, "user", "pass")
	id, err := client.CreateTransaction(TransactionInput{
		AccountID:       50,
		Type:            "income",
		Amount:          5000.00,
		TransactionDate: &date,
		Description:     &desc,
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if id != 100 {
		t.Errorf("Expected ID 100, got %d", id)
	}
}

func TestCreateTransaction_ErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unprocessable entity", http.StatusUnprocessableEntity)
	}))
	defer server.Close()

	client := NewClient(server.URL, "user", "pass")
	_, err := client.CreateTransaction(TransactionInput{AccountID: 1, Type: "income", Amount: 100})
	if err == nil {
		t.Fatal("Expected error for non-2xx response, got nil")
	}
}

func TestCreateBill_ErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	client := NewClient(server.URL, "user", "pass")
	_, err := client.CreateBill(BillInput{BillNumber: "BILL-ERR", Amount: 100})
	if err == nil {
		t.Fatal("Expected error for non-2xx response, got nil")
	}
}

func TestCreateInvoice_ErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	client := NewClient(server.URL, "user", "pass")
	_, err := client.CreateInvoice(InvoiceInput{InvoiceNumber: "INV-ERR", Amount: 100})
	if err == nil {
		t.Fatal("Expected error for non-2xx response, got nil")
	}
}

func TestCreatePayout_ErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad request", http.StatusBadRequest)
	}))
	defer server.Close()

	client := NewClient(server.URL, "user", "pass")
	_, err := client.CreatePayout(PayoutInput{OutletName: "Outlet", Platform: PlatformSwiggy, FinalPayoutAmt: 100})
	if err == nil {
		t.Fatal("Expected error for non-2xx response, got nil")
	}
}

func TestPayoutInput_String(t *testing.T) {
	p := PayoutInput{
		OutletName:            "Test Outlet",
		Platform:              PlatformZomato,
		PeriodStart:           "2026-01-01",
		PeriodEnd:             "2026-01-31",
		SettlementDate:        "2026-02-05",
		TotalOrders:           150,
		GrossSalesAmt:         50000,
		RestaurantDiscountAmt: 1000,
		PlatformCommissionAmt: 2500,
		TaxesTcsTdsAmt:        250,
		MarketingAdsAmt:       500,
		FinalPayoutAmt:        45750,
		UtrNumber:             "UTR123456",
	}
	s := p.String()
	if s == "" {
		t.Error("PayoutInput.String() returned empty string")
	}
	// Verify key fields appear in the output.
	for _, want := range []string{"Test Outlet", "zomato", "150", "UTR123456"} {
		if !containsStr(s, want) {
			t.Errorf("PayoutInput.String() missing %q in output: %s", want, s)
		}
	}
}

func TestNewClient_TrimsTrailingSlash(t *testing.T) {
	c := NewClient("http://accounting:8080/", "user", "pass")
	if c.baseURL != "http://accounting:8080" {
		t.Errorf("baseURL = %q, want trailing slash removed", c.baseURL)
	}
}

func containsStr(s, substr string) bool {
	return containsSubstr(s, substr)
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
