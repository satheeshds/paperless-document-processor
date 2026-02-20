package accounting

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

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
			var input BillInput
			json.NewDecoder(r.Body).Decode(&input)
			if input.Amount != 10050 {
				t.Errorf("Expected amount 10050, got %d", input.Amount)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(Response[Bill]{Data: Bill{ID: 30, Amount: 10050}})
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
		Amount:     10050,
	})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if id != 30 {
		t.Errorf("Expected ID 30, got %d", id)
	}
}

func TestCreatePayout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && r.URL.Path == "/api/v1/payouts" {
			var input PayoutInput
			json.NewDecoder(r.Body).Decode(&input)
			if input.FinalPayoutAmt != 340000 {
				t.Errorf("Expected amount 340000, got %d", input.FinalPayoutAmt)
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
