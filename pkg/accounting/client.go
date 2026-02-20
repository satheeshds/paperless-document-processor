package accounting

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Client struct {
	baseURL string
	user    string
	pass    string
	client  *http.Client
}

type Contact struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"` // vendor, customer
}

type ContactInput struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Bill struct {
	ID         int     `json:"id"`
	ContactID  *int    `json:"contact_id"`
	BillNumber string  `json:"bill_number"`
	IssueDate  *string `json:"issue_date"`
	DueDate    *string `json:"due_date"`
	Amount     int     `json:"amount"` // in paise
	Status     string  `json:"status"`
}

type BillInput struct {
	ContactID  *int   `json:"contact_id"`
	BillNumber string `json:"bill_number"`
	IssueDate  string `json:"issue_date,omitempty"`
	DueDate    string `json:"due_date,omitempty"`
	Amount     int    `json:"amount"` // in paise
	Status     string `json:"status"`
	FileURL    string `json:"file_url,omitempty"`
	Notes      string `json:"notes,omitempty"`
}

type Payout struct {
	ID                    int     `json:"id"`
	OutletName            string  `json:"outlet_name"`
	Platform              string  `json:"platform"`
	PeriodStart           *string `json:"period_start"`
	PeriodEnd             *string `json:"period_end"`
	SettlementDate        *string `json:"settlement_date"`
	TotalOrders           int     `json:"total_orders"`
	GrossSalesAmt         int     `json:"gross_sales_amt"`
	RestaurantDiscountAmt int     `json:"restaurant_discount_amt"`
	PlatformCommissionAmt int     `json:"platform_commission_amt"`
	TaxesTcsTdsAmt        int     `json:"taxes_tcs_tds_amt"`
	MarketingAdsAmt       int     `json:"marketing_ads_amt"`
	FinalPayoutAmt        int     `json:"final_payout_amt"`
	UtrNumber             string  `json:"utr_number"`
}

type PayoutInput struct {
	OutletName            string  `json:"outlet_name"`
	Platform              string  `json:"platform"`
	PeriodStart           *string `json:"period_start"`
	PeriodEnd             *string `json:"period_end"`
	SettlementDate        *string `json:"settlement_date"`
	TotalOrders           int     `json:"total_orders"`
	GrossSalesAmt         int     `json:"gross_sales_amt"`
	RestaurantDiscountAmt int     `json:"restaurant_discount_amt"`
	PlatformCommissionAmt int     `json:"platform_commission_amt"`
	TaxesTcsTdsAmt        int     `json:"taxes_tcs_tds_amt"`
	MarketingAdsAmt       int     `json:"marketing_ads_amt"`
	FinalPayoutAmt        int     `json:"final_payout_amt"`
	UtrNumber             string  `json:"utr_number"`
}

func (p PayoutInput) String() string {
	return fmt.Sprintf("PayoutInput{OutletName: %s, Platform: %s, PeriodStart: %s, PeriodEnd: %s, SettlementDate: %s, TotalOrders: %d, GrossSalesAmt: %d, RestaurantDiscountAmt: %d, PlatformCommissionAmt: %d, TaxesTcsTdsAmt: %d, MarketingAdsAmt: %d, FinalPayoutAmt: %d, UtrNumber: %s}", p.OutletName, p.Platform, p.PeriodStart, p.PeriodEnd, p.SettlementDate, p.TotalOrders, p.GrossSalesAmt, p.RestaurantDiscountAmt, p.PlatformCommissionAmt, p.TaxesTcsTdsAmt, p.MarketingAdsAmt, p.FinalPayoutAmt, p.UtrNumber)
}

type Response[T any] struct {
	Data  T      `json:"data"`
	Error string `json:"error,omitempty"`
}

func NewClient(baseURL, user, pass string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		user:    user,
		pass:    pass,
		client:  &http.Client{},
	}
}

func (c *Client) request(method, path string, body interface{}) (*http.Response, error) {
	u := fmt.Sprintf("%s/api/v1/%s", c.baseURL, strings.TrimLeft(path, "/"))
	var buf io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		buf = bytes.NewBuffer(jsonBody)
	}

	req, err := http.NewRequest(method, u, buf)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(c.user, c.pass)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return c.client.Do(req)
}

func (c *Client) GetOrCreateVendor(name string) (int, error) {
	// 1. Check if exists
	resp, err := c.request("GET", "contacts?type=vendor", nil)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var listResp Response[[]Contact]
	if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
		return 0, err
	}

	for _, contact := range listResp.Data {
		if strings.EqualFold(contact.Name, name) {
			return contact.ID, nil
		}
	}

	// 2. Create if not exists
	input := ContactInput{Name: name, Type: "vendor"}
	resp, err = c.request("POST", "contacts", input)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var createResp Response[Contact]
	if err := json.NewDecoder(resp.Body).Decode(&createResp); err != nil {
		return 0, err
	}

	return createResp.Data.ID, nil
}

func (c *Client) CreateBill(bill BillInput) (int, error) {
	resp, err := c.request("POST", "bills", bill)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("failed to create bill: %d %s", resp.StatusCode, string(body))
	}

	var createResp Response[Bill]
	if err := json.NewDecoder(resp.Body).Decode(&createResp); err != nil {
		return 0, err
	}

	return createResp.Data.ID, nil
}

func (c *Client) CreatePayout(payout PayoutInput) (int, error) {
	resp, err := c.request("POST", "payouts", payout)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("failed to create payout: %d %s", resp.StatusCode, string(body))
	}

	var createResp Response[Payout]
	if err := json.NewDecoder(resp.Body).Decode(&createResp); err != nil {
		return 0, err
	}

	return createResp.Data.ID, nil
}
