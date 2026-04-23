package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// ServiceCredentials holds the rotated service account username and password
// for a given tenant.
type ServiceCredentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Client is an HTTP client for the nexus-gateway that authenticates using
// configured admin credentials and provides service-account rotation.
type Client struct {
	baseURL    string
	adminUser  string
	adminPass  string
	httpClient *http.Client
}

// NewClient creates a new nexus-gateway client with the given base URL and
// admin credentials.
func NewClient(baseURL, adminUser, adminPass string) *Client {
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		adminUser:  adminUser,
		adminPass:  adminPass,
		httpClient: &http.Client{},
	}
}

type rotateResponse struct {
	Data  ServiceCredentials `json:"data"`
	Error string             `json:"error,omitempty"`
}

// RotateServiceAccount calls the nexus-gateway to rotate (or fetch) the
// service account credentials for tenantID. Admin credentials are used for
// the request. The returned credentials should be cached by the caller.
func (c *Client) RotateServiceAccount(tenantID string) (*ServiceCredentials, error) {
	path := fmt.Sprintf("%s/api/v1/service-accounts/%s/rotate", c.baseURL, tenantID)

	req, err := http.NewRequest(http.MethodPost, path, bytes.NewReader([]byte(`{}`)))
	if err != nil {
		return nil, fmt.Errorf("gateway: create rotate request: %w", err)
	}
	req.SetBasicAuth(c.adminUser, c.adminPass)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gateway: rotate service account: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("gateway: rotate service account for tenant %q: status %d: %s", tenantID, resp.StatusCode, string(body))
	}

	var result rotateResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("gateway: decode rotate response: %w", err)
	}

	if result.Error != "" {
		return nil, fmt.Errorf("gateway: rotate service account for tenant %q: %s", tenantID, result.Error)
	}

	return &result.Data, nil
}
